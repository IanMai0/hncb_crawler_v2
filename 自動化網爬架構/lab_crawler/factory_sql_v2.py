# filename: csv_to_db_etl.py
# -*- coding: utf-8 -*-
"""
專案說明：
- 目的：CSV -> MySQL
- 成功：寫入 FactoryInfo_stage2
- 失敗：寫入 FactoryInfo_error_log，並記錄 __異常註記__ 與 error_batch_time
"""

from __future__ import annotations
import os
import csv
import sys
import traceback
from datetime import datetime
from typing import Dict, Any, Iterable, List, Optional, Protocol, Tuple

import pymysql


# ========== 設定區（可改為 .env） ==========
# 使用環境變數，避免硬編碼
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "your_password")
DB_NAME = os.getenv("DB_NAME", "CrawlerTestDB")
DB_CHARSET = os.getenv("DB_CHARSET", "utf8mb4")

# 重要：CSV 編碼
CSV_ENCODING = os.getenv("CSV_ENCODING", "utf-8-sig")

# 預設批次大小
DEFAULT_CHUNK = int(os.getenv("CHUNK", "5000"))

# ========== 欄位對映（CSV -> stage2） ==========
# 說明：左邊是 DB 欄位，右邊是 CSV 標題名稱（來源欄位）
FIELD_MAPPING_STAGE2: Dict[str, str] = {
    "Party_ID": "統一編號",
    "Party_Name": "工廠名稱",
    "Party_Addr": "工廠地址",
    "Party_Reg_Code": "工廠登記編號",
    "Party_Licence_Code": "工廠設立許可案號",
    "Party_Adr_Dist": "工廠市鎮鄉村里",
    "Person_Name": "工廠負責人姓名",
    "Party_Type": "工廠組織型態",
    "Party_Status": "工廠登記狀態",
    "Industry_Category": "產業類別",
    "Main_Product": "主要產品",
    "Industry_Code": "產業類別代號",
    "Pro_Code": "主要產品代號",
    "PS": "__異常註記__",  # 可用於備註，若來源沒有，可置空
}

# ========== SQL（預先編譯） ==========
SQL_INSERT_STAGE2 = """
INSERT INTO FactoryInfo_stage2 (
    Party_ID, Party_Name, Party_Addr, Party_Reg_Code, Party_Licence_Code,
    Party_Adr_Dist, Person_Name, Party_Type, Party_Status, Industry_Category,
    Main_Product, Industry_Code, Pro_Code, PS, batch_time
) VALUES (
    %(Party_ID)s, %(Party_Name)s, %(Party_Addr)s, %(Party_Reg_Code)s, %(Party_Licence_Code)s,
    %(Party_Adr_Dist)s, %(Person_Name)s, %(Party_Type)s, %(Party_Status)s, %(Industry_Category)s,
    %(Main_Product)s, %(Industry_Code)s, %(Pro_Code)s, %(PS)s, %(batch_time)s
)
"""

SQL_INSERT_ERROR = """
INSERT INTO FactoryInfo_error_log (
    工廠名稱, 工廠登記編號, 工廠設立許可案號, 工廠地址, 工廠市鎮鄉村里,
    工廠負責人姓名, 統一編號, 工廠組織型態, 工廠登記狀態, 產業類別,
    主要產品, 產業類別代號, 主要產品代號, __異常註記__, error_batch_time
) VALUES (
    %(工廠名稱)s, %(工廠登記編號)s, %(工廠設立許可案號)s, %(工廠地址)s, %(工廠市鎮鄉村里)s,
    %(工廠負責人姓名)s, %(統一編號)s, %(工廠組織型態)s, %(工廠登記狀態)s, %(產業類別)s,
    %(主要產品)s, %(產業類別代號)s, %(主要產品代號)s, %(__異常註記__)s, %(error_batch_time)s
)
"""


# ========== 介面設計（依賴反轉） ==========
class DBClient(Protocol):
    """DB 介面（Protocol）：方便日後替換成其他 DB 實作"""
    def executemany(self, sql: str, params: List[Dict[str, Any]]) -> None: ...
    def execute(self, sql: str, params: Dict[str, Any]) -> None: ...
    def commit(self) -> None: ...
    def close(self) -> None: ...


class PyMySQLClient:
    """PyMySQL 具體實作"""
    def __init__(self, host: str, port: int, user: str, password: str, database: str, charset: str = "utf8mb4"):
        self.conn = pymysql.connect(
            host=host, port=port, user=user, password=password, database=database, charset=charset
        )
        self.cur = self.conn.cursor()

    def executemany(self, sql: str, params: List[Dict[str, Any]]) -> None:
        self.cur.executemany(sql, params)

    def execute(self, sql: str, params: Dict[str, Any]) -> None:
        self.cur.execute(sql, params)

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        try:
            self.cur.close()
        finally:
            self.conn.close()


# ========== 驗證器（可擴充規則） ==========
class RecordValidator:
    """資料驗證（可擴充）"""
    # 重要欄位與規則
    REQUIRED_FIELDS: Tuple[str, ...] = ("Party_ID",)
    PARTY_ID_LEN = 8

    @classmethod
    def validate(cls, rec: Dict[str, Any]) -> None:
        """驗證資料；錯誤就 raise Exception"""
        # 必填檢查
        for k in cls.REQUIRED_FIELDS:
            if not rec.get(k):
                raise ValueError(f"{k} 不可為空")

        # Party_ID 長度
        pid = str(rec["Party_ID"])
        if len(pid) != cls.PARTY_ID_LEN or not pid.isdigit():
            raise ValueError("Party_ID 格式錯誤，需為 8 位數字")


# ========== 轉換器（CSV -> Stage2 欄位） ==========
class Transformer:
    """欄位對映 + 清洗"""
    def __init__(self, mapping: Dict[str, str]):
        self.mapping = mapping

    def to_stage2(self, src_row: Dict[str, Any], batch_time: str) -> Dict[str, Any]:
        """將 CSV 的一列轉換為 stage2 欄位"""
        out: Dict[str, Any] = {}
        for dst_col, src_col in self.mapping.items():
            out[dst_col] = src_row.get(src_col, None)
        out["batch_time"] = batch_time
        # 基礎清洗：去除左右空白
        for k, v in list(out.items()):
            if isinstance(v, str):
                out[k] = v.strip()
        return out


# ========== ETL 核心 ========== 
class CSVToMySQLETL:
    """CSV -> MySQL（成功/失敗分流）"""

    def __init__(self, db: DBClient, transformer: Transformer, validator: RecordValidator):
        self.db = db
        self.transformer = transformer
        self.validator = validator
        self.batch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # --- Extract ---
    def extract(self, csv_path: str, encoding: str = CSV_ENCODING) -> Iterable[Dict[str, Any]]:
        """讀取 CSV，逐列回傳字典"""
        with open(csv_path, "r", encoding=encoding, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row

    # --- Transform & Load ---
    def process(self, rows: Iterable[Dict[str, Any]], chunk: int = DEFAULT_CHUNK) -> None:
        """處理流程：Transform + Load，批次提交"""
        ok_buf: List[Dict[str, Any]] = []
        i = 0
        for i, raw in enumerate(rows, start=1):
            try:
                # 轉換
                rec = self.transformer.to_stage2(raw, self.batch_time)
                # 驗證
                self.validator.validate(rec)
                ok_buf.append(rec)
                # 分批寫入成功表
                if len(ok_buf) >= chunk:
                    self.db.executemany(SQL_INSERT_STAGE2, ok_buf)
                    self.db.commit()
                    print(f"已寫入成功資料 {len(ok_buf)} 筆（累計列數：{i}）")
                    ok_buf.clear()
            except Exception as e:
                # 單筆錯就即刻寫入 error_log（保留原始欄位 + 註記）
                error_row = dict(raw)
                error_row["__異常註記__"] = f"{e.__class__.__name__}: {e}"
                error_row["error_batch_time"] = self.batch_time
                self.db.execute(SQL_INSERT_ERROR, error_row)

                # 也可選擇累積後批次寫 error_log（視你的量級與需求）
                # 這裡用即刻寫，避免錯誤過多時記憶體暴增

        # flush 成功緩衝
        if ok_buf:
            self.db.executemany(SQL_INSERT_STAGE2, ok_buf)
            self.db.commit()
            print(f"最後批次成功寫入 {len(ok_buf)} 筆（總列數：{i}）")

        # 最終提交（保險）
        self.db.commit()

    # --- Run 一條龍 ---
    def run(self, csv_path: str, chunk: int = DEFAULT_CHUNK, encoding: str = CSV_ENCODING) -> None:
        print(f"開始執行：CSV -> MySQL；檔案：{csv_path}；批次：{chunk}；時間：{self.batch_time}")
        try:
            rows = self.extract(csv_path=csv_path, encoding=encoding)
            self.process(rows, chunk=chunk)
            print("✅ 全部完成")
        except Exception:
            # 致命錯誤：輸出堆疊（建議同時導向檔案 log）
            print("❌ 發生致命錯誤，程序結束")
            traceback.print_exc()
            raise


# ========== CLI 入口 ==========
def main(args: Optional[List[str]] = None) -> None:
    """CLI 入口：python csv_to_db_etl.py <csv_path> [chunk]"""
    argv = sys.argv if args is None else args
    if len(argv) < 2:
        print("用法：python csv_to_db_etl.py <csv_path> [chunk]")
        sys.exit(1)

    csv_path = argv[1]
    chunk = int(argv[2]) if len(argv) >= 3 else DEFAULT_CHUNK

    # 建立 DB 連線（可改為工廠模式）
    db = PyMySQLClient(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME, charset=DB_CHARSET
    )

    try:
        transformer = Transformer(mapping=FIELD_MAPPING_STAGE2)
        validator = RecordValidator()
        etl = CSVToMySQLETL(db=db, transformer=transformer, validator=validator)
        etl.run(csv_path=csv_path, chunk=chunk, encoding=CSV_ENCODING)
    finally:
        db.close()


if __name__ == "__main__":
    main()
