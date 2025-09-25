import os
from typing import Optional, Sequence
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ======== 連線設定（建議用環境變數）========
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "ffpw8f-w6fWf5Wvstr")
DB_NAME = os.getenv("DB_NAME", "CrawlerTestDB")
DB_SCHEMA = os.getenv("DB_SCHEMA", "CrawlerTestDB")  # MySQL 下 schema=database

def get_engine(db: Optional[str] = None) -> Engine:
    dbname = db if db else ""
    url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{dbname}?charset=utf8mb4"
    return create_engine(url, pool_size=5, max_overflow=10, pool_recycle=1800, pool_pre_ping=True)

# ================= 基礎：建庫 / 建表 =================

DDL_CREATE_DATABASE = """
CREATE DATABASE IF NOT EXISTS `CrawlerTestDB` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
"""

DDL_CREATE_TABLES = """
-- FactoryInfo（正式表）
CREATE TABLE IF NOT EXISTS `CrawlerTestDB`.`FactoryInfo` (
    `Party_ID` VARCHAR(8) PRIMARY KEY COMMENT '統一編號',
    `Party_Name` VARCHAR(255) COMMENT '工廠名稱',
    `Party_Addr` VARCHAR(255) COMMENT '工廠地址',
    `Party_Reg_Code` VARCHAR(50) COMMENT '工廠登記編號',
    `Party_Licence_Code` VARCHAR(20) COMMENT '工廠設立許可案號',
    `Party_Adr_Dist` VARCHAR(100) COMMENT '工廠市鎮鄉村里',
    `Person_Name` VARCHAR(20) COMMENT '工廠負責人姓名',
    `Party_Type` VARCHAR(20) COMMENT '工廠組織型態',
    `Party_Status` VARCHAR(20) COMMENT '工廠登記狀態',
    `Industry_Category` VARCHAR(255) COMMENT '產業類別',
    `Main_Product` VARCHAR(255) COMMENT '主要產品',
    `Industry_Code` VARCHAR(255) COMMENT '產業類別代號',
    `Pro_Code` VARCHAR(255) COMMENT '主要產品代號',
    `PS` VARCHAR(255) COMMENT '異常註記',
    `batch_time` DATETIME COMMENT '匯入批次時間'
);

-- 原始匯入（Raw）stage
CREATE TABLE IF NOT EXISTS `CrawlerTestDB`.`FactoryInfo_stage` (
    `工廠名稱` VARCHAR(500),
    `工廠登記編號` VARCHAR(500),
    `工廠設立許可案號` VARCHAR(500),
    `工廠地址` VARCHAR(500),
    `工廠市鎮鄉村里` VARCHAR(500),
    `工廠負責人姓名` VARCHAR(500),
    `統一編號` VARCHAR(500),
    `工廠組織型態` VARCHAR(500),
    `工廠登記狀態` VARCHAR(500),
    `產業類別` VARCHAR(500),
    `主要產品` VARCHAR(500),
    `產業類別代號` VARCHAR(500),
    `主要產品代號` VARCHAR(500),
    `__異常註記__` VARCHAR(500),
    `batch_time` DATETIME COMMENT '匯入批次時間'
);

-- 清洗後（前哨站）
CREATE TABLE IF NOT EXISTS `CrawlerTestDB`.`FactoryInfo_stage2` (
    `Party_ID` VARCHAR(8),
    `Party_Name` VARCHAR(255),
    `Party_Addr` VARCHAR(255),
    `Party_Reg_Code` VARCHAR(20),
    `Party_Licence_Code` VARCHAR(20),
    `Party_Adr_Dist` VARCHAR(255),
    `Person_Name` VARCHAR(20),
    `Party_Type` VARCHAR(20),
    `Party_Status` VARCHAR(20),
    `Industry_Category` VARCHAR(255),
    `Main_Product` VARCHAR(255),
    `Industry_Code` VARCHAR(255),
    `Pro_Code` VARCHAR(255),
    `PS` VARCHAR(255),
    `batch_time` DATETIME COMMENT '匯入批次時間'
);

-- 壞資料紀錄
CREATE TABLE IF NOT EXISTS `CrawlerTestDB`.`FactoryInfo_error_log` (
    `工廠名稱` VARCHAR(500),
    `工廠登記編號` VARCHAR(500),
    `工廠設立許可案號` VARCHAR(500),
    `工廠地址` VARCHAR(500),
    `工廠市鎮鄉村里` VARCHAR(500),
    `工廠負責人姓名` VARCHAR(500),
    `統一編號` VARCHAR(500),
    `工廠組織型態` VARCHAR(500),
    `工廠登記狀態` VARCHAR(500),
    `產業類別` VARCHAR(500),
    `主要產品` VARCHAR(500),
    `產業類別代號` VARCHAR(500),
    `主要產品代號` VARCHAR(500),
    `__異常註記__` VARCHAR(500),
    `error_batch_time` DATETIME COMMENT '錯誤批次時間'
);
"""

def ensure_database_and_tables():
    # 先連「無 DB」建立 DB
    with get_engine("").begin() as conn:
        conn.execute(text(DDL_CREATE_DATABASE))
    # 再連 DB 建表
    with get_engine(DB_NAME).begin() as conn:
        for chunk in DDL_CREATE_TABLES.split(";"):
            if chunk.strip():
                conn.execute(text(chunk))

# ================= 流程：更新批次時間 / 清洗入站 =================

def mark_stage_batch_time(batch_time: str):
    """
    將 stage 未填的 batch_time 一次補上固定值（例如 '2025-06-01 00:00:00'）
    """
    sql = text(f"""
        UPDATE `{DB_SCHEMA}`.`FactoryInfo_stage`
        SET batch_time = :bt
        WHERE batch_time IS NULL
    """)
    with get_engine(DB_NAME).begin() as conn:
        conn.execute(sql, {"bt": batch_time})

def stage_to_stage2(batch_time: str):
    """
    依規則從 stage 匯入到 stage2（合格資料）
    """
    sql = text(f"""
        INSERT INTO `{DB_SCHEMA}`.`FactoryInfo_stage2` (
            Party_ID, Party_Name, Party_Addr, Party_Reg_Code, Party_Licence_Code,
            Party_Adr_Dist, Person_Name, Party_Type, Party_Status,
            Industry_Category, Main_Product, Industry_Code, Pro_Code, PS, batch_time
        )
        SELECT 
            `統一編號`,
            `工廠名稱`,
            `工廠地址`,
            `工廠登記編號`,
            `工廠設立許可案號`,
            `工廠市鎮鄉村里`,
            `工廠負責人姓名`,
            `工廠組織型態`,
            `工廠登記狀態`,
            `產業類別`,
            `主要產品`,
            `產業類別代號`,
            `主要產品代號`,
            `__異常註記__`,
            batch_time
        FROM `{DB_SCHEMA}`.`FactoryInfo_stage`
        WHERE batch_time = :bt
          AND CHAR_LENGTH(`統一編號`) = 8
          AND `統一編號` REGEXP '^[0-9]+$'
          AND CHAR_LENGTH(`工廠登記編號`) <= 20
          AND CHAR_LENGTH(`工廠設立許可案號`) <= 20
          AND CHAR_LENGTH(`工廠負責人姓名`) <= 20
          AND CHAR_LENGTH(`工廠組織型態`) <= 20
          AND CHAR_LENGTH(`工廠登記狀態`) <= 20
          AND CHAR_LENGTH(`主要產品代號`) <= 255
          AND CHAR_LENGTH(`工廠名稱`) <= 255
          AND CHAR_LENGTH(`工廠地址`) <= 255
          AND CHAR_LENGTH(`工廠市鎮鄉村里`) <= 255
          AND CHAR_LENGTH(`產業類別`) <= 255
          AND CHAR_LENGTH(`主要產品`) <= 255
          AND CHAR_LENGTH(`產業類別代號`) <= 255
    """)
    with get_engine(DB_NAME).begin() as conn:
        conn.execute(sql, {"bt": batch_time})

def stage_to_error_log(batch_time: str):
    """
    依規則將不合格資料寫入 error_log
    """
    sql = text(f"""
        INSERT INTO `{DB_SCHEMA}`.`FactoryInfo_error_log` (
            `工廠名稱`, `工廠登記編號`, `工廠設立許可案號`,
            `工廠地址`, `工廠市鎮鄉村里`, `工廠負責人姓名`,
            `統一編號`, `工廠組織型態`, `工廠登記狀態`,
            `產業類別`, `主要產品`, `產業類別代號`,
            `主要產品代號`, `__異常註記__`, `error_batch_time`
        )
        SELECT 
            `工廠名稱`, `工廠登記編號`, `工廠設立許可案號`,
            `工廠地址`, `工廠市鎮鄉村里`, `工廠負責人姓名`,
            `統一編號`, `工廠組織型態`, `工廠登記狀態`,
            `產業類別`, `主要產品`, `產業類別代號`,
            `主要產品代號`, `__異常註記__`,
            batch_time
        FROM `{DB_SCHEMA}`.`FactoryInfo_stage`
        WHERE batch_time = :bt
          AND (
                CHAR_LENGTH(`統一編號`) <> 8
             OR `統一編號` NOT REGEXP '^[0-9]+$'
             OR CHAR_LENGTH(`工廠登記編號`) > 20
             OR CHAR_LENGTH(`工廠設立許可案號`) > 20
             OR CHAR_LENGTH(`工廠負責人姓名`) > 20
             OR CHAR_LENGTH(`工廠組織型態`) > 20
             OR CHAR_LENGTH(`工廠登記狀態`) > 20
             OR CHAR_LENGTH(`主要產品代號`) > 255
             OR CHAR_LENGTH(`工廠名稱`) > 255
             OR CHAR_LENGTH(`工廠地址`) > 255
             OR CHAR_LENGTH(`工廠市鎮鄉村里`) > 255
             OR CHAR_LENGTH(`產業類別`) > 255
             OR CHAR_LENGTH(`主要產品`) > 255
             OR CHAR_LENGTH(`產業類別代號`) > 255
          )
    """)
    with get_engine(DB_NAME).begin() as conn:
        conn.execute(sql, {"bt": batch_time})

# ================= 目標清單比對 / 報表 =================

def create_target_table_if_needed():
    """
    建立 target250808（若你要從 CSV 匯入測試清單）
    """
    sql = text(f"""
        CREATE TABLE IF NOT EXISTS `{DB_SCHEMA}`.`target250808` (
            Party_ID VARCHAR(8) PRIMARY KEY
        )
    """)
    with get_engine(DB_NAME).begin() as conn:
        conn.execute(sql)

def load_target_from_csv(csv_path: str, chunk: int = 10000):
    """
    將 CSV 的「統一編號」寫入 target250808
    """
    create_target_table_if_needed()
    eng = get_engine(DB_NAME)
    df_iter = pd.read_csv(csv_path, dtype=str, chunksize=chunk)
    with eng.begin() as conn:
        for df in df_iter:
            if "統一編號" not in df.columns:
                raise KeyError("CSV 需有「統一編號」欄位")
            df = df[["統一編號"]].rename(columns={"統一編號": "Party_ID"})
            df["Party_ID"] = df["Party_ID"].astype(str).str.strip()
            rows = [{"pid": v} for v in df["Party_ID"].dropna().unique()]
            if rows:
                conn.execute(
                    text(f"INSERT IGNORE INTO `{DB_SCHEMA}`.`target250808` (Party_ID) VALUES (:pid)"),
                    rows
                )

def query_success_details(batch_time: str) -> pd.DataFrame:
    """
    清單與 stage2 的成功匹配（只抓該批次）
    """
    sql = text(f"""
        SELECT 
            i.Party_ID,
            f.*
        FROM `{DB_SCHEMA}`.`target250808` i
        JOIN `{DB_SCHEMA}`.`FactoryInfo_stage2` f
              ON i.Party_ID = f.Party_ID
        WHERE f.batch_time = :bt
    """)
    with get_engine(DB_NAME).connect() as conn:
        return pd.read_sql(sql, conn, params={"bt": batch_time})

def query_fail_list(batch_time: str) -> pd.DataFrame:
    """
    清單中查無工廠（該批次）
    """
    sql = text(f"""
        SELECT 
            i.Party_ID
        FROM `{DB_SCHEMA}`.`target250808` i
        LEFT JOIN `{DB_SCHEMA}`.`FactoryInfo_stage2` f
               ON i.Party_ID = f.Party_ID
              AND f.batch_time = :bt
        WHERE f.Party_ID IS NULL
    """)
    with get_engine(DB_NAME).connect() as conn:
        return pd.read_sql(sql, conn, params={"bt": batch_time})

def build_quality_report(batch_time: Optional[str] = None) -> pd.DataFrame:
    """
    產生「成功 / 無工廠」合併明細（對齊你的 SQL）
    """
    where_bt = "AND f.batch_time = :bt" if batch_time else ""
    params = {"bt": batch_time} if batch_time else {}
    sql = text(f"""
        SELECT 
            i.Party_ID,
            CASE WHEN f.Party_ID IS NOT NULL THEN '成功' ELSE '無工廠資料' END AS match_status,
            f.Party_Name,
            f.Party_Addr,
            f.Party_Reg_Code,
            f.Party_Licence_Code,
            f.Party_Adr_Dist,
            f.Person_Name,
            f.Party_Type,
            f.Party_Status,
            f.Industry_Category,
            f.Main_Product,
            f.Industry_Code,
            f.Pro_Code,
            f.PS,
            f.batch_time
        FROM `{DB_SCHEMA}`.`target250808` i
        LEFT JOIN `{DB_SCHEMA}`.`FactoryInfo_stage2` f
               ON i.Party_ID = f.Party_ID
              {where_bt}
        ORDER BY match_status DESC, i.Party_ID
    """)
    with get_engine(DB_NAME).connect() as conn:
        return pd.read_sql(sql, conn, params=params)

def summarize_success_fail(batch_time: str) -> pd.DataFrame:
    sql = text(f"""
        SELECT 
            CASE WHEN f.Party_ID IS NOT NULL THEN '成功' ELSE '無工廠資料' END AS match_status,
            COUNT(*) AS total_count
        FROM `{DB_SCHEMA}`.`target250808` i
        LEFT JOIN `{DB_SCHEMA}`.`FactoryInfo_stage2` f
               ON i.Party_ID = f.Party_ID
              AND f.batch_time = :bt
        GROUP BY match_status
    """)
    with get_engine(DB_NAME).connect() as conn:
        return pd.read_sql(sql, conn, params={"bt": batch_time})

def one_to_one_vs_many(batch_time: str) -> pd.DataFrame:
    sql = text(f"""
        WITH counts AS (
            SELECT Party_ID, COUNT(*) AS freq
            FROM `{DB_SCHEMA}`.`FactoryInfo_stage2`
            WHERE batch_time = :bt
            GROUP BY Party_ID
        )
        SELECT 
            SUM(CASE WHEN freq = 1 THEN 1 ELSE 0 END) AS one_to_one_ids,
            SUM(CASE WHEN freq > 1 THEN 1 ELSE 0 END) AS one_to_many_ids,
            COUNT(*) AS total_ids
        FROM counts
    """)
    with get_engine(DB_NAME).connect() as conn:
        return pd.read_sql(sql, conn, params={"bt": batch_time})

# ================= 主流程範例 =================

def run_pipeline(
    batch_time: str,
    target_csv: Optional[str] = None,
    export_paths: Optional[dict] = None
):
    """
    执行一轮：
      1) 建庫建表
      2) 補 stage 的 batch_time
      3) 合格 → stage2；不合格 → error_log
      4) 載入清單（可選）
      5) 產生報表與輸出（可選）
    """
    ensure_database_and_tables()
    mark_stage_batch_time(batch_time)
    stage_to_stage2(batch_time)
    stage_to_error_log(batch_time)

    if target_csv:
        load_target_from_csv(target_csv)

    # 報表
    df_success = query_success_details(batch_time)
    df_fail = query_fail_list(batch_time)
    df_quality = build_quality_report(batch_time)
    df_summary = summarize_success_fail(batch_time)
    df_12 = one_to_one_vs_many(batch_time)

    # 可選輸出
    if export_paths:
        def safe_to_csv(df: pd.DataFrame, path: Optional[str]):
            if path:
                df.to_csv(path, index=False, encoding="utf-8-sig")
        safe_to_csv(df_success, export_paths.get("success"))
        safe_to_csv(df_fail, export_paths.get("fail"))
        safe_to_csv(df_quality, export_paths.get("quality"))
        safe_to_csv(df_summary, export_paths.get("summary"))
        safe_to_csv(df_12, export_paths.get("one_to_many"))

    return {
        "success": df_success,
        "fail": df_fail,
        "quality": df_quality,
        "summary": df_summary,
        "one_to_many": df_12
    }

if __name__ == "__main__":
    # 範例參數（請自行調整）
    BT = "2025-06-01 00:00:00"
    TARGET_CSV = None  # 例如 r"./target250808.csv"（含「統一編號」欄位）
    EXPORT = {
        "success": "./out_success.csv",
        "fail": "./out_fail.csv",
        "quality": "./out_quality.csv",
        "summary": "./out_summary.csv",
        "one_to_many": "./out_1toN.csv",
    }
    run_pipeline(BT, target_csv=TARGET_CSV, export_paths=EXPORT)


