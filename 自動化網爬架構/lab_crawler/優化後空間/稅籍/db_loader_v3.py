# -*- coding: utf-8 -*-
"""db_loader_v3.py

這一版做兩件事：

1) **支援新表 tmp_rawData**（我們後來加的 raw 優先入庫概念）
2) **支援舊表 Tmp_TaxInfo / TaxInfo**（沿用既有資料庫，不打掉重練）

核心設計：
- tmp_rawData：每次 run_id 都會把 CSV 三種 row_type 全灌進去（HEADER/META/DATA）。
- Tmp_TaxInfo：日批暫存表（舊表），每次跑之前 TRUNCATE，跑完也 TRUNCATE。
- TaxInfo：主表（舊表），採「只寫入新增與異動」的 append-only 歷史策略（跟舊版一致）。

額外：
- 提供 merge_diff_tmp_to_main_taxinfo()：用更乾淨、可吃索引（至少 Party_ID）且可控的 SQL，
  只抓出 "新增" 或 "欄位有異動" 的資料寫入 TaxInfo。
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pymysql
from pymysql import err as pymysql_err

# optional: load .env into environment (so MYSQL_* can be read)
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    # python-dotenv not installed or .env missing; ignore
    pass


# =========================
# logger
# =========================

def get_logger(name: str = "gcis_pipeline") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    return logger


# =========================
# mysql
# =========================

def get_mysql_settings_from_env() -> Dict[str, Any]:
    host = os.getenv("MYSQL_HOST")
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    db = os.getenv("MYSQL_DB")
    port = int(os.getenv("MYSQL_PORT", "3306"))

    if not host or not user or not password or not db:
        raise ValueError("MySQL 環境變數不足：MYSQL_HOST/USER/PASSWORD/DB 必填。")

    return {
        "host": host,
        "user": user,
        "password": password,
        "database": db,
        "port": port,
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
        "autocommit": False,
    }


def connect_mysql(cfg: Dict[str, Any]):
    return pymysql.connect(**cfg)


# =========================
# helpers
# =========================

def _current_db_name(conn) -> str:
    """回傳目前連線使用的 DB 名稱。"""
    with conn.cursor() as cur:
        cur.execute("SELECT DATABASE() AS db")
        row = cur.fetchone()
        return str(row.get("db") or "").strip()


def table_exists(conn, table_name: str, schema_name: Optional[str] = None) -> bool:
    """檢查 table 是否存在（避免因 legacy DB 缺表而整個流程炸掉）。"""
    schema = (schema_name or _current_db_name(conn) or "crawlerdb").strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 AS ok
            FROM information_schema.tables
            WHERE table_schema=%s AND table_name=%s
            LIMIT 1
            """,
            (schema, table_name),
        )
        return cur.fetchone() is not None


# =========================
# daily cleanup (TRUNCATE)
# =========================

def truncate_daily_tables(conn, logger: Optional[logging.Logger] = None) -> None:
    """日批前後都可呼叫。

    - crawlerdb.tmp_rawData：新表
    - crawlerdb.Tmp_TaxInfo：舊表

    你要求：日批用完就清空，不留在 tmp 表。
    """
    logger = logger or get_logger()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE crawlerdb.tmp_rawData")
        cur.execute("TRUNCATE TABLE crawlerdb.Tmp_TaxInfo")
    conn.commit()
    logger.info("🧹 已清空日批暫存表：tmp_rawData / Tmp_TaxInfo")


# =========================
# tmp_rawData (new table)
# =========================


def truncate_tmp_rawdata(conn, logger: Optional[logging.Logger] = None) -> None:
    """清空新版 tmp_rawData（每日批次用完就清）。"""
    logger = logger or get_logger()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE crawlerdb.tmp_rawData")
    conn.commit()
    logger.info("✅ 已清空 tmp_rawData")


def truncate_legacy_tmp_taxinfo(conn, logger: Optional[logging.Logger] = None) -> None:
    """清空舊版 Tmp_TaxInfo（每日批次用完就清）。"""
    logger = logger or get_logger()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE crawlerdb.Tmp_TaxInfo")
    conn.commit()
    logger.info("✅ 已清空 Tmp_TaxInfo")

def insert_tmp_rawdata(
    conn,
    raw_rows: List[Tuple[Any, ...]],
    logger: Optional[logging.Logger] = None,
    batch_size: int = 10_000,
) -> int:
    """批次寫入 crawlerdb.tmp_rawData。

    raw_rows tuple 欄位順序：
      (run_id, source_url, local_zip_path, local_csv_path, downloaded_at, file_date,
       row_num, row_type, c01..c16)

    downloaded_at：字串或 datetime 都可（pymysql 會處理）
    file_date：YYYY-MM-DD 或 None
    """
    logger = logger or get_logger()

    sql = """
    INSERT INTO crawlerdb.tmp_rawData
      (run_id, source_url, local_zip_path, local_csv_path,
       downloaded_at, file_date,
       row_num, row_type,
       c01,c02,c03,c04,c05,c06,c07,c08,c09,c10,c11,c12,c13,c14,c15,c16)
    VALUES
      (%s,%s,%s,%s,%s,%s,%s,%s,
       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    inserted = 0
    with conn.cursor() as cur:
        for i in range(0, len(raw_rows), batch_size):
            chunk = raw_rows[i : i + batch_size]
            cur.executemany(sql, chunk)
            inserted += len(chunk)
        conn.commit()

    logger.info(f"✅ tmp_rawData 入庫完成：{inserted} 列")
    return inserted


def count_rawdata_data_rows(conn, run_id: str, logger: Optional[logging.Logger] = None) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM crawlerdb.tmp_rawData WHERE run_id=%s AND row_type='DATA'",
            (run_id,),
        )
        return int(cur.fetchone()["cnt"])


# =========================
# Tmp_TaxInfo (legacy tmp table)
# =========================

def insert_legacy_tmp_taxinfo(
    conn,
    rows: List[Dict[str, Any]],
    logger: Optional[logging.Logger] = None,
    batch_size: int = 10_000,
) -> int:
    """寫入舊版 crawlerdb.Tmp_TaxInfo（沒有 run_id，日批暫存表）。"""
    logger = logger or get_logger()

    if not rows:
        logger.warning("⚠️ Tmp_TaxInfo 無資料可寫入")
        return 0

    sql = """
    INSERT INTO crawlerdb.Tmp_TaxInfo
      (Party_Addr, Party_ID, Parent_Party_ID, Party_Name, PaidIn_Capital, Setup_Date,
       Party_Type, Use_Invoice, Ind_Code, Ind_Name, Ind_Code1, Ind_Name1,
       Ind_Code2, Ind_Name2, Ind_Code3, Ind_Name3)
    VALUES
      (%(party_addr)s, %(party_id)s, %(parent_party_id)s, %(party_name)s, %(paidin_capital)s, %(setup_date)s,
       %(party_type)s, %(use_invoice)s, %(ind_code)s, %(ind_name)s, %(ind_code1)s, %(ind_name1)s,
       %(ind_code2)s, %(ind_name2)s, %(ind_code3)s, %(ind_name3)s)
    """

    inserted = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            cur.executemany(sql, chunk)
            inserted += len(chunk)
        conn.commit()

    logger.info(f"✅ Tmp_TaxInfo 入庫完成：{inserted} 筆")
    return inserted


def count_legacy_tmp_taxinfo(conn, logger: Optional[logging.Logger] = None) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM crawlerdb.Tmp_TaxInfo")
        return int(cur.fetchone()["cnt"])


# =========================
# diff tmp -> main (legacy TaxInfo)
# =========================

def merge_diff_tmp_to_main_taxinfo(conn, logger: Optional[logging.Logger] = None) -> Dict[str, int]:
    """把 Tmp_TaxInfo 的資料與 TaxInfo 最新版本比對，只寫入新增與異動。

    舊版做法是用 self-join 找最新一筆（t2 is null），然後逐欄 <=> 比對。
    這裡改用 MySQL 8 window function 取每個 Party_ID 的最新版本，然後用 md5 hash 做一次性比對。

    備註：
    - TaxInfo 沒有 PK/UK，也沒有索引定義在你貼的 DDL 內，所以這步驟本質上還是吃 I/O。
    - 但至少 SQL 結構更乾淨，可讀性更高；若 Party_ID 有索引，會明顯快。
    """
    logger = logger or get_logger()

    # 1) 取 TaxInfo 每個 Party_ID 的最新一筆
    #    - 用 ROW_NUMBER() 避免 self-join t2。
    #    - Update_Time 若有同秒多筆（理論上不太該），會挑其中一筆，但依然可接受。
    insert_sql = """
    INSERT INTO crawlerdb.TaxInfo
      (Party_ID, Party_Addr, Parent_Party_ID, Party_Name, PaidIn_Capital, Setup_Date,
       Party_Type, Use_Invoice, Ind_Code, Ind_Name, Ind_Code1, Ind_Name1,
       Ind_Code2, Ind_Name2, Ind_Code3, Ind_Name3)
    WITH latest AS (
      SELECT
        Party_ID, Party_Addr, Parent_Party_ID, Party_Name, PaidIn_Capital, Setup_Date,
        Party_Type, Use_Invoice, Ind_Code, Ind_Name, Ind_Code1, Ind_Name1,
        Ind_Code2, Ind_Name2, Ind_Code3, Ind_Name3,
        ROW_NUMBER() OVER (PARTITION BY Party_ID ORDER BY Update_Time DESC) AS rn,
        MD5(CONCAT_WS('|',
            IFNULL(Party_Addr,''), IFNULL(Parent_Party_ID,''), IFNULL(Party_Name,''),
            IFNULL(CAST(PaidIn_Capital AS CHAR),''), IFNULL(CAST(Setup_Date AS CHAR),''),
            IFNULL(Party_Type,''), IFNULL(Use_Invoice,''),
            IFNULL(Ind_Code,''), IFNULL(Ind_Name,''),
            IFNULL(Ind_Code1,''), IFNULL(Ind_Name1,''),
            IFNULL(Ind_Code2,''), IFNULL(Ind_Name2,''),
            IFNULL(Ind_Code3,''), IFNULL(Ind_Name3,'')
        )) AS row_hash
      FROM crawlerdb.TaxInfo
    ),
    tmp AS (
      SELECT
        a.Party_ID, a.Party_Addr, a.Parent_Party_ID, a.Party_Name, a.PaidIn_Capital, a.Setup_Date,
        a.Party_Type, a.Use_Invoice, a.Ind_Code, a.Ind_Name, a.Ind_Code1, a.Ind_Name1,
        a.Ind_Code2, a.Ind_Name2, a.Ind_Code3, a.Ind_Name3,
        MD5(CONCAT_WS('|',
            IFNULL(a.Party_Addr,''), IFNULL(a.Parent_Party_ID,''), IFNULL(a.Party_Name,''),
            IFNULL(CAST(a.PaidIn_Capital AS CHAR),''), IFNULL(CAST(a.Setup_Date AS CHAR),''),
            IFNULL(a.Party_Type,''), IFNULL(a.Use_Invoice,''),
            IFNULL(a.Ind_Code,''), IFNULL(a.Ind_Name,''),
            IFNULL(a.Ind_Code1,''), IFNULL(a.Ind_Name1,''),
            IFNULL(a.Ind_Code2,''), IFNULL(a.Ind_Name2,''),
            IFNULL(a.Ind_Code3,''), IFNULL(a.Ind_Name3,'')
        )) AS row_hash
      FROM crawlerdb.Tmp_TaxInfo a
    )
    SELECT
      t.Party_ID, t.Party_Addr, t.Parent_Party_ID, t.Party_Name, t.PaidIn_Capital, t.Setup_Date,
      t.Party_Type, t.Use_Invoice, t.Ind_Code, t.Ind_Name, t.Ind_Code1, t.Ind_Name1,
      t.Ind_Code2, t.Ind_Name2, t.Ind_Code3, t.Ind_Name3
    FROM tmp t
    LEFT JOIN latest l
      ON l.Party_ID = t.Party_ID AND l.rn = 1
    WHERE
      l.Party_ID IS NULL  -- 新統編
      OR t.row_hash <> l.row_hash;  -- 欄位異動
    """

    with conn.cursor() as cur:
        affected = cur.execute(insert_sql)
    conn.commit()

    # 2) 回報筆數（方便你在 log 裡看）
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM crawlerdb.Tmp_TaxInfo")
        tmp_cnt = int(cur.fetchone()["cnt"])
        cur.execute("SELECT COUNT(*) AS cnt FROM crawlerdb.TaxInfo")
        main_cnt = int(cur.fetchone()["cnt"])

    logger.info(f"✅ TaxInfo 差異寫入完成：tmp={tmp_cnt}，affected={affected}，TaxInfo(累積歷史)總筆數={main_cnt}")
    return {"tmp_cnt": tmp_cnt, "affected": int(affected), "main_cnt": main_cnt}


# =========================
# TaxRecord (legacy state table)
# =========================

def ensure_taxrecord_for_new_party_ids(conn, logger: Optional[logging.Logger] = None) -> int:
    """把 TaxInfo 中出現但 TaxRecord 不存在的 Party_ID 補進 TaxRecord。

    注意：你的 legacy DB 不一定有這張表（或可能在不同 schema）。
    這個步驟不該因為缺表把整個日批打爆，所以：
    - 若環境變數 DISABLE_TAXRECORD=1 -> 直接跳過
    - 若 TaxRecord 不存在 -> 記 warning 並跳過
    """
    logger = logger or get_logger()

    if os.getenv("DISABLE_TAXRECORD", "").strip().lower() in {"1", "true", "yes", "on"}:
        logger.warning("⚠️ 已停用 TaxRecord 寫入（DISABLE_TAXRECORD=1）")
        return 0

    schema = _current_db_name(conn) or "crawlerdb"
    if not table_exists(conn, "TaxRecord", schema_name=schema):
        logger.warning(f"⚠️ 找不到 {schema}.TaxRecord，跳過 TaxRecord 補寫（不影響主流程）")
        return 0

    sql = f"""
    INSERT INTO {schema}.TaxRecord (Party_ID, Insert_Time)
    SELECT DISTINCT TI.Party_ID, NOW()
    FROM {schema}.TaxInfo TI
    WHERE NOT EXISTS (
      SELECT 1 FROM {schema}.TaxRecord TR WHERE TR.Party_ID = TI.Party_ID
    )
    """

    try:
        with conn.cursor() as cur:
            inserted = cur.execute(sql)
        conn.commit()
    except pymysql_err.ProgrammingError as e:
        # 1146: Table doesn't exist
        if getattr(e, "args", []) and len(e.args) >= 1 and int(e.args[0]) == 1146:
            logger.warning(f"⚠️ TaxRecord 缺表（{schema}.TaxRecord），跳過補寫：{e}")
            conn.rollback()
            return 0
        raise

    logger.info(f"✅ TaxRecord 新增 Party_ID：{inserted}")
    return int(inserted)
