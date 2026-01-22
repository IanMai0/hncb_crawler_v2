# -*- coding: utf-8 -*-
"""db_loader_v2.py

目標：沿用「新版流程」，但資料表採用「舊版 DDL」
  - crawlerdb.Tmp_TaxInfo（日批 tmp，跑完要清空）
  - crawlerdb.TaxInfo（main，保留歷史：只新增新/異動，不刪不覆蓋）

另外：
  - 如果你的 DB 還存在 crawlerdb.tmp_rawData（新版表），本模組也會「有就清」。

環境變數：
  MYSQL_HOST / MYSQL_USER / MYSQL_PASSWORD / MYSQL_DB (必填)
  MYSQL_PORT (預設 3306)
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pymysql


# =========================
# Logger
# =========================

def get_logger(name: str = "gcis_pipeline") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s", "%Y-%m-%d %H:%M:%S")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


# =========================
# MySQL settings
# =========================

@dataclass(frozen=True)
class MySQLSettings:
    host: str
    user: str
    password: str
    db: str
    port: int = 3306
    charset: str = "utf8mb4"


def get_mysql_settings_from_env() -> MySQLSettings:
    host = (os.getenv("MYSQL_HOST") or "").strip()
    user = (os.getenv("MYSQL_USER") or "").strip()
    password = (os.getenv("MYSQL_PASSWORD") or "").strip()
    db = (os.getenv("MYSQL_DB") or "").strip()
    port = int((os.getenv("MYSQL_PORT") or "3306").strip())

    if not (host and user and password and db):
        raise ValueError("MySQL 環境變數不足：MYSQL_HOST/USER/PASSWORD/DB 必填。")

    return MySQLSettings(host=host, user=user, password=password, db=db, port=port)


def connect_mysql(cfg: MySQLSettings):
    # DictCursor：讀取時用欄位名
    return pymysql.connect(
        host=cfg.host,
        user=cfg.user,
        password=cfg.password,
        database=cfg.db,
        port=cfg.port,
        charset=cfg.charset,
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


# =========================
# Helpers
# =========================

def table_exists(conn, schema: str, table: str) -> bool:
    sql = """
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema=%s AND table_name=%s
    LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return cur.fetchone() is not None


def truncate_if_exists(conn, full_table_name: str, logger: Optional[logging.Logger] = None) -> None:
    """full_table_name: e.g. crawlerdb.Tmp_TaxInfo"""
    logger = logger or get_logger()
    if "." not in full_table_name:
        raise ValueError("truncate_if_exists 需要 schema.table 格式")

    schema, table = full_table_name.split(".", 1)
    if not table_exists(conn, schema, table):
        logger.info(f"(skip) table 不存在：{full_table_name}")
        return

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {full_table_name}")
    conn.commit()
    logger.info(f"✅ 已清空：{full_table_name}")


def count_rows(conn, full_table_name: str, where_sql: str = "", params: Tuple[Any, ...] = ()) -> int:
    sql = f"SELECT COUNT(*) AS cnt FROM {full_table_name}"
    if where_sql:
        sql += f" WHERE {where_sql}"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return int(cur.fetchone()["cnt"])


# =========================
# Tmp_TaxInfo 入庫（舊版表）
# =========================

def insert_tmp_taxinfo_legacy(
    conn,
    rows: List[Tuple[Any, ...]],
    *,
    logger: Optional[logging.Logger] = None,
    truncate_before_insert: bool = True,
) -> int:
    """批次寫入 crawlerdb.Tmp_TaxInfo。

    rows 欄位順序必須是：
      Party_Addr, Party_ID, Parent_Party_ID, Party_Name, PaidIn_Capital, Setup_Date,
      Party_Type, Use_Invoice, Ind_Code, Ind_Name, Ind_Code1, Ind_Name1,
      Ind_Code2, Ind_Name2, Ind_Code3, Ind_Name3

    注意：舊版 tmp 是日批用，預設先 TRUNCATE。
    """
    logger = logger or get_logger()

    if truncate_before_insert:
        truncate_if_exists(conn, "crawlerdb.Tmp_TaxInfo", logger=logger)

    if not rows:
        logger.warning("⚠️ Tmp_TaxInfo 無資料可寫入")
        return 0

    sql = """
    INSERT INTO crawlerdb.Tmp_TaxInfo
    (Party_Addr, Party_ID, Parent_Party_ID, Party_Name, PaidIn_Capital, Setup_Date,
     Party_Type, Use_Invoice, Ind_Code, Ind_Name, Ind_Code1, Ind_Name1,
     Ind_Code2, Ind_Name2, Ind_Code3, Ind_Name3)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    logger.info(f"✅ Tmp_TaxInfo 入庫完成：{len(rows)} 筆")
    return len(rows)


# =========================
# tmp vs. main：新增/異動寫入 TaxInfo（保留歷史）
# =========================

def insert_diff_tmp_to_main_taxinfo(conn, *, logger: Optional[logging.Logger] = None) -> Dict[str, int]:
    """把 Tmp_TaxInfo 與 TaxInfo（每個 Party_ID 的最新一筆）比對：

    - Tmp 有、main 沒有 => 新統編：寫入 TaxInfo
    - Tmp 有、main 有但任一欄位不同 => 異動：寫入 TaxInfo

    main table：永遠只做 INSERT（歷史表）。

    回傳：{tmp_cnt, inserted_cnt, main_cnt}
    """
    logger = logger or get_logger()

    tmp_cnt = count_rows(conn, "crawlerdb.Tmp_TaxInfo")

    # 這段是「正確且比較快」的寫法核心：
    # 1) 先用子查詢抓出每個 Party_ID 的 max(Update_Time)
    # 2) 再 join 回 TaxInfo 得到最新一筆（避免 self-join t2 > t 的 N^2）
    # 3) 將 tmp 與 latest 做 null-safe 欄位比對
    insert_sql = """
    INSERT INTO crawlerdb.TaxInfo
    (Party_ID, Party_Addr, Parent_Party_ID, Party_Name, PaidIn_Capital, Setup_Date,
     Party_Type, Use_Invoice, Ind_Code, Ind_Name, Ind_Code1, Ind_Name1,
     Ind_Code2, Ind_Name2, Ind_Code3, Ind_Name3)
    SELECT
      a.Party_ID, a.Party_Addr, a.Parent_Party_ID, a.Party_Name, a.PaidIn_Capital, a.Setup_Date,
      a.Party_Type, a.Use_Invoice, a.Ind_Code, a.Ind_Name, a.Ind_Code1, a.Ind_Name1,
      a.Ind_Code2, a.Ind_Name2, a.Ind_Code3, a.Ind_Name3
    FROM crawlerdb.Tmp_TaxInfo a
    LEFT JOIN (
      SELECT Party_ID, MAX(Update_Time) AS max_ut
      FROM crawlerdb.TaxInfo
      GROUP BY Party_ID
    ) lu
      ON lu.Party_ID = a.Party_ID
    LEFT JOIN crawlerdb.TaxInfo t
      ON t.Party_ID = lu.Party_ID AND t.Update_Time = lu.max_ut
    WHERE
      t.Party_ID IS NULL
      OR NOT (
        a.Party_Addr      <=> t.Party_Addr AND
        a.Parent_Party_ID <=> t.Parent_Party_ID AND
        a.Party_Name      <=> t.Party_Name AND
        a.PaidIn_Capital  <=> t.PaidIn_Capital AND
        a.Setup_Date      <=> t.Setup_Date AND
        a.Party_Type      <=> t.Party_Type AND
        a.Use_Invoice     <=> t.Use_Invoice AND
        a.Ind_Code        <=> t.Ind_Code AND
        a.Ind_Name        <=> t.Ind_Name AND
        a.Ind_Code1       <=> t.Ind_Code1 AND
        a.Ind_Name1       <=> t.Ind_Name1 AND
        a.Ind_Code2       <=> t.Ind_Code2 AND
        a.Ind_Name2       <=> t.Ind_Name2 AND
        a.Ind_Code3       <=> t.Ind_Code3 AND
        a.Ind_Name3       <=> t.Ind_Name3
      )
    """

    with conn.cursor() as cur:
        inserted = cur.execute(insert_sql)
    conn.commit()

    main_cnt = count_rows(conn, "crawlerdb.TaxInfo")

    logger.info(f"✅ TaxInfo 寫入完成（只新增/異動）：tmp={tmp_cnt}，inserted={inserted}，TaxInfo總筆數={main_cnt}")
    return {"tmp_cnt": tmp_cnt, "inserted_cnt": int(inserted), "main_cnt": main_cnt}


def cleanup_daily_tmp_tables(conn, *, logger: Optional[logging.Logger] = None) -> None:
    """日批結束後清空 tmp 表。

    - 一定清 Tmp_TaxInfo
    - 如果存在 tmp_rawData（新版表）也一併清
    """
    logger = logger or get_logger()
    truncate_if_exists(conn, "crawlerdb.Tmp_TaxInfo", logger=logger)
    truncate_if_exists(conn, "crawlerdb.tmp_rawData", logger=logger)

