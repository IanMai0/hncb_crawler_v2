# -*- coding: utf-8 -*-
"""
db_loader.py
- MySQL 連線
- 批次寫入 tmp_rawData / tmp_taxInfo
- merge 寫入 taxInfo（只落新/異動）
- 統一 logs function
"""

from __future__ import annotations

import os
import sys
import json
import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pymysql


# =========================
# Logs Function（共用）
# =========================
def get_logger(name: str = "gcis_pipeline") -> logging.Logger:
    """
    統一 Logger：console + 檔案
    - LOG_DIR 預設 ./logs
    - 檔名：gcis_pipeline_YYYYMMDD.log
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    log_dir = os.getenv("LOG_DIR", os.path.join(os.getcwd(), "logs"))
    os.makedirs(log_dir, exist_ok=True)

    today = datetime.now().strftime("%Y%m%d")
    log_path = os.path.join(log_dir, f"{name}_{today}.log")

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)

    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger


# =========================
# DB Settings / Connection
# =========================
@dataclass(frozen=True)
class MySQLSettings:
    host: str
    user: str
    password: str
    database: str
    port: int = 3306
    charset: str = "utf8mb4"


def get_mysql_settings_from_env() -> MySQLSettings:
    return MySQLSettings(
        host=os.getenv("MYSQL_HOST", ""),
        user=os.getenv("MYSQL_USER", ""),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DB", ""),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        charset=os.getenv("MYSQL_CHARSET", "utf8mb4"),
    )


def connect_mysql(cfg: MySQLSettings) -> pymysql.connections.Connection:
    if not all([cfg.host, cfg.user, cfg.password, cfg.database]):
        raise ValueError("MySQL 環境變數不足：MYSQL_HOST/USER/PASSWORD/DB 必填。")

    conn = pymysql.connect(
        host=cfg.host,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        port=cfg.port,
        charset=cfg.charset,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )
    return conn


# =========================
# Helpers
# =========================
def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def make_taxinfo_row_hash(row: Dict[str, Any]) -> str:
    """
    用清洗後欄位計算 hash（只要欄位內容同，就視為同版本）
    注意：不要把 run_id / etl_at 這些會變的東西納入
    """
    keys = [
        "party_addr", "party_id", "parent_party_id", "party_name",
        "paidin_capital", "setup_date", "party_type", "use_invoice",
        "ind_code", "ind_name", "ind_code1", "ind_name1",
        "ind_code2", "ind_name2", "ind_code3", "ind_name3",
    ]
    payload = {k: row.get(k) for k in keys}
    return sha256_hex(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str))


# =========================
# Load tmp_rawData
# =========================
def insert_tmp_rawdata(
    conn: pymysql.connections.Connection,
    rows: List[Tuple[Any, ...]],
    logger: logging.Logger,
    batch_size: int = 2000
) -> int:
    """
    rows tuple 格式：
    (run_id, source_url, local_zip_path, local_csv_path, downloaded_at, file_date,
     row_num, row_type, c01..c16)
    """
    if not rows:
        return 0

    sql = """
    INSERT INTO crawlerdb.tmp_rawData
    (run_id, source_url, local_zip_path, local_csv_path, downloaded_at, file_date,
     row_num, row_type,
     c01,c02,c03,c04,c05,c06,c07,c08,c09,c10,c11,c12,c13,c14,c15,c16)
    VALUES
    (%s,%s,%s,%s,%s,%s,
     %s,%s,
     %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    total = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            cur.executemany(sql, chunk)
            total += len(chunk)
        conn.commit()

    logger.info(f"✅ tmp_rawData 入庫完成：{total} 列")
    return total


# =========================
# Load tmp_taxInfo
# =========================
def insert_tmp_taxinfo(
    conn: pymysql.connections.Connection,
    rows: List[Dict[str, Any]],
    logger: logging.Logger,
    batch_size: int = 2000
) -> int:
    if not rows:
        return 0

    sql = """
    INSERT INTO crawlerdb.tmp_taxInfo
    (run_id, source_file_date, row_num,
     party_addr, party_id, parent_party_id, party_name, paidin_capital, setup_date,
     party_type, use_invoice,
     ind_code, ind_name, ind_code1, ind_name1, ind_code2, ind_name2, ind_code3, ind_name3,
     row_hash)
    VALUES
    (%(run_id)s, %(source_file_date)s, %(row_num)s,
     %(party_addr)s, %(party_id)s, %(parent_party_id)s, %(party_name)s, %(paidin_capital)s, %(setup_date)s,
     %(party_type)s, %(use_invoice)s,
     %(ind_code)s, %(ind_name)s, %(ind_code1)s, %(ind_name1)s, %(ind_code2)s, %(ind_name2)s, %(ind_code3)s, %(ind_name3)s,
     %(row_hash)s)
    """

    # 先補 hash
    for r in rows:
        r["row_hash"] = make_taxinfo_row_hash(r)

    total = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            cur.executemany(sql, chunk)
            total += len(chunk)
        conn.commit()

    logger.info(f"✅ tmp_taxInfo 入庫完成：{total} 筆")
    return total


# =========================
# Merge to main table taxInfo (history)
# =========================
def upsert_latest_taxinfo(
    conn,
    run_id: str,
    logger
) -> dict:
    # tmp 筆數
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM crawlerdb.tmp_taxInfo WHERE run_id=%s", (run_id,))
        tmp_cnt = int(cur.fetchone()["c"])

    upsert_sql = """
    INSERT INTO crawlerdb.taxInfo (
      party_id, party_addr, parent_party_id, party_name, paidin_capital, setup_date,
      party_type, use_invoice,
      ind_code, ind_name, ind_code1, ind_name1, ind_code2, ind_name2, ind_code3, ind_name3,
      row_hash, source_file_date, last_run_id
    )
    SELECT
      t.party_id, t.party_addr, t.parent_party_id, t.party_name, t.paidin_capital, t.setup_date,
      t.party_type, t.use_invoice,
      t.ind_code, t.ind_name, t.ind_code1, t.ind_name1, t.ind_code2, t.ind_name2, t.ind_code3, t.ind_name3,
      t.row_hash, t.source_file_date, t.run_id
    FROM crawlerdb.tmp_taxInfo t
    WHERE t.run_id = %s
    ON DUPLICATE KEY UPDATE
      party_addr      = IF(VALUES(row_hash) <> row_hash, VALUES(party_addr), party_addr),
      parent_party_id = IF(VALUES(row_hash) <> row_hash, VALUES(parent_party_id), parent_party_id),
      party_name      = IF(VALUES(row_hash) <> row_hash, VALUES(party_name), party_name),
      paidin_capital  = IF(VALUES(row_hash) <> row_hash, VALUES(paidin_capital), paidin_capital),
      setup_date      = IF(VALUES(row_hash) <> row_hash, VALUES(setup_date), setup_date),
      party_type      = IF(VALUES(row_hash) <> row_hash, VALUES(party_type), party_type),
      use_invoice     = IF(VALUES(row_hash) <> row_hash, VALUES(use_invoice), use_invoice),
      ind_code        = IF(VALUES(row_hash) <> row_hash, VALUES(ind_code), ind_code),
      ind_name        = IF(VALUES(row_hash) <> row_hash, VALUES(ind_name), ind_name),
      ind_code1       = IF(VALUES(row_hash) <> row_hash, VALUES(ind_code1), ind_code1),
      ind_name1       = IF(VALUES(row_hash) <> row_hash, VALUES(ind_name1), ind_name1),
      ind_code2       = IF(VALUES(row_hash) <> row_hash, VALUES(ind_code2), ind_code2),
      ind_name2       = IF(VALUES(row_hash) <> row_hash, VALUES(ind_name2), ind_name2),
      ind_code3       = IF(VALUES(row_hash) <> row_hash, VALUES(ind_code3), ind_code3),
      ind_name3       = IF(VALUES(row_hash) <> row_hash, VALUES(ind_name3), ind_name3),
      source_file_date= IF(VALUES(row_hash) <> row_hash, VALUES(source_file_date), source_file_date),
      last_run_id     = IF(VALUES(row_hash) <> row_hash, VALUES(last_run_id), last_run_id),
      updated_at      = IF(VALUES(row_hash) <> row_hash, CURRENT_TIMESTAMP(6), updated_at),
      row_hash        = IF(VALUES(row_hash) <> row_hash, VALUES(row_hash), row_hash);
    """

    with conn.cursor() as cur:
        affected = cur.execute(upsert_sql, (run_id,))
        conn.commit()

    # 最新表總筆數（應該約等於 unique party_id 數）
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM crawlerdb.taxInfo")
        main_cnt = int(cur.fetchone()["c"])

    logger.info(f"✅ taxInfo UPSERT 完成：tmp={tmp_cnt}，affected={affected}，taxInfo(最新表)總筆數={main_cnt}")
    return {"tmp_cnt": tmp_cnt, "affected": int(affected), "main_cnt": main_cnt}

