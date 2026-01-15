# -*- coding: utf-8 -*-
"""
crawler_etl.py
- 下載 ZIP / 解壓 CSV（檔名加時間戳）
- CSV 第2行（META行）日期檢查
- 原封不動寫入 tmp_rawData
- 由 raw 做 ETL，寫入 tmp_taxInfo
"""

from __future__ import annotations

import os
import csv
import re
import shutil
import zipfile
import urllib.request
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

from db_loader import (
    get_logger,
    connect_mysql,
    get_mysql_settings_from_env,
    insert_tmp_rawdata,
    insert_tmp_taxinfo,
)

SOURCE_URL = "https://eip.fia.gov.tw/data/BGMOPEN1.zip"

# 你的 CSV 有 16 欄
CSV_COLS = 16


# =========================
# 基本轉換（ETL 用）
# =========================
def normalize_text_keep_spaces(s: Optional[str]) -> Optional[str]:
    """
    保留語意空白，只做：
    - 去頭尾
    - 全形空白 -> 半形空白
    - 多空白壓縮
    """
    if s is None:
        return None
    s = str(s).strip()
    s = re.sub(r"[\u3000]+", " ", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s or None


def only_digits(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    d = "".join(ch for ch in str(s) if ch.isdigit())
    return d or None


def to_int_or_none(v: Optional[str]) -> Optional[int]:
    if v is None:
        return None
    v = str(v).strip()
    if v.isdigit():
        return int(v)
    return None


def roc_yyyMMdd_to_date(v: Optional[str]) -> Optional[date]:
    """
    民國 yyyMMdd -> date；不合法回 None
    例：1040413 -> 2015-04-13
    """
    if not v:
        return None
    s = str(v).strip()
    if not re.fullmatch(r"\d{7}", s):
        return None
    y = int(s[:3]) + 1911
    m = int(s[3:5])
    d = int(s[5:7])
    try:
        return date(y, m, d)
    except ValueError:
        return None


def parse_meta_date(s: Optional[str]) -> Optional[date]:
    """
    解析 META 行第1欄，例如：15-JAN-26 / 02-JAN-26
    -> 2026-01-15
    """
    if not s:
        return None
    s = str(s).strip()
    # 格式：DD-MON-YY
    try:
        dt = datetime.strptime(s, "%d-%b-%y")
        return dt.date()
    except ValueError:
        return None


# =========================
# 下載與解壓（檔名加時間戳）
# =========================
def download_and_extract(work_dir: str) -> Tuple[str, str, datetime]:
    """
    回傳：
      local_zip_path, local_csv_path, downloaded_at
    """
    os.makedirs(work_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    zip_name = f"BGMOPEN1_{ts}.zip"
    csv_name = f"BGMOPEN1_{ts}.csv"
    zip_path = os.path.join(work_dir, zip_name)
    csv_path = os.path.join(work_dir, csv_name)

    downloaded_at = datetime.now()

    # 下載
    urllib.request.urlretrieve(SOURCE_URL, zip_path)

    # 解壓：只取 BGMOPEN1.csv，並改名為帶時間戳的檔名
    with zipfile.ZipFile(zip_path, "r") as zf:
        # 嘗試找出 zip 內的 csv（通常叫 BGMOPEN1.csv）
        members = zf.namelist()
        target = None
        for m in members:
            if m.lower().endswith("bgmopen1.csv"):
                target = m
                break
        if not target:
            raise RuntimeError(f"ZIP 內找不到 BGMOPEN1.csv，內容：{members[:10]} ...")

        extracted_path = zf.extract(target, path=work_dir)
        # 改名成帶時間戳的 csv
        shutil.move(extracted_path, csv_path)

    return zip_path, csv_path, downloaded_at


# =========================
# CSV -> tmp_rawData（原封不動）
# =========================
def csv_to_tmp_raw(conn, run_id: str, zip_path: str, csv_path: str, downloaded_at: datetime) -> date:
    logger = get_logger()
    raw_rows: List[Tuple[Any, ...]] = []

    file_date: Optional[date] = None

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)

        for idx, row in enumerate(reader, start=1):
            # 補齊/截斷到固定 16 欄（避免欄數不齊爆炸）
            row = (row + [""] * CSV_COLS)[:CSV_COLS]

            if idx == 1:
                row_type = "HEADER"
            elif idx == 2:
                row_type = "META"
                file_date = parse_meta_date(row[0])
            else:
                row_type = "DATA"

            raw_rows.append((
                run_id,
                SOURCE_URL,
                zip_path,
                csv_path,
                downloaded_at.strftime("%Y-%m-%d %H:%M:%S"),
                file_date.strftime("%Y-%m-%d") if file_date else None,
                idx,
                row_type,
                row[0], row[1], row[2], row[3],
                row[4], row[5], row[6], row[7],
                row[8], row[9], row[10], row[11],
                row[12], row[13], row[14], row[15]
            ))

    insert_tmp_rawdata(conn, raw_rows, logger=logger)

    if not file_date:
        raise RuntimeError("CSV META 行日期解析失敗：第2行第1欄不是預期格式（例如 15-JAN-26）。")

    return file_date


# =========================
# tmp_rawData -> ETL -> tmp_taxInfo
# =========================
def raw_to_clean_etl(conn, run_id: str, source_file_date: date) -> int:
    logger = get_logger()

    # 讀 raw 的 DATA 行（row_type=DATA）
    sql = """
    SELECT row_num,
           c01 AS party_addr, c02 AS party_id, c03 AS parent_party_id, c04 AS party_name,
           c05 AS paidin_capital, c06 AS setup_date, c07 AS party_type, c08 AS use_invoice,
           c09 AS ind_code, c10 AS ind_name, c11 AS ind_code1, c12 AS ind_name1,
           c13 AS ind_code2, c14 AS ind_name2, c15 AS ind_code3, c16 AS ind_name3
    FROM crawlerdb.tmp_rawData
    WHERE run_id=%s AND row_type='DATA'
    ORDER BY row_num
    """

    clean_rows: List[Dict[str, Any]] = []
    with conn.cursor() as cur:
        cur.execute(sql, (run_id,))
        for r in cur.fetchall():
            party_id = normalize_text_keep_spaces(r.get("party_id"))
            if not party_id:
                # 統編空的，直接跳過（也可以改成寫入異常表）
                continue

            row: Dict[str, Any] = {
                "run_id": run_id,
                "source_file_date": source_file_date.strftime("%Y-%m-%d"),
                "row_num": int(r["row_num"]),
                "party_addr": normalize_text_keep_spaces(r.get("party_addr")),
                "party_id": party_id,
                "parent_party_id": normalize_text_keep_spaces(r.get("parent_party_id")),
                "party_name": normalize_text_keep_spaces(r.get("party_name")),
                "paidin_capital": to_int_or_none(r.get("paidin_capital")),
                "setup_date": roc_yyyMMdd_to_date(r.get("setup_date")),
                "party_type": normalize_text_keep_spaces(r.get("party_type")),
                "use_invoice": (normalize_text_keep_spaces(r.get("use_invoice")) or None),
                "ind_code": normalize_text_keep_spaces(r.get("ind_code")),
                "ind_name": normalize_text_keep_spaces(r.get("ind_name")),
                "ind_code1": normalize_text_keep_spaces(r.get("ind_code1")),
                "ind_name1": normalize_text_keep_spaces(r.get("ind_name1")),
                "ind_code2": only_digits(r.get("ind_code2")),
                "ind_name2": normalize_text_keep_spaces(r.get("ind_name2")),
                "ind_code3": normalize_text_keep_spaces(r.get("ind_code3")),
                "ind_name3": normalize_text_keep_spaces(r.get("ind_name3")),
            }

            # setup_date 轉成 YYYY-MM-DD（MySQL DATE）
            if isinstance(row["setup_date"], date):
                row["setup_date"] = row["setup_date"].strftime("%Y-%m-%d")
            else:
                row["setup_date"] = None

            clean_rows.append(row)

    # 寫入 tmp_taxInfo（不清空，因為用 run_id 區分）
    inserted = insert_tmp_taxinfo(conn, clean_rows, logger=logger)
    return inserted


# =========================
# 檔案日期完整性檢查（重要）
# =========================
def validate_file_date_or_raise(source_file_date: date) -> None:
    logger = get_logger()

    today = datetime.now().date()
    if source_file_date != today:
        # 你要的是「匹配」，我這裡採用嚴格模式：不匹配就擋住（避免寫錯日）
        msg = f"❌ 檔案日期不匹配：CSV第2行日期={source_file_date}，批次執行日={today}"
        logger.error(msg)
        raise RuntimeError(msg)

    logger.info(f"✅ 檔案日期檢查通過：{source_file_date}（與批次執行日一致）")


# =========================
# 對外入口：抓檔 + raw 入庫 + ETL 產 clean tmp
# =========================
def run_download_raw_etl(work_dir: str, run_id: str) -> Dict[str, Any]:
    logger = get_logger()
    logger.info("=== 開始：下載 + raw 入庫 + ETL ===")

    zip_path, csv_path, downloaded_at = download_and_extract(work_dir)
    logger.info(f"✅ 下載完成：{zip_path}")
    logger.info(f"✅ 解壓完成：{csv_path}")

    cfg = get_mysql_settings_from_env()
    conn = connect_mysql(cfg)

    try:
        file_date = csv_to_tmp_raw(conn, run_id, zip_path, csv_path, downloaded_at)
        logger.info(f"✅ META 日期解析：{file_date}")

        # 檔案完整性 / 日期檢查（嚴格擋）
        validate_file_date_or_raise(file_date)

        clean_cnt = raw_to_clean_etl(conn, run_id, file_date)
        logger.info(f"✅ ETL 完成：tmp_taxInfo 筆數={clean_cnt}")

        return {
            "run_id": run_id,
            "zip_path": zip_path,
            "csv_path": csv_path,
            "downloaded_at": downloaded_at.strftime("%Y-%m-%d %H:%M:%S"),
            "file_date": file_date.strftime("%Y-%m-%d"),
            "clean_cnt": clean_cnt,
        }
    finally:
        conn.close()
        logger.info("=== 結束：下載 + raw 入庫 + ETL ===")
