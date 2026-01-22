# -*- coding: utf-8 -*-
"""crawler_etl_v2.py

v2 重點：
- 流程沿用新版（下載/解壓/日期檢查/ETL/入庫）
- 但 DB 寫入採用舊表：crawlerdb.Tmp_TaxInfo / crawlerdb.TaxInfo

此檔只做：
1) 下載 ZIP / 解壓 CSV（檔名加時間戳）
2) CSV 第2行（META行）日期解析 + 完整性檢查
3) 讀 CSV DATA 列，做輕量 ETL，寫入 crawlerdb.Tmp_TaxInfo

後續 tmp vs. main 的比對/寫入，由 db_loader_v2.insert_diff_tmp_to_main_taxinfo() 負責。

環境變數：
- STRICT_FILE_DATE=1 (預設嚴格比對檔案日期)
- EXPECT_FILE_DATE=YYYY-MM-DD (用於回補/測試)
- ALLOW_INSECURE_SSL=1 (才允許在 SSL 憑證驗證失敗時降級 verify=False 下載)
"""

from __future__ import annotations

import csv
import os
import re
import shutil
import zipfile
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
import certifi

from db_loader_v2 import (
    connect_mysql,
    get_logger,
    get_mysql_settings_from_env,
    insert_tmp_taxinfo_legacy,
)

SOURCE_URL = "https://eip.fia.gov.tw/data/BGMOPEN1.zip"
CSV_COLS = 16


# =========================
# 基本轉換（ETL 用）
# =========================

def normalize_text_keep_spaces(s: Optional[str]) -> Optional[str]:
    """保留語意空白，只做：trim + 全形空白轉半形 + 多空白壓縮"""
    if s is None:
        return None
    s = str(s).strip()
    s = re.sub(r"[\u3000]+", " ", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s or None


def to_int_or_none(v: Optional[str]) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    if s.isdigit():
        return int(s)
    # 有些欄位可能混入空白/全形
    s2 = "".join(ch for ch in s if ch.isdigit())
    return int(s2) if s2.isdigit() else None


def roc_yyyMMdd_to_iso(v: Optional[str]) -> Optional[str]:
    """民國 yyyMMdd -> YYYY-MM-DD（不合法回 None）"""
    if not v:
        return None
    s = str(v).strip()
    if not re.fullmatch(r"\d{7}", s):
        return None
    y = int(s[:3]) + 1911
    m = int(s[3:5])
    d = int(s[5:7])
    try:
        return date(y, m, d).strftime("%Y-%m-%d")
    except ValueError:
        return None


def parse_meta_date(s: Optional[str]) -> Optional[date]:
    """解析 META 行第1欄，例如：15-JAN-26 / 02-JAN-26 -> 2026-01-15"""
    if not s:
        return None
    s = str(s).strip()
    try:
        dt = datetime.strptime(s, "%d-%b-%y")
        return dt.date()
    except ValueError:
        return None


# =========================
# 檔案日期完整性檢查（重要）
# =========================

def validate_file_date_or_raise(source_file_date: date) -> None:
    logger = get_logger()

    strict = (os.getenv("STRICT_FILE_DATE", "1").strip().lower() not in {"0", "false", "no", "off"})
    expect = (os.getenv("EXPECT_FILE_DATE") or os.getenv("BACKFILL_FILE_DATE") or "").strip()

    if not strict:
        logger.warning(f"⚠️ 已停用嚴格檔案日期檢查：source_file_date={source_file_date} (STRICT_FILE_DATE=0)")
        return

    if expect:
        try:
            expected_date = datetime.strptime(expect, "%Y-%m-%d").date()
        except ValueError:
            raise RuntimeError(f"❌ EXPECT_FILE_DATE 格式錯誤，需 YYYY-MM-DD，收到：{expect}")

        if source_file_date != expected_date:
            raise RuntimeError(
                f"❌ 檔案日期不匹配：CSV第2行日期={source_file_date}，期望日期={expected_date} (EXPECT_FILE_DATE)"
            )
        logger.info(f"✅ 檔案日期檢查通過：{source_file_date}（符合 EXPECT_FILE_DATE={expected_date}）")
        return

    today = datetime.now().date()
    if source_file_date != today:
        raise RuntimeError(f"❌ 檔案日期不匹配：CSV第2行日期={source_file_date}，批次執行日={today}")

    logger.info(f"✅ 檔案日期檢查通過：{source_file_date}（與批次執行日一致）")


# =========================
# 下載與解壓（檔名加時間戳）
# =========================

def _download_file(url: str, out_path: str, *, timeout: int = 120, max_retry: int = 3) -> None:
    """優先 verify=certifi；若仍失敗，且 ALLOW_INSECURE_SSL=1 才降級 verify=False。"""
    logger = get_logger()

    allow_insecure = (os.getenv("ALLOW_INSECURE_SSL", "0").strip().lower() in {"1", "true", "yes", "on"})

    last_err: Optional[Exception] = None
    for attempt in range(1, max_retry + 1):
        try:
            logger.info(f"下載中：{url} (attempt={attempt}/{max_retry}, verify=certifi)")
            with requests.get(url, stream=True, timeout=timeout, verify=certifi.where()) as r:
                r.raise_for_status()
                with open(out_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            return
        except requests.exceptions.SSLError as e:
            logger.warning(f"SSL 驗證失敗：{e}")
            last_err = e
        except Exception as e:
            logger.warning(f"下載失敗：{e}")
            last_err = e

    if allow_insecure:
        logger.warning("⚠️ 將降級為 verify=False 下載（ALLOW_INSECURE_SSL=1）。這是不安全的，只建議在內網/臨時救火使用。")
        with requests.get(url, stream=True, timeout=timeout, verify=False) as r:
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        return

    raise RuntimeError(f"下載失敗（重試 {max_retry} 次仍失敗）：{last_err}")


def download_and_extract(work_dir: str) -> Tuple[str, str, datetime]:
    """回傳：local_zip_path, local_csv_path, downloaded_at"""
    os.makedirs(work_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    zip_name = f"BGMOPEN1_{ts}.zip"
    csv_name = f"BGMOPEN1_{ts}.csv"
    zip_path = os.path.join(work_dir, zip_name)
    csv_path = os.path.join(work_dir, csv_name)

    downloaded_at = datetime.now()

    _download_file(SOURCE_URL, zip_path, timeout=120, max_retry=3)

    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.namelist()
        target = None
        for m in members:
            if m.lower().endswith("bgmopen1.csv"):
                target = m
                break
        if not target:
            raise RuntimeError(f"ZIP 內找不到 BGMOPEN1.csv，內容：{members[:10]} ...")

        extracted_path = zf.extract(target, path=work_dir)
        shutil.move(extracted_path, csv_path)

    return zip_path, csv_path, downloaded_at


# =========================
# CSV -> Tmp_TaxInfo（舊版 tmp）
# =========================

def etl_csv_to_legacy_tmp(
    conn,
    *,
    csv_path: str,
    strict_date_check: bool = True,
) -> Dict[str, Any]:
    """讀 CSV -> 解析 META 日期 -> 驗證 -> ETL -> 寫入 crawlerdb.Tmp_TaxInfo"""
    logger = get_logger()

    file_date: Optional[date] = None
    data_rows: List[Tuple[Any, ...]] = []
    data_cnt = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for idx, row in enumerate(reader, start=1):
            row = (row + [""] * CSV_COLS)[:CSV_COLS]

            if idx == 1:
                # header
                continue
            if idx == 2:
                file_date = parse_meta_date(row[0])
                continue

            # DATA
            data_cnt += 1
            # 欄位映射（同舊 code）
            party_addr = normalize_text_keep_spaces(row[0]) or ""  # 舊 DDL: NOT NULL
            party_id = normalize_text_keep_spaces(row[1])
            if not party_id:
                continue  # 統編空的直接跳過

            parent_party_id = to_int_or_none(row[2])
            party_name = normalize_text_keep_spaces(row[3])
            paidin_capital = to_int_or_none(row[4])
            setup_date = roc_yyyMMdd_to_iso(row[5])
            party_type = normalize_text_keep_spaces(row[6])
            use_invoice = (normalize_text_keep_spaces(row[7]) or None)

            ind_code = to_int_or_none(row[8])
            ind_name = normalize_text_keep_spaces(row[9])
            ind_code1 = to_int_or_none(row[10])
            ind_name1 = normalize_text_keep_spaces(row[11])
            ind_code2 = to_int_or_none(row[12])
            ind_name2 = normalize_text_keep_spaces(row[13])
            ind_code3 = to_int_or_none(row[14])
            ind_name3 = normalize_text_keep_spaces(row[15])

            data_rows.append(
                (
                    party_addr,
                    party_id,
                    parent_party_id,
                    party_name,
                    paidin_capital,
                    setup_date,
                    party_type,
                    use_invoice,
                    ind_code,
                    ind_name,
                    ind_code1,
                    ind_name1,
                    ind_code2,
                    ind_name2,
                    ind_code3,
                    ind_name3,
                )
            )

    if not file_date:
        raise RuntimeError("CSV META 行日期解析失敗：第2行第1欄不是預期格式（例如 15-JAN-26）。")

    if strict_date_check:
        validate_file_date_or_raise(file_date)

    inserted = insert_tmp_taxinfo_legacy(conn, data_rows, logger=logger)

    return {
        "file_date": file_date.strftime("%Y-%m-%d"),
        "csv_data_rows": data_cnt,
        "tmp_inserted": inserted,
    }


# =========================
# 對外入口：抓檔 + ETL -> Tmp_TaxInfo
# =========================

def run_download_and_etl_to_tmp(work_dir: str, run_id: str) -> Dict[str, Any]:
    logger = get_logger()
    logger.info("=== 開始：下載 + 解壓 + ETL -> Tmp_TaxInfo ===")

    zip_path, csv_path, downloaded_at = download_and_extract(work_dir)
    logger.info(f"✅ 下載完成：{zip_path}")
    logger.info(f"✅ 解壓完成：{csv_path}")

    cfg = get_mysql_settings_from_env()
    conn = connect_mysql(cfg)

    try:
        info = etl_csv_to_legacy_tmp(conn, csv_path=csv_path, strict_date_check=True)
        logger.info(f"✅ META 日期解析：{info['file_date']}")
        logger.info(f"✅ Tmp_TaxInfo 入庫完成：{info['tmp_inserted']} 筆")

        return {
            "run_id": run_id,
            "zip_path": zip_path,
            "csv_path": csv_path,
            "downloaded_at": downloaded_at.strftime("%Y-%m-%d %H:%M:%S"),
            **info,
        }
    finally:
        conn.close()
        logger.info("=== 結束：下載 + 解壓 + ETL -> Tmp_TaxInfo ===")


def run_etl_existing_csv(csv_path: str, run_id: str, *, strict_date_check: bool = False) -> Dict[str, Any]:
    """跳過下載/解壓：直接用本機 csv 跑 ETL -> Tmp_TaxInfo"""
    logger = get_logger()
    logger.info("=== 開始：ETL(既有 CSV) -> Tmp_TaxInfo ===")
    cfg = get_mysql_settings_from_env()
    conn = connect_mysql(cfg)

    try:
        info = etl_csv_to_legacy_tmp(conn, csv_path=csv_path, strict_date_check=strict_date_check)
        logger.info(f"✅ META 日期解析：{info['file_date']}")
        logger.info(f"✅ Tmp_TaxInfo 入庫完成：{info['tmp_inserted']} 筆")

        return {
            "run_id": run_id,
            "csv_path": csv_path,
            "downloaded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            **info,
        }
    finally:
        conn.close()
        logger.info("=== 結束：ETL(既有 CSV) -> Tmp_TaxInfo ===")
