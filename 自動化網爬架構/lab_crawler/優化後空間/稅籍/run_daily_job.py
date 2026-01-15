# -*- coding: utf-8 -*-
"""
run_daily_job.py
- Windows / AWS EC2 排程入口
- 依序執行：
  1) 下載 + raw 入庫 + ETL
  2) 驗證（row count 等）
  3) merge 到 main table taxInfo
"""

from __future__ import annotations

import os
from datetime import datetime

from db_loader import (
    get_logger,
    connect_mysql,
    get_mysql_settings_from_env,
    merge_tmp_to_main_taxinfo,
)
from crawler_etl import run_download_raw_etl


def make_run_id() -> str:
    # 你要查驗：run_id 可直接對應 log/DB
    return datetime.now().strftime("RUN_%Y%m%d_%H%M%S")


def verify_counts(conn, run_id: str, logger):
    """
    最小驗證：
    - tmp_rawData data row count
    - tmp_taxInfo count
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM crawlerdb.tmp_rawData WHERE run_id=%s AND row_type='DATA'", (run_id,))
        raw_data_cnt = int(cur.fetchone()["c"])

        cur.execute("SELECT COUNT(*) AS c FROM crawlerdb.tmp_taxInfo WHERE run_id=%s", (run_id,))
        clean_cnt = int(cur.fetchone()["c"])

    logger.info(f"[核對] tmp_rawData(DATA)={raw_data_cnt}，tmp_taxInfo={clean_cnt}")

    # 你要嚴謹可以加更多 gate，例如 clean_cnt 不得為 0
    if clean_cnt <= 0:
        raise RuntimeError("❌ 驗證失敗：tmp_taxInfo 筆數為 0，停止寫入 main table。")


def main():
    logger = get_logger()

    work_dir = os.getenv("WORK_DIR", os.path.join(os.getcwd(), "work"))
    os.makedirs(work_dir, exist_ok=True)

    run_id = make_run_id()
    logger.info(f"START | run_id={run_id}")

    # 1) 下載 + raw 入庫 + ETL
    result = run_download_raw_etl(work_dir=work_dir, run_id=run_id)
    logger.info(f"流程資訊：{result}")

    # 2) 驗證 + 3) merge
    cfg = get_mysql_settings_from_env()
    conn = connect_mysql(cfg)
    try:
        verify_counts(conn, run_id, logger)
        merge_result = merge_tmp_to_main_taxinfo(conn, run_id, logger)
        logger.info(f"✅ main merge 結果：{merge_result}")
    finally:
        conn.close()

    logger.info(f"END | run_id={run_id}")


if __name__ == "__main__":
    main()
