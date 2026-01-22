# -*- coding: utf-8 -*-
"""run_daily_job_v2.py

v2 需求：
- 保留「日批」模式：下載 -> 解壓 -> ETL -> tmp 入庫 -> tmp vs main 差異寫入 main
- 新增「from_csv」模式：指定已下載的 CSV 路徑，跳過下載/解壓，直接 ETL + 入庫
- 跑完後清空 tmp（以及若存在 tmp_rawData 也一併清）
- main (crawlerdb.TaxInfo) 保留所有歷史異動/新增（只新增，不更新、不刪除）

用法：
  # 日批（下載）
  python run_daily_job_v2.py daily --work-dir ...

  # 用既有 CSV（跳過下載）
  python run_daily_job_v2.py from_csv --csv ... --work-dir ...

可額外指定 run_id：
  --run-id RUN_20260122_133715

"""

from __future__ import annotations

import argparse
import os
from datetime import datetime

from crawler_etl_v2 import (
    download_and_extract,
    parse_meta_date_from_csv,
    validate_file_date_or_raise,
    etl_csv_to_tmp_taxinfo,
)

from db_loader_v2 import (
    get_logger,
    connect_mysql,
    get_mysql_settings_from_env,
    truncate_tmp_tables,
    count_tmp_taxinfo,
    insert_tmp_taxinfo,
    insert_diff_tmp_to_main_taxinfo,
)


def _build_run_id(now: datetime) -> str:
    return now.strftime("RUN_%Y%m%d_%H%M%S")


def _run_pipeline(csv_path: str, zip_path: str | None, work_dir: str, run_id: str) -> dict:
    logger = get_logger()

    cfg = get_mysql_settings_from_env()
    conn = connect_mysql(cfg)

    try:
        # 0) 每次日批都當成一次性 tmp：先清空
        truncate_tmp_tables(conn, logger=logger)

        # 1) META 日期解析 + 檢查
        file_date = parse_meta_date_from_csv(csv_path)
        logger.info(f"✅ META 日期解析：{file_date}")
        validate_file_date_or_raise(file_date)

        # 2) ETL + tmp 入庫
        rows = etl_csv_to_tmp_taxinfo(csv_path)
        insert_tmp_taxinfo(conn, rows, logger=logger)
        tmp_cnt = count_tmp_taxinfo(conn)
        logger.info(f"✅ Tmp_TaxInfo 入庫完成：{tmp_cnt} 筆")

        # 3) tmp vs main：只寫入新增/異動到 TaxInfo（保留歷史）
        merge_result = insert_diff_tmp_to_main_taxinfo(conn, logger=logger)
        logger.info(
            f"✅ TaxInfo 增量寫入完成：tmp={tmp_cnt}，inserted={merge_result['inserted']}，TaxInfo(總筆數)={merge_result['main_total']}"
        )

        # 4) 結束：清空 tmp（你要求跑完就清）
        truncate_tmp_tables(conn, logger=logger)

        return {
            "run_id": run_id,
            "zip_path": zip_path,
            "csv_path": csv_path,
            "file_date": file_date.isoformat(),
            "tmp_cnt": tmp_cnt,
            "main_inserted": merge_result["inserted"],
            "main_total": merge_result["main_total"],
        }
    finally:
        conn.close()


def cmd_daily(args: argparse.Namespace) -> None:
    logger = get_logger()

    now = datetime.now()
    run_id = args.run_id or _build_run_id(now)

    logger.info(f"START | run_id={run_id}")
    logger.info("=== 開始：下載 + 解壓 ===")

    zip_path, csv_path, downloaded_at = download_and_extract(args.work_dir)
    logger.info(f"✅ 下載完成：{zip_path}")
    logger.info(f"✅ 解壓完成：{csv_path}")

    result = _run_pipeline(csv_path=csv_path, zip_path=zip_path, work_dir=args.work_dir, run_id=run_id)
    logger.info(f"END | run_id={run_id}")
    logger.info(f"流程資訊：{result}")


def cmd_from_csv(args: argparse.Namespace) -> None:
    logger = get_logger()

    now = datetime.now()
    run_id = args.run_id or _build_run_id(now)

    csv_path = os.path.abspath(args.csv)
    zip_path = os.path.abspath(args.zip) if args.zip else None

    logger.info(f"START | run_id={run_id}")
    logger.info("=== 開始：使用既有 CSV（跳過下載/解壓） ===")
    logger.info(f"CSV：{csv_path}")
    if zip_path:
        logger.info(f"ZIP：{zip_path}")

    result = _run_pipeline(csv_path=csv_path, zip_path=zip_path, work_dir=args.work_dir, run_id=run_id)
    logger.info(f"END | run_id={run_id}")
    logger.info(f"流程資訊：{result}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="GCIS tax daily pipeline v2")
    sub = parser.add_subparsers(dest="command", required=True)

    p1 = sub.add_parser("daily", help="download zip then process")
    p1.add_argument("--work-dir", default="./work", help="where to store downloaded files")
    p1.add_argument("--run-id", default=None, help="override run_id")
    p1.set_defaults(func=cmd_daily)

    p2 = sub.add_parser("from_csv", help="process an existing CSV file")
    p2.add_argument("--csv", required=True, help="path to existing CSV")
    p2.add_argument("--zip", default=None, help="optional zip path (for logging only)")
    p2.add_argument("--work-dir", default="./work", help="where to store logs/tmp")
    p2.add_argument("--run-id", default=None, help="override run_id")
    p2.set_defaults(func=cmd_from_csv)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
