import os
import sys
import time
import argparse
import logging
from pathlib import Path
from typing import Optional

import requests
import zipfile
import datetime

# === ETL 元件（沿用現有模組）===
from lab_factory_etl_v5 import (
    DataPreprocessor, DataCleaner, DataAnomalyReporter,
    StatisticalSummaryEngine, Output
)  # 來源：  :contentReference[oaicite:2]{index=2}

# ===== Logger 設定 =====
LOG_FILE = "./logs/factory_run.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
              logging.StreamHandler()]
)
logger = logging.getLogger("main")

# ===== 下載與解壓（沿用原本流程，調整成可重用函式）=====
def download_and_extract_zip(url: str, extract_to: str = "./data") -> str:
    """
    下載 zip → 解壓 → 檔名加上時間戳；回傳「單一解壓出的檔名」。
    若 zip 內多檔，會回傳最後一個改名後的檔名。
    來源邏輯：流程控制檔。 :contentReference[oaicite:3]{index=3}
    """
    os.makedirs(extract_to, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename = f"{ts}.zip"
    zip_path = os.path.join(extract_to, zip_filename)

    # HEAD 檢查
    try:
        head = requests.head(url, timeout=5, allow_redirects=True)
        if head.status_code != 200:
            raise RuntimeError(f"URL 狀態碼 {head.status_code}")
    except requests.RequestException as e:
        raise RuntimeError(f"URL 無法連線: {e}")

    logger.info(f"開始下載：{url} → {zip_path}")
    resp = requests.get(url, stream=True, timeout=30)
    resp.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    logger.info(f"下載完成：{zip_path}")

    last_new_name: Optional[str] = None
    with zipfile.ZipFile(zip_path, "r") as z:
        for member in z.namelist():
            extracted_path = z.extract(member, path=extract_to)
            dir_, fname = os.path.split(extracted_path)
            new_name = f"{ts}_{fname}"
            os.replace(extracted_path, os.path.join(dir_, new_name))
            last_new_name = new_name
            logger.info(f"解壓並重命名：{member} → {new_name}")

    if not last_new_name:
        raise RuntimeError("ZIP 內沒有檔案")

    logger.info("✅ 下載與解壓完成")
    return last_new_name

# ===== 單次 ETL 執行 =====
def run_etl_from_csv(csv_path: str, make_top100: bool = True) -> str:
    """
    讀取 CSV → 預處理 → 清洗/異常 → 統計 → 匯出
    回傳輸出的整理後檔名。
    內部呼叫：lab_factory_etl_v5 的類別。 :contentReference[oaicite:4]{index=4}
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"找不到檔案：{csv_path}")

    logger.info(f"ETL 開始：{csv_path}")
    preprocessor = DataPreprocessor()
    df = preprocessor.preprocess(str(csv_path))

    df_after = DataCleaner(df).convert_and_handle_errors()
    df_after = DataAnomalyReporter(df_after).execute()

    if make_top100:
        StatisticalSummaryEngine(df_after).output_top_100_factory_holders(
            output_path="output/top_100_companies.csv"
        )

    # 匯出處理後資料
    out = Output(df=df_after, TargetFileName=csv_path.name)
    out.output_data_to_csv()
    logger.info("ETL 完成")
    return f"output/處理後_{csv_path.name}"

# ===== 入口（兩種模式：1) 指定 CSV；2) 給 ZIP URL 先下載再跑）=====
def parse_args():
    p = argparse.ArgumentParser(description="Factory ETL runner")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--csv", help="直接指定已下載好的 CSV 檔路徑")
    g.add_argument("--zip-url", help="遠端 ZIP 連結，先下載再執行 ETL")
    p.add_argument("--no-top100", action="store_true", help="不輸出 top_100_companies.csv")
    p.add_argument("--download-dir", default="./data", help="ZIP 解壓目標資料夾")
    return p.parse_args()

def main():
    args = parse_args()
    try:
        if args.csv:
            csv_name = Path(args.csv)
        else:
            csv_basename = download_and_extract_zip(args.zip_url, extract_to=args.download_dir)
            csv_name = Path(args.download_dir) / csv_basename

        output_path = run_etl_from_csv(str(csv_name), make_top100=not args.no_top100)
        logger.info(f"✅ 全流程完成，輸出：{output_path}")
    except Exception as e:
        logger.exception(f"流程失敗：{e}")
        sys.exit(1)

if __name__ == "__main__":
    main()


