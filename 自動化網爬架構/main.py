import time
import logging
import time
import requests
import datetime
import zipfile
import os
import sys
import json, re, argparse
from typing import List, Dict, Any
import csv

# === import API ===
import app

# === import 工具 ===
from lab_crawler.gcis import switchIP as switch_IP  # IP 切換
from lab_crawler.gcis import DailyQuota  # 控制每日使用限制
from lab_crawler.gcis import export_pending_ids as export_pending_ids # 輸出尚未完成名單

# === import lab crawler ===
# 工廠_流程控制
from lab_crawler.factory_flow_control import download_and_extract_zip  # 流程控制
# 工廠_ETL
from lab_crawler.factory_etl import DataPreprocessor
from lab_crawler.factory_etl import DataCleaner
from lab_crawler.factory_etl import DataAnomalyReporter
from lab_crawler.factory_etl import StatisticalSummaryEngine
from lab_crawler.factory_etl import Output
# 工廠_sql
from lab_crawler.factory_sql import get_engine
from lab_crawler.factory_sql import stage_to_stage2  # 依規則從 stage 匯入到 stage2（合格資料）
from lab_crawler.factory_sql import stage_to_error_log  # 依規則將不合格資料寫入 error_log
from lab_crawler.factory_sql import create_target_table_if_needed  # 建表_目標查詢統編 to DB
from lab_crawler.factory_sql import load_target_from_csv  # 將 CSV 的「統一編號」寫入 target250808
# 國貿局網爬
from lab_crawler.crawler_tradeAdmin import read_DB as tradeAdmin_read_DB_target  # import 目標處裡對象統編
# GCIS
from lab_crawler.gcis import run_crawler_company_info as gcis_com_info  # 商工商業
from lab_crawler.gcis import run_crawler_business_info as gcis_bus_info  # 商工公司

from lab_crawler import gcis
from lab_crawler import tax
from lab_crawler import moea
from lab_crawler import pcc
from lab_crawler import ppstrq

# 排程
class  OrchestratorLayer:
    def __init__(self):
        self.logger_factory = None

    def factory(self):
        try:
            # 1. 自動下載工廠數據 (內建檢查數據源網址異常)
            ZIP_URL = (
                "https://serv.gcis.nat.gov.tw/RDownLoad/Data/statistical/"
                "%E7%94%9F%E7%94%A2%E4%B8%AD%E5%B7%A5%E5%BB%A0%E6%B8%85%E5%86%8A.zip"
            )
            staging_dir = "./data/input"
            self.logger_factory.info(f"{'=' * 33}\n=== ETL 開始 ===")
            # 1.1 下載 / 解壓 / 回傳檔名
            TargetFileName = download_and_extract_zip(ZIP_URL, extract_to=staging_dir)
            return TargetFileName
        except Exception as e:
                self.logger_factory.exception("ETL 遇到例外，自動下載工廠數據 中斷：")

    def tax_daily_v2(self):
        pass

# 爬蟲
class CrawlerLayer:
    def __init__(self):
        pass

    # 國貿局
    def tradeAdmin(self):
        pass

    # 商工公司
    def gcis_company(self, intputPath: str, outputPath: str):
        """
        intputPath: 目標處裡統編 csv file
        outputPath: 數據輸出 csv file
        """
        gcis_com_info(
            path=intputPath,
            out_c_path=outputPath
        )

    # 商工商業
    def gcis_business(self, intputPath: str, outputPath: str):
        """
        intputPath: 目標處裡統編 csv file
        outputPath: 數據輸出 csv file
        """
        gcis_bus_info(
            path=intputPath,
            out_b=outputPath
        )

# ETL
class ETLLayer:
    def __init__(self):
        self.logger_factory = None

    def factory(self, TargetFileName):
        try:
            # 2. 資料預處裡
            preprocessor = DataPreprocessor()
            df = preprocessor.preprocess(csv_path=rf"./data/{TargetFileName}")
            # df = preprocessor.preprocess(csv_path=rf"./data/20250822_104820_11404.csv")  # 手動用
            self.logger_factory.info(f"資料預處理 完成")
            try:
                # 2.1 資料清洗與異常處裡
                df_after = DataCleaner(df).convert_and_handle_errors()
                df_after = DataAnomalyReporter(df_after).execute()
                self.logger_factory.info(f"資料清洗與異常處理 完成")
                try:
                    # 2.2 敘述性統計
                    print("\n" + "=" * 40 + "\n")
                    StatisticalSummaryEngine(df_after).execute()  # 執行敘述性統計
                    time.sleep(1)
                    self.logger_factory.info(f"資料敘述性統計 完成")
                    try:
                        # 2.3 匯出整理後資料成 CSV
                        output = Output(df=df_after, TargetFileName=TargetFileName)
                        output.output_data_to_csv()
                        self.logger_factory.info(f"匯出整理後資料成 CSV 完成")
                    except Exception as e:
                        self.logger_factory.exception("ETL 遇到例外，匯出整理後資料成 CSV 中斷：")
                except Exception as e:
                    self.logger_factory.exception("ETL 遇到例外，敘述性統計 中斷：")
            except Exception as e:
                self.logger_factory.exception("ETL 遇到例外，清洗與異常處理 中斷：")
        except Exception as e:
            self.logger_factory.exception("ETL 遇到例外，資料預處理 中斷：")



# ========== 儲存層 ==========
class StorageLayer:
    """儲存層：集中處理寫入 DB"""
    def __init__(self, db_client):
        self.db = db_client

    # 初始化, 環境變數 or config
    def initialize(self):
        DB_HOST = os.getenv("DB_HOST")
        DB_USER = os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_NAME = os.getenv("DB_NAME")

    def factory_csv_to_db(
        self,
        success_records: List[Dict[str, Any]],
        error_records: List[Dict[str, Any]],
        sql_success: str,
        sql_error: str,
    ) -> None:
        """將成功與錯誤資料分別寫入 DB"""
        if success_records:
            self.db.executemany(sql_success, success_records)

        if error_records:
            self.db.executemany(sql_error, error_records)

        self.db.commit()
        print(f"✅ 已寫入 DB：成功 {len(success_records)} 筆, 失敗 {len(error_records)} 筆")


# ========== 匯出層 ==========
class OutputLayer:
    """輸出層：輸出結果 CSV"""
    def __init__(self, output_dir: str = "./outputs"):
        self.output_dir = output_dir

    def export_csv(self, filename: str, records: List[Dict[str, Any]], fieldnames: List[str]) -> str:
        """輸出 CSV 檔案，回傳檔案路徑"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        filepath = f"{self.output_dir}/{filename}_{timestamp}.csv"

        with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

        print(f"📤 已匯出 CSV：{filepath}（{len(records)} 筆）")
        return filepath

    def export_by_party_ids(self, party_ids: List[str], all_records: List[Dict[str, Any]]) -> str:
        """
        根據指定的 Party_ID 清單，篩選資料並輸出 CSV
        """
        filtered = [r for r in all_records if r.get("Party_ID") in party_ids]

        if not filtered:
            print("⚠️ 沒有符合的 Party_ID")
            return ""

        fieldnames = list(filtered[0].keys())
        return self.export_csv("party_id_export", filtered, fieldnames)


class loggerLayer:
    def __init__(self):
        pass

    def factory(self):
        # === 設定 Logger ===
        LOG_FILE = "./logs/factory_run.log"
        # 先確保 logs 資料夾存在
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
                logging.StreamHandler()
            ]
        )
        logger = logging.getLogger(__name__)
        return logger

    def gcis(self):
        pass

    def tax(self):
        pass


class toolBox:
    def sys(self):
        pass
    # 拆檔功能
    # 切換 ip 功能

class userInterFace:
    def __init__(self):
        pass

    def main(self):
        ap = argparse.ArgumentParser()
        ap.add_argument("--csv", required=True)
        ap.add_argument("--out", default="result.csv")
        args = ap.parse_args()


