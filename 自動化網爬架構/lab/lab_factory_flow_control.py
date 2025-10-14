import logging
import time
import requests
import datetime
import zipfile
import os
import sys

"""
工廠 主體程式
"""

# import 資料品質剖析器 ETL
sys.path.append(os.path.abspath("C:/Users/wits/Downloads/HNCB/tests/crawler_hncb"))
# from lab_factory_etl_v5 import main as lab_factory_etl_v5_main  # 打包測試使用
from lab_factory_etl_v5 import DataPreprocessor
from lab_factory_etl_v5 import DataCleaner
from lab_factory_etl_v5 import DataAnomalyReporter
from lab_factory_etl_v5 import StatisticalSummaryEngine
from lab_factory_etl_v5 import Output

# === 設定 Logger ===
LOG_FILE = "./logs/etl_run.log"
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

# 自動下載目標數據_工廠（內建連線檢查）
def download_and_extract_zip(url: str, extract_to: str = ".") -> None:
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename = f"{ts}"
    zip_path = os.path.join(extract_to, zip_filename)

    # 檢查資料源網址是否有效
    try:
        head_resp = requests.head(url, timeout=5, allow_redirects=True)
        if head_resp.status_code != 200:
            logger.error(f"❌ 下載中止：URL 無效，狀態碼 {head_resp.status_code}")
            return
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ 下載中止：URL 無法連線 - {type(e).__name__} - {str(e)}")
        return

    logger.info(f"開始下載：工廠數據(月)作業\n{url}\n→\n{zip_path}")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    logger.info(f"下載完成：{zip_path}")

    with zipfile.ZipFile(zip_path, 'r') as z:
        members = z.namelist()
        logger.info(f"ZIP 內容：{members}")
        for member in members:
            extracted_path = z.extract(member, path=extract_to)
            dir_, fname = os.path.split(extracted_path)
            new_name = f"{ts}_{fname}"
            new_path = os.path.join(dir_, new_name)
            os.replace(extracted_path, new_path)
            logger.info(f"解壓並重命名：{member} → {new_name}")

    logger.info("✅ 全部下載, 解壓與重命名完成。")

    return new_name  # 回傳下載目標數據檔名, 供給下個 Class 資料剖析器 ETL

# 自動下載異常判斷：url, api, 網爬 (待雲端與 DB Setup Complete, 才繼續開發)
class AutomaticAbnormalJudgment:
    def __init__(self, url_checker, api_checker, crawler_checker):
        self.api_checker = api_checker
        self.crawler_checker = crawler_checker
        self.url_checker = url_checker

    def judgmen(self, source_id: str) -> bool:
        """
        綜合評估是否應啟動自動下載任務,
        待資料庫與雲端部屬整合後，繼續開發。
        """
        if self.api_checker.has_issue(source_id):  # Check API
            return False
        if self.crawler_checker.has_issue(source_id):  # Check 網爬程式
            return False

        if not self.url_checker.is_valid(source_id):  # Check 資料源：工廠數據
            return False
        return True

    # 串接 TG API, 以接獲即時異常通知
    def send_telegram(self, message: str, token: str, chat_id: str):
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message}
        requests.post(url, data=payload)


# 以下省略 initialize, DataCleaner, DataAnomalyReporter, StatisticalSummaryEngine 的實作...
# 只示範在 execute ETL 流程裡呼叫 logger：

if __name__ == "__main__":
    try:    
        try:
            # 1. 自動下載工廠數據 (內建檢查數據源網址異常)
            ZIP_URL = (
                "https://serv.gcis.nat.gov.tw/RDownLoad/Data/statistical/"
                "%E7%94%9F%E7%94%A2%E4%B8%AD%E5%B7%A5%E5%BB%A0%E6%B8%85%E5%86%8A.zip"
            )
            staging_dir = ".\data"
            logger.info(f"{'='*33}\n=== ETL 開始 ===")
            # # 1.1 下載 / 解壓 / 回傳檔名
            TargetFileName = download_and_extract_zip(ZIP_URL, extract_to=staging_dir)
            # TargetFileName = "20250822_104820_11404.csv"  # 手動用
            try:
                # 2. 資料預處裡
                preprocessor = DataPreprocessor()
                df = preprocessor.preprocess(csv_path=rf"./data/{TargetFileName}")
                # df = preprocessor.preprocess(csv_path=rf"./data/20250822_104820_11404.csv")  # 手動用
                logger.info(f"資料預處理 完成")
                try:
                    # 2.1 資料清洗與異常處裡
                    df_after = DataCleaner(df).convert_and_handle_errors()
                    df_after = DataAnomalyReporter(df_after).execute()
                    logger.info(f"資料清洗與異常處理 完成")
                    try:
                        # 2.2 敘述性統計
                        print("\n" + "=" * 40 + "\n")
                        StatisticalSummaryEngine(df_after).execute()  # 執行敘述性統計
                        time.sleep(1)
                        logger.info(f"資料敘述性統計 完成")
                        try:
                            # 2.3 匯出整理後資料成 CSV
                            output = Output(df=df_after, TargetFileName=TargetFileName)
                            output.output_data_to_csv()
                            logger.info(f"匯出整理後資料成 CSV 完成")
                        except Exception as e:
                            logger.exception("ETL 遇到例外，匯出整理後資料成 CSV 中斷：")
                    except Exception as e:
                        logger.exception("ETL 遇到例外，敘述性統計 中斷：")
                except Exception as e:
                    logger.exception("ETL 遇到例外，清洗與異常處理 中斷：")
            except Exception as e:
                logger.exception("ETL 遇到例外，資料預處理 中斷：")
        except Exception as e:
            logger.exception("ETL 遇到例外，自動下載工廠數據 中斷：")
    except Exception as e:
        logger.exception("ETL 遇到例外，流程中斷：")

