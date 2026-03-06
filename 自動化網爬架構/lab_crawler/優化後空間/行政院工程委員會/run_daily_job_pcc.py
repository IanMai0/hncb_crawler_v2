import logging
import requests
import datetime
import os
import pandas as pd
import logging
import requests
import datetime
import os
import pandas as pd
import urllib3


"""
行政院工程委員會_優良/拒往 主體程式
功能：下載 Excel 並直接轉為 CSV
"""

# 設定 URL
URL_EXCELLENT = "https://web.pcc.gov.tw/vms/emlm/emlmPublicSearch/queryEMFile/xls"
URL_BLACKLIST = "https://web.pcc.gov.tw/vms/rvlm/rvlmPublicSearch/queryRVFile/xls"

# 關閉 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =====================
# 設定 Logger
# =====================
LOG_FILE = "./logs/etl_run.log"
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


def download_and_convert_to_csv(url: str, category_name: str, output_dir: str = "./data") -> str:
    current_ts = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    os.makedirs(output_dir, exist_ok=True)
    temp_excel = os.path.join(output_dir, f"temp_{category_name}_{current_ts}.xls")
    csv_path = os.path.join(output_dir, f"{category_name}_{current_ts}.csv")

    try:
        # 重點：加入 verify=False 略過憑證檢查
        logger.info(f"📥 開始下載 [{category_name}]，已略過 SSL 驗證...")
        with requests.get(url, stream=True, timeout=60, verify=False) as resp:
            resp.raise_for_status()

            with open(temp_excel, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):  # 處理下載效率
                    f.write(chunk)
            try:  # xls > csv
                logger.info(f"🔄 轉換中：{category_name} (Excel -> CSV)")
                # 這裡如果 pd.read_excel 報錯，可能需要安裝 xlrd: pip install xlrd
                df = pd.read_excel(temp_excel, engine="xlrd")
                df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                logger.info(f"✅ 轉換成功：{csv_path}")
            except Exception as e:
                print(f'xls > csv 遭遇未知錯誤\n{e}')

    except Exception as e:
        logger.error(f"❌ 處理 [{category_name}] 時發生錯誤：{str(e)}")
        return None
    # finally:  # 把臨時存下來的 xls 移除
    #     if os.path.exists(temp_excel):
    #         os.remove(temp_excel)
    return csv_path



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


# =====================
# 主程式執行
# =====================
if __name__ == "__main__":
    # 執行優良廠商下載與轉換
    path_1 = download_and_convert_to_csv(URL_EXCELLENT, "pcc_excellent", output_dir="./data")

    # 執行拒往廠商下載與轉換
    path_2 = download_and_convert_to_csv(URL_BLACKLIST, "pcc_blacklist", output_dir="./data")

    if path_1 and path_2:
        logger.info("🚀 [DONE] 所有 Excel 轉 CSV 任務已完成。")

