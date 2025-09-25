import requests
import time
import random
import csv
import os
import pandas as pd
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List


# === 初始化 Logging 系統 ===
os.makedirs("./logs", exist_ok=True)
logging.basicConfig(
    filename="./logs/log_tradeAdmin.txt",
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# === 工具：10 分鐘時間桶（例如 10:03 → 10:00；10:19 → 10:10） ===
def _minute_bucket(dt: datetime, step: int = 10) -> str:
    m = (dt.minute // step) * step
    return dt.strftime(f"%Y%m%d_%H{m:02d}")

# === 國貿暑網爬 ===
class tradeAdmin:
    def __init__(self, codesCom: str):
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://fbfh.trade.gov.tw/fb/web/queryBasicf.do",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://fbfh.trade.gov.tw",
            "Content-Type": "application/json;charset=UTF-8",
        }
        self.session = requests.Session()

        self.verifySHidden = ""  # 預設為空 才能抓取當下即時的版本
        self.codesCom = codesCom
        self.api_payload = {}

        # 收集資料的容器
        self.basicData: List[list] = []
        self.gradeData: List[list] = []

        # === 新增：批次匯出控制 ===
        self.output_dir = "./output"
        os.makedirs(self.output_dir, exist_ok=True)
        self.export_window_sec = 10 * 60           # 每 10 分鐘匯出一次
        self._last_export_ts = time.time()         # 上次匯出時間
        self._current_bucket = _minute_bucket(datetime.now())  # 目前時間桶

    def initialize(self):
        logging.info("初始化 Session")
        print(f"\n初始化 Session, 目標統一編號: {self.codesCom}")
        logging.info(f"=== 處理公司：{self.codesCom} ===")

        payload = {
            "state": "queryAll",
            "verifyCode": "5408",
            "verifyCodeHidden": "5408",
            "verifySHidden": "",  # 初始空
            "q_BanNo": self.codesCom,
            "q_ieType": "E"
        }

        try:
            res = self.session.post(
                "https://fbfh.trade.gov.tw/fb/web/queryBasicf.do",
                data=payload,
                headers=self.headers
            )
            time.sleep(0.5)

            soup = BeautifulSoup(res.text, "html.parser")
            self.verifySHidden = soup.find("input", {"name": "verifySHidden"})["value"]
            # 更新 api payload
            self.api_payload = {
                "banNo": self.codesCom,
                "verifySHidden": self.verifySHidden
            }
            logging.info(f"✅ verifySHidden擷取成功：{self.verifySHidden}")
            print(f"✅ verifySHidden擷取成功：{self.verifySHidden}")
        except Exception as e:
            logging.error(f"❌ 初始化 verifySHidden 錯誤：{e}")

    # 無資料值處理
    def parse_or_empty(self):
        logging.info(f"[{self.codesCom}] 檢查是否為有效查詢...")
        api_url = "https://fbfh.trade.gov.tw/fb/common/popBasic.action"
        res = self.session.post(api_url, json=self.api_payload, headers=self.headers)
        data = res.json()
        has_data = data["retrieveDataList"]
        time.sleep(0.5)

        return has_data

    def get_basicData(self):
        logging.info(f"[{self.codesCom}] 抓取基本資料中...")
        api_url = "https://fbfh.trade.gov.tw/fb/common/popBasic.action"
        try:
            res = self.session.post(api_url, json=self.api_payload, headers=self.headers)
            data = res.json()
            print(f"=== 抓取1_聯絡電話/進出口資格 ===\nAPI 回應: JSON: {data}")
            print(data)
            time.sleep(1)

            company_data = data["retrieveDataList"][0]
            print("✅ 統一編號：", company_data[0])
            print("✅ 公司名稱：", company_data[1])
            print("✅ 電話：", company_data[8])
            print("✅ 進口資格：", company_data[19])
            print("✅ 出口資格：", company_data[20])
            self.basicData.append([company_data[0], company_data[1], company_data[8], company_data[19], company_data[20]])

            print('=== deep res json: basic data ===')
            print(company_data)

            logging.info(f"[{self.codesCom}] ✅ 基本資料擷取成功：{company_data[1]}")
        except Exception as e:
            logging.error(f"[{self.codesCom}] ❌ 抓取基本資料錯誤：{e}")

    def get_gradeData(self):
        logging.info(f"[{self.codesCom}] 抓取實績級距中...")
        api_url = "https://fbfh.trade.gov.tw/fb/common/popGrade.action"
        try:
            res = self.session.post(api_url, json=self.api_payload, headers=self.headers)
            data = res.json()

            print(f"=== 抓取2_實績級距 ===\nAPI 回應: JSON: {data}")
            time.sleep(1)

            records = data.get("retrieveDataList", [])
            if records:
                self.gradeData.extend(records)
                logging.info(f"[{self.codesCom}] ✅ 共擷取實績級距 {len(records)} 筆")
            else:
                logging.warning(f"[{self.codesCom}] ⚠️ 無實績級距資料")

        except Exception as e:
            logging.error(f"[{self.codesCom}] ❌ 抓取實績級距錯誤：{e}")

    # === 變更：export_to_csv 改成分桶 append，且新檔才寫表頭 ===
    def export_to_csv(self, output_dir: str = "./output"):
        os.makedirs(output_dir, exist_ok=True)
        try:
            bucket = self._current_bucket
            basic_path = os.path.join(output_dir, f"basic_info_{bucket}.csv")
            grade_path = os.path.join(output_dir, f"export_import_grade_{bucket}.csv")

            # basic_info 追加寫入
            if self.basicData:
                file_exists = os.path.exists(basic_path)
                with open(basic_path, "a" if file_exists else "w", newline='', encoding='utf-8-sig') as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(["統一編號", "公司名稱", "電話", "進口資格", "出口資格"])
                    writer.writerows(self.basicData)

            # export_import_grade 追加寫入
            if self.gradeData:
                file_exists = os.path.exists(grade_path)
                with open(grade_path, "a" if file_exists else "w", newline='', encoding='utf-8-sig') as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(["統一編號", "時間週期", "公司名稱", "公司名稱英文", "總進口實績", "總出口實績", "統計時間年"])
                    writer.writerows(self.gradeData)

            # 匯出後清空當前緩存，避免下個 10 分鐘視窗重複寫入
            self.basicData.clear()
            self.gradeData.clear()

            logging.info(f"✅ 資料成功匯出至 CSV（bucket={bucket}）")

        except Exception as e:
            logging.error(f"❌ 匯出 CSV 時發生錯誤：{e}")

    # === 每 10 分鐘或時間桶變化就匯出一次 ===
    def export_if_due(self):
        now = time.time()
        bucket_now = _minute_bucket(datetime.now())
        due_by_time = (now - self._last_export_ts) >= self.export_window_sec
        bucket_changed = (bucket_now != self._current_bucket)

        if due_by_time or bucket_changed:
            # 更新桶名到最新，確保寫到新的檔案
            self._current_bucket = bucket_now
            self.export_to_csv(self.output_dir)
            self._last_export_ts = time.time()  # 重設計時

def read_DB_target():
    df = pd.read_csv(
        "./intput/250808需求/250808需求_169911筆目標統編_資本額1000萬up.csv",
        dtype=str,
        encoding="utf-8"
    )
    codesCom = df["統一編號"]
    return codesCom

if __name__ == '__main__':
    codesCom = read_DB_target()  # 讀取目標統一編號

    start_index = 0
    end_index = 6
    selected_codesCom = codesCom[start_index:end_index]
    logging.info("\n")
    ta = tradeAdmin("")

    for num in selected_codesCom:
        ta.codesCom = str(num)
        ta.initialize()

        has_data = ta.parse_or_empty()
        if has_data == []:
            print('該統編無資料')
        else:
            ta.get_basicData()
            ta.get_gradeData()

        # === 新增：每處理完一家公司，就檢查是否到 10 分鐘或跨桶，到了就匯出一次 ===
        ta.export_if_due()

    # === 流程結束前，確保最後一批也寫出去 ===
    ta.export_to_csv("./output")
    logging.info("🎉 所有公司資料處理完畢")

