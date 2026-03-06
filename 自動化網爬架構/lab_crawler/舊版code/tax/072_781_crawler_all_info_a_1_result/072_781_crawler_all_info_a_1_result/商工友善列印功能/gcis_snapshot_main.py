# -*- coding: utf-8 -*-
"""
友善列印（安全強化版）
- 改用 CDP Page.printToPDF 直接輸出 PDF（移除 pyautogui 和座標點擊）
- company_id 白名單驗證（8 碼數字）
- 加入 page load timeout、嚴謹例外處理與資源釋放
"""
import os
import re
import time
import base64
import traceback
from datetime import datetime

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

CONFIG = {
    "output_directory": os.getenv("GCIS_SNAPSHOT_OUT", r"C:\HNB\HNB_Code\gcis_snapshot\output"),
    "retry_limit": 5,
    "page_load_timeout": 30,   # 秒
    "wait_short": 10,          # 顯性等待（秒）
    "wait_long": 15
}

# --------- 工具函式 ---------
_digit8 = re.compile(r"^\d{8}$")

def ensure_output_directory() -> str:
    outdir = CONFIG["output_directory"]
    os.makedirs(outdir, exist_ok=True)
    return outdir

def _assert_company_id(cid: str) -> str:
    cid = str(cid).strip()
    if not _digit8.match(cid):
        raise ValueError("company_id 必須為 8 碼數字")
    return cid

def initialize_driver() -> webdriver.Chrome:
    opts = webdriver.ChromeOptions()
    # 建議 headless，可視需要關掉
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-sync")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    # 關閉 PDF 內建檢視器避免干擾
    prefs = {"plugins.always_open_pdf_externally": True}
    opts.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(CONFIG["page_load_timeout"])
    return driver

# --------- 主要流程（改用 printToPDF） ---------
def Gcis_Snapshot(company_id: str, test_type: bool = False):
    attempt = 0
    while attempt < CONFIG["retry_limit"]:
        attempt += 1
        start_time = time.time()
        driver = None
        try:
            company_id = _assert_company_id(company_id)
            outdir = ensure_output_directory()
            if test_type:
                print(f"[嘗試 {attempt}/{CONFIG['retry_limit']}]")

            driver = initialize_driver()
            if test_type:
                print("[步驟2] 開啟查詢頁")
            driver.get("https://findbiz.nat.gov.tw/fts/query/QueryBar/queryInit.do")

            if test_type:
                print("[步驟3] 等待輸入框出現")
            WebDriverWait(driver, CONFIG["wait_short"]).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="qryCond"]'))
            )

            if test_type:
                print("[步驟4] 輸入統一編號並送出")
            query_input = driver.find_element(By.XPATH, '//*[@id="qryCond"]')
            query_input.send_keys(company_id)
            query_input.send_keys(Keys.RETURN)

            # reCAPTCHA 簡易偵測（有就重試）
            try:
                WebDriverWait(driver, 2).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'g-recaptcha'))
                )
                if test_type:
                    print("[驗證碼] 偵測到 reCAPTCHA，關閉並重試")
                driver.quit()
                driver = None
                continue
            except Exception:
                pass

            if test_type:
                print("[步驟5] 等待搜尋結果")
            try:
                WebDriverWait(driver, CONFIG["wait_short"]).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="vParagraph"]/div/div[1]/a'))
                )
            except Exception:
                if test_type:
                    print("[步驟5-1] 無符合條件資料 → Not_companyID")
                return company_id, "Not_companyID"

            if test_type:
                print("[步驟6] 進入公司詳情")
            first_link = driver.find_element(By.XPATH, '//*[@id="vParagraph"]/div/div[1]/a')
            first_link.click()

            if test_type:
                print("[步驟7] 等待友善列印按鈕並點擊")
            WebDriverWait(driver, CONFIG["wait_long"]).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="friendlyPrint"]'))
            )
            driver.find_element(By.XPATH, '//*[@id="friendlyPrint"]').click()

            # 友善列印另開視窗 → 切換到新視窗
            WebDriverWait(driver, CONFIG["wait_short"]).until(EC.number_of_windows_to_be(2))
            driver.switch_to.window(driver.window_handles[-1])

            # 直接用 CDP 匯出 PDF
            if test_type:
                print("[步驟8] 以 Page.printToPDF 匯出 PDF")
            pdf = driver.execute_cdp_cmd("Page.printToPDF", {
                "landscape": False,
                "printBackground": True,
                "paperWidth": 8.27,     # A4 寬（英吋）
                "paperHeight": 11.69,   # A4 高（英吋）
                "marginTop": 0.39,
                "marginBottom": 0.39,
                "marginLeft": 0.39,
                "marginRight": 0.39,
                "preferCSSPageSize": True
            })
            pdf_bytes = base64.b64decode(pdf["data"])
            ts = datetime.now().strftime("%Y%m%d_%H%M")
            file_name = f"gcis_friendlyprint_result_{company_id}_{ts}.pdf"
            file_path = os.path.join(outdir, file_name)
            with open(file_path, "wb") as f:
                f.write(pdf_bytes)

            if test_type:
                print(f"[完成] 輸出：{file_path}，耗時 {time.time()-start_time:.2f}s")
            return company_id, "success"

        except Exception:
            print("[錯誤] 發生異常：")
            traceback.print_exc()
        finally:
            try:
                if driver is not None:
                    driver.quit()
            except Exception:
                pass

    print(f"[流程結束] 全部重試失敗（{CONFIG['retry_limit']} 次）")
    return company_id, "failure"

# --------- 範例執行（保持你原本的結果彙整格式） ---------
if __name__ == "__main__":
    company_ids = ['20828393', '12345678', '04541302', '12215548', '12992265']
    results = []
    for idx, cid in enumerate(company_ids, start=1):
        print(f"[處理進度] {idx}/{len(company_ids)} → {cid}")
        start = time.time()
        cid, status = Gcis_Snapshot(cid, test_type=False)
        elapsed = time.time() - start
        results.append({"company_id": cid, "status": status, "speedtime": elapsed})
        print(f"[完成] {cid} | {status} | {elapsed:.2f}s")

    df = pd.DataFrame(results, columns=['company_id', 'status', 'speedtime'])
    print("\n[自動化查詢結果]")
    print(df)

