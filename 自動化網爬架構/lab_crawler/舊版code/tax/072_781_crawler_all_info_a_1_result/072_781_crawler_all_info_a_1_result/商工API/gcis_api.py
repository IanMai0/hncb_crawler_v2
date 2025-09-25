# -*- coding: utf-8 -*-
"""
gcis_api.py（已修補版）
- 一律 HTTPS + 驗憑證；所有 requests 具備 timeout + Retry
- Swagger schemes 改為 https
- 預設不開 debug、預設不綁 0.0.0.0（可用環境變數覆寫）
- 參數白名單驗證，避免 OData filter 注入
"""
import os
import re
import time
import json
import traceback
from datetime import datetime

from flask import Flask, request, jsonify, send_file
from flasgger import Swagger, swag_from

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# （以下 RPA 仍保留原行為）
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pyautogui
from bs4 import BeautifulSoup

app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False

# ---- 安全預設 ----
REQ_TIMEOUT = (10, 30)
_GCIS_BASE = "https://data.gcis.nat.gov.tw/od/data/api"

def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "GCIS-API/1.0"})
    return s

SESSION = _make_session()

# ---- Swagger（改為 https）----
app.config["SWAGGER"] = {
    "title": "GCIS API",
    "uiversion": 3,
    "specs_route": "/apidocs/",
    "openapi": "3.0.0",
}
swagger = Swagger(app, template={
    "swagger": "2.0",
    "info": {"title": "GCIS API", "description": "提供公司、商業或分公司資訊查詢的 API", "version": "1.0.0"},
    "basePath": "/",
    "schemes": ["https"],
})

# ---- 輸入白名單驗證 ----
_digit8 = re.compile(r"^\d{8}$")
_item = re.compile(r"^[A-Z0-9_]{1,20}$")
_agency = re.compile(r"^[A-Z0-9\u4e00-\u9fa5]{1,20}$")

def _assert_tax_id(cid: str) -> str:
    if not _digit8.match(str(cid)):
        raise ValueError("cid 必須為 8 碼數字（統一編號）")
    return str(cid)

def _assert_item(code: str) -> str:
    code = str(code).upper()
    if not _item.match(code):
        raise ValueError("營業項目代碼格式不符")
    return code

def _assert_agency(s: str) -> str:
    s = str(s).strip()
    if not _agency.match(s):
        raise ValueError("Agency 格式不符")
    return s

# ---- 通用請求 ----
def _gcis_get(resource_id: str, flt: str):
    url = f"{_GCIS_BASE}/{resource_id}"
    params = {"$format": "json", "$filter": flt, "$skip": 0, "$top": 50}
    r = SESSION.get(url, params=params, timeout=REQ_TIMEOUT)  # 一律驗憑證
    r.raise_for_status()
    return r.json()

# ---- 業務函式 ----
def get_c1(cid: str):
    cid = _assert_tax_id(cid)
    return _gcis_get("5F64D864-61CB-4D0D-8AD9-492047CC1EA6", f"Business_Accounting_NO eq {cid}")

def get_c3(cid: str):
    cid = _assert_tax_id(cid)
    return _gcis_get("236EE382-4942-41A9-BD03-CA0709025E7C", f"Business_Accounting_NO eq {cid}")

def get_stack(cid: str):
    # 若要 XML，請另行處理解析；此處暫不使用
    cid = _assert_tax_id(cid)
    url = f"{_GCIS_BASE}/4E5F7653-1B91-4DDC-99D5-468530FAE396"
    params = {"$format": "json", "$filter": f"Business_Accounting_NO eq {cid}", "$skip": 0, "$top": 50}
    r = SESSION.get(url, params=params, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.json()

def get_b1(president_no: str, agency: str):
    president_no = _assert_tax_id(president_no)
    agency = _assert_agency(agency)
    return _gcis_get(
        "7E6AFA72-AD6A-46D3-8681-ED77951D912D",
        f"President_No eq {president_no} and Agency eq {agency}",
    )

def get_ch(cid: str):
    cid = _assert_tax_id(cid)
    return _gcis_get("673F0FC0-B3A7-429F-9041-E9866836B66D", f"No eq {cid}")

def get_ci(cid: str):
    cid = _assert_tax_id(cid)
    return _gcis_get("4E5F7653-1B91-4DDC-99D5-468530FAE396", f"Business_Accounting_NO eq {cid}")

# ---- RPA 產 PDF（原行為保留）----
CONFIG = {
    "output_directory": r"C:\Users\Administrator\PycharmProjects\PythonProject\gcis_batch\tmp_pdf",
    "retry_limit": 5,
    "move_duration": 0.3,
    "source_path_coordinates": (650, 55),
    "file_name_coordinates": (350, 450),
    "print_button_coordinates": (775, 705),
}

def initialize_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--no-first-run')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-background-networking')
    options.add_argument('--disable-sync')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument("--user-data-dir=C:/Users/Administrator/AppData/Local/Google/Chrome/User Data")
    options.add_argument("--profile-directory=Default")
    return webdriver.Chrome(options=options)

def ensure_output_directory():
    outdir = CONFIG["output_directory"]
    os.makedirs(outdir, exist_ok=True)
    return outdir

def Gcis_Snapshot(company_id, test_type):
    attempt = 0
    while attempt < CONFIG["retry_limit"]:
        attempt += 1
        if test_type:
            print(f"[嘗試第 {attempt}/{CONFIG['retry_limit']}]")
        start_time = time.time()
        driver = None
        try:
            driver = initialize_driver()
            ensure_output_directory()
            if test_type:
                print("[第2步] 進入查詢網站")
            driver.get("https://findbiz.nat.gov.tw/fts/query/QueryBar/queryInit.do")

            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="qryCond"]')))
            query_input = driver.find_element(By.XPATH, '//*[@id="qryCond"]')
            query_input.send_keys(_assert_tax_id(company_id))
            query_input.send_keys(Keys.RETURN)
            time.sleep(1)

            # reCAPTCHA 偵測
            try:
                WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CLASS_NAME, 'g-recaptcha')))
                if test_type:
                    print("[驗證碼] 重新嘗試")
                driver.quit()
                continue
            except Exception:
                pass

            try:
                WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '//*[@id="vParagraph"]/div/div[1]/a')))
            except Exception:
                return company_id, "Not_companyID"

            driver.find_element(By.XPATH, '//*[@id="vParagraph"]/div/div[1]/a').click()
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, '//*[@id="friendlyPrint"]')))
            driver.find_element(By.XPATH, '//*[@id="friendlyPrint"]').click()

            WebDriverWait(driver, 10).until(EC.number_of_windows_to_be(2))
            driver.switch_to.window(driver.window_handles[-1])

            x, y = CONFIG["print_button_coordinates"]
            pyautogui.moveTo(x, y, duration=CONFIG["move_duration"])
            time.sleep(3)
            pyautogui.click()

            xs, ys = CONFIG["source_path_coordinates"]
            pyautogui.moveTo(xs, ys, duration=CONFIG["move_duration"]); time.sleep(2); pyautogui.click()
            pyautogui.typewrite(CONFIG["output_directory"]); time.sleep(0.5); pya_]()
