# -*- coding: utf-8 -*-
"""
api_main.py（已修補版）
- 強制 HTTPS 與驗證憑證
- requests 共用 Session，具備 timeout + Retry
- 對輸入參數做白名單驗證
"""
import re
import sys
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---- 安全預設 ----
REQ_TIMEOUT = (10, 30)  # (connect, read)
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
    s.headers.update({"User-Agent": "GCIS-Client/1.0"})
    return s

SESSION = _make_session()

# ---- 輸入白名單驗證 ----
_digit8 = re.compile(r"^\d{8}$")                   # 統一編號
_item_code = re.compile(r"^[A-Z0-9_]{1,20}$")       # 營業項目代碼

def _assert_tax_id(cid: str) -> str:
    if not _digit8.match(str(cid)):
        raise ValueError("cid 必須為 8 碼數字（統一編號）")
    return str(cid)

def _assert_item(code: str) -> str:
    code = str(code).upper()
    if not _item_code.match(code):
        raise ValueError("營業項目代碼格式不符（僅限 A-Z/0-9/底線，<=20）")
    return code

# ---- GCIS 查詢 ----
def get_c1(cid: str) -> str:
    """公司基本資料（C1）"""
    cid = _assert_tax_id(cid)
    url = f"{_GCIS_BASE}/5F64D864-61CB-4D0D-8AD9-492047CC1EA6"
    params = {"$format": "json", "$filter": f"Business_Accounting_NO eq {cid}", "$skip": 0, "$top": 50}
    r = SESSION.get(url, params=params, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.text

def get_c3(cid: str) -> str:
    """公司營業項目（C3）"""
    cid = _assert_tax_id(cid)
    url = f"{_GCIS_BASE}/236EE382-4942-41A9-BD03-CA0709025E7C"
    params = {"$format": "json", "$filter": f"Business_Accounting_NO eq {cid}", "$skip": 0, "$top": 50}
    r = SESSION.get(url, params=params, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.text

def get_ci(item_code: str) -> str:
    """營業項目對照（CI）"""
    item_code = _assert_item(item_code)
    url = f"{_GCIS_BASE}/FCB90AB1-E382-45CE-8D4F-394861851E28"  # ← 已改為 HTTPS
    params = {"$format": "json", "$filter": f"Business_Item eq {item_code}", "$skip": 0, "$top": 50}
    r = SESSION.get(url, params=params, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.text

if __name__ == "__main__":
    start_time = datetime.now()
    print("START Time :", start_time.strftime("%Y-%m-%d %H:%M:%S"))

    cid = sys.argv[1] if len(sys.argv) > 1 else "20828393"
    try:
        result = get_c1(cid)
        print(result)
    except Exception as e:
        print(f"❌ 失敗：{e}")
