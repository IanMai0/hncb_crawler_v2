# tradeAdmin_optimized_secure.py
# -*- coding: utf-8 -*-
"""
加強版（安全優化）說明：
- CWE-778: 全面升級 Logging（TimedRotatingFileHandler、每次請求摘要、耗時、fail_streak、遮罩敏感資訊）
- CWE-295/SSRF: 僅允許 https://fbfh.trade.gov.tw，嚴格檢查 URL 主機名，不 mount http
- CWE-400: 限制回應大小（2 MB）、circuit breaker（連續錯誤暫停）、退避抖動
- CWE-532: 遮蔽 verifySHidden / payload
- CWE-73: CSV Injection防護（以單引號'前置）、原子寫檔（.tmp -> replace）
- 其他：訊號安全收尾（flush + logger handlers flush）、輸入/回傳健壯化
"""
from __future__ import annotations

import os
import sys
import csv
import time
import logging
import random
import signal
from datetime import datetime
from typing import List, Dict, Any, Iterable, Optional, Set, Tuple
from contextlib import contextmanager
from urllib.parse import urlparse

import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.exceptions import RequestException, Timeout
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from zoneinfo import ZoneInfo

# =========================
# 常數與全域設定
# =========================
HTTP_CONNECT_TIMEOUT = 10
HTTP_READ_TIMEOUT = 30
DEFAULT_TIMEOUT = (HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT)

# 允許的網域（避免 SSRF / 錯址）
ALLOWED_HOST = "fbfh.trade.gov.tw"
ALLOWED_SCHEME = "https"
MAX_RESPONSE_BYTES = 2 * 1024 * 1024  # 2 MB

# Circuit breaker
CB_FAIL_THRESHOLD = 20
CB_COOLDOWN_SEC = 60

# Timezone
TZ = ZoneInfo("Asia/Taipei")
def now_ts() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

# CSV 安全表頭
BASIC_HEADERS = ["統一編號", "公司名稱", "電話", "進口資格", "出口資格", "查詢時間"]
GRADE_HEADERS = ["統一編號", "時間週期", "公司名稱", "公司名稱英文", "總進口實績", "總出口實績", "統計時間年", "查詢時間"]

# =========================
# Logging（TimedRotatingFileHandler）
# =========================
os.makedirs("./logs", exist_ok=True)
logger = logging.getLogger("tradeAdmin")
logger.setLevel(logging.INFO)

# 避免重複加入 handler（多次 import/執行）
if not logger.handlers:
    from logging.handlers import TimedRotatingFileHandler

    # 檔案輪轉（每日）、保留14天
    fh = TimedRotatingFileHandler(
        "./logs/tradeAdmin_secure.log",
        when="midnight",
        backupCount=14,
        encoding="utf-8",
        utc=False
    )
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))

    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(sh)

def _sleep_jitter(base: float = 0.25, span: float = 0.35):
    time.sleep(base + random.random() * span)

# =========================
# 工具：遮罩敏感內容、URL 白名單檢查、CSV 注入防護
# =========================
def mask_value(v: Any, keep: int = 4) -> str:
    s = "" if v is None else str(v)
    if len(s) <= keep:
        return "*" * len(s)
    return s[:keep] + "*" * (len(s) - keep)

def mask_payload(d: Dict[str, Any] | None) -> Dict[str, Any] | None:
    if not isinstance(d, dict):
        return None
    masked = {}
    for k, v in d.items():
        key = str(k).lower()
        if any(s in key for s in ["verify", "token", "secret", "password"]):
            masked[k] = mask_value(v)
        else:
            # 避免巨量 payload 打滿 log
            s = "" if v is None else str(v)
            masked[k] = s[:128] + ("..." if len(s) > 128 else "")
    return masked

def ensure_allowed_url(url: str):
    """僅允許 https://fbfh.trade.gov.tw/..."""
    p = urlparse(url)
    if p.scheme.lower() != ALLOWED_SCHEME or p.hostname != ALLOWED_HOST:
        raise RequestException(f"Blocked URL (scheme/host not allowed): {url}")

def sanitize_csv_cell(val: Any) -> str:
    """CSV 注入防護：若以 = + - @ 開頭，加上 ' 前綴。"""
    s = "" if val is None else str(val)
    if s and s[0] in ("=", "+", "-", "@"):
        return "'" + s
    return s

def _safe_str(v: Any) -> str:
    return "" if v is None else str(v).strip()

# 將級距代碼 A-M 轉為可讀字串與數值上下限
def normalize_band(val: Any) -> Tuple[str, Optional[float], Optional[float]]:
    s = (str(val) if val is not None else "").strip().upper()
    try:
        if s and s.replace(".", "", 1).isdigit():
            v = float(s)
            return (f"{v}", v, v)
    except Exception:
        pass

    mapping = {
        "A": (">=10", 10.0, None),
        "B": (">=9,<10", 9.0, 10.0),
        "C": (">=8,<9", 8.0, 9.0),
        "D": (">=7,<8", 7.0, 8.0),
        "E": (">=6,<7", 6.0, 7.0),
        "F": (">=5,<6", 5.0, 6.0),
        "G": (">=4,<5", 4.0, 5.0),
        "H": (">=3,<4", 3.0, 4.0),
        "I": (">=2,<3", 2.0, 3.0),
        "J": (">=1,<2", 1.0, 2.0),
        "K": (">=0.5,<1", 0.5, 1.0),
        "L": (">0,<0.5", 0.0, 0.5),
        "M": ("=0", 0.0, 0.0),
    }
    if s in mapping:
        rng, lo, hi = mapping[s]
        return (f"{s} ({rng})", lo, hi)

    return ("", None, None)

# =========================
# 安全 Session 與 HTTP Client
# =========================
def build_session() -> requests.Session:
    s = requests.Session()
    # 僅 mount https，不 mount http（避免降級/誤用）
    adapter = HTTPAdapter(
        pool_connections=64,
        pool_maxsize=64,
        max_retries=Retry(
            total=5,
            connect=5,
            read=5,
            status=5,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "POST"}),
            raise_on_status=False,
            respect_retry_after_header=True,
        ),
    )
    s.mount("https://", adapter)

    # 預設 header
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://fbfh.trade.gov.tw",
        "Referer": "https://fbfh.trade.gov.tw/fb/web/queryBasicf.do",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    })

    # 額外：避免意外用到系統 Proxy
    s.trust_env = False
    return s

class HttpClient:
    """封裝 requests，統一錯誤處理與 retry 行為；必要時重建 Session；加入 circuit breaker 與風險控管。"""
    def __init__(self):
        self.session = build_session()
        self.fail_streak = 0
        self.rebuild_threshold = 8  # 連續失敗達門檻就重建
        self.cb_open_until: Optional[float] = None  # circuit breaker 冷卻截止

    def _check_circuit(self):
        if self.cb_open_until and time.time() < self.cb_open_until:
            raise RequestException("Circuit breaker open; cooling down")

    def _open_circuit_if_needed(self):
        if self.fail_streak >= CB_FAIL_THRESHOLD:
            self.cb_open_until = time.time() + CB_COOLDOWN_SEC
            logger.error(f"⚡ Circuit breaker OPEN for {CB_COOLDOWN_SEC}s (fail_streak={self.fail_streak})")

    def rebuild(self):
        try:
            self.session.close()
        except Exception:
            pass
        self.session = build_session()
        self.fail_streak = 0
        logger.warning("🔁 Session rebuilt due to consecutive failures")

    def _log_request_summary(self, method: str, url: str, start: float, resp: Optional[requests.Response], payload=None, err: Optional[Exception] = None):
        dur_ms = int((time.time() - start) * 1000)
        status = resp.status_code if resp is not None else None
        masked = mask_payload(payload)
        try:
            ensure_allowed_url(url)
            url_ok = True
        except Exception:
            url_ok = False

        msg = {
            "method": method,
            "url": url,
            "url_allowed": url_ok,
            "status": status,
            "duration_ms": dur_ms,
            "fail_streak": self.fail_streak,
            "payload_masked": masked,
            "error": str(err) if err else "",
        }
        # 控制 log 體積
        logger.info(f"[HTTP] {msg}")

    def _enforce_limits(self, resp: requests.Response):
        # 回應大小上限
        size = 0
        for chunk in resp.iter_content(chunk_size=64 * 1024):
            size += len(chunk)
            if size > MAX_RESPONSE_BYTES:
                raise RequestException(f"Response too large: {size} bytes")
        # 讀完後將內容放回以便後續 json() 使用
        resp._content = resp.content  # type: ignore

    def _request(self, method: str, url: str, *, timeout=DEFAULT_TIMEOUT, stream=False, **kwargs) -> requests.Response:
        start = time.time()
        payload = kwargs.get("data") or kwargs.get("json") or None
        resp = None

        # SSRF/白名單檢查
        ensure_allowed_url(url)
        self._check_circuit()

        try:
            resp = self.session.request(method, url, timeout=timeout, stream=True, **kwargs)
            self._enforce_limits(resp)
            self.fail_streak = 0
            _sleep_jitter()
            self._log_request_summary(method, url, start, resp, payload=payload)
            return resp
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            self.fail_streak += 1
            self._open_circuit_if_needed()
            logger.warning(f"🌐 transient error ({self.fail_streak}): {e}")
            if self.fail_streak >= self.rebuild_threshold:
                self.rebuild()
            self._log_request_summary(method, url, start, resp, payload=payload, err=e)
            raise
        except Exception as e:
            self.fail_streak += 1
            self._open_circuit_if_needed()
            logger.error(f"HTTP error: {e}")
            self._log_request_summary(method, url, start, resp, payload=payload, err=e)
            raise

    def post_form(self, url: str, data: dict, *, timeout=DEFAULT_TIMEOUT, headers: dict | None = None) -> requests.Response:
        return self._request("POST", url, data=data, headers=headers, timeout=timeout)

    def post_json(self, url: str, json: dict, *, timeout=DEFAULT_TIMEOUT, headers: dict | None = None) -> requests.Response:
        h = {}
        if headers:
            h.update(headers)
        h.setdefault("Content-Type", "application/json;charset=UTF-8")
        return self._request("POST", url, json=json, headers=h, timeout=timeout)

# =========================
# 安全收尾：中斷訊號 flush（exporter + logger）
# =========================
def flush_all_handlers():
    for h in logger.handlers:
        try:
            h.flush()
        except Exception:
            pass

def install_signal_handlers(exporter: "BucketCsvExporter"):
    def _handler(signum, frame):
        logger.warning(f"⚠️ received signal {signum}, flushing buffers...")
        try:
            exporter.flush()
            flush_all_handlers()
        finally:
            sys.exit(128 + signum)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handler)
        except Exception:
            pass

# =========================
# 匯出器（CSV 原子寫入 + CSV 注入防護）
# =========================
class BucketCsvExporter:
    """
    固定檔名版本：
    - basic 寫入 ./output/basic_info.csv
    - grade 寫入 ./output/export_import_grade.csv
    - 每 X 分鐘 flush 緩存（避免太頻繁 I/O）
    - 原子寫入：tmp -> replace（降低部分寫入風險）
    """
    def __init__(self, output_dir="./output", flush_minutes=10, encoding="utf-8-sig"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.encoding = encoding

        self.basic_path = os.path.join(self.output_dir, "basic_info.csv")
        self.grade_path = os.path.join(self.output_dir, "export_import_grade.csv")

        self.basic_buffer: List[List[Any]] = []
        self.grade_buffer: List[List[Any]] = []
        self.flush_window_sec = flush_minutes * 60
        self._last_export_ts = time.time()

        self._ensure_headers()

    def _atomic_append_rows(self, path: str, rows: List[List[Any]], header_len: int):
        if not rows:
            return

        # 校驗列長度 + CSV 注入防護
        fixed: List[List[str]] = []
        for idx, r in enumerate(rows, 1):
            # 長度調整
            if len(r) < header_len:
                r = r + [""] * (header_len - len(r))
                logger.warning(f"修正行長度（{path} 第{idx}列）：補空值")
            elif len(r) > header_len:
                r = r[:header_len]
                logger.warning(f"截斷行長度（{path} 第{idx}列）：截斷多餘欄位")

            # 注入防護
            fixed.append([sanitize_csv_cell(x) for x in r])

        # 原子寫入：先將舊檔 + 新行寫成 tmp，再 replace
        tmp_path = path + ".tmp"
        if os.path.exists(path):
            # 讀舊檔（避免重複表頭）
            with open(path, "r", encoding=self.encoding, newline="") as f_in, \
                 open(tmp_path, "w", encoding=self.encoding, newline="") as f_out:
                for line in f_in:
                    f_out.write(line)
                writer = csv.writer(f_out)
                writer.writerows(fixed)
        else:
            # 新檔：寫表頭 + rows
            headers = BASIC_HEADERS if path.endswith("basic_info.csv") else GRADE_HEADERS
            with open(tmp_path, "w", encoding=self.encoding, newline="") as f_out:
                writer = csv.writer(f_out)
                writer.writerow(headers)
                writer.writerows(fixed)
        os.replace(tmp_path, path)

    def _ensure_headers(self):
        def _ensure(path: str, headers: List[str]):
            if not os.path.exists(path):
                with open(path, "w", newline="", encoding=self.encoding) as f:
                    csv.writer(f).writerow(headers)
        _ensure(self.basic_path, BASIC_HEADERS)
        _ensure(self.grade_path, GRADE_HEADERS)

    def add_basic(self, row: List[Any]):
        self.basic_buffer.append(row)

    def add_grade(self, row: List[Any]):
        self.grade_buffer.append(row)

    def flush(self):
        self._atomic_append_rows(self.basic_path, self.basic_buffer, header_len=len(BASIC_HEADERS))
        self._atomic_append_rows(self.grade_path, self.grade_buffer, header_len=len(GRADE_HEADERS))
        flushed_basic = len(self.basic_buffer)
        flushed_grade = len(self.grade_buffer)

        self.basic_buffer.clear()
        self.grade_buffer.clear()
        self._last_export_ts = time.time()

        if flushed_basic or flushed_grade:
            logger.info(f"✅ 匯出完成：basic {flushed_basic} 筆、grade {flushed_grade} 筆 → 固定檔名")

    def export_if_due(self):
        if (time.time() - self._last_export_ts) >= self.flush_window_sec:
            self.flush()

# =========================
# 3) I/O：讀取目標、維護已完成與未完成名單
# =========================
def load_target_ids(input_csv: str) -> List[str]:
    df = pd.read_csv(input_csv, dtype=str, encoding="utf-8-sig", usecols=["統一編號"])
    ids = (
        df["統一編號"].dropna().astype(str).str.strip().tolist()
    )
    # 去重且保留原始順序
    seen = set()
    unique_ids = []
    for x in ids:
        if x not in seen:
            unique_ids.append(x)
            seen.add(x)
    return unique_ids

def load_done_ids_from_outputs(output_dir: str) -> Set[str]:
    if not os.path.isdir(output_dir):
        return set()
    basic = set(); grade = set()
    bp = os.path.join(output_dir, "basic_info.csv")
    gp = os.path.join(output_dir, "export_import_grade.csv")
    if os.path.isfile(bp) and os.path.getsize(bp) > 0:
        bdf = pd.read_csv(bp, dtype=str, encoding="utf-8-sig", usecols=["統一編號"])
        basic = set(bdf["統一編號"].dropna().astype(str).str.strip())
    if os.path.isfile(gp) and os.path.getsize(gp) > 0:
        gdf = pd.read_csv(gp, dtype=str, encoding="utf-8-sig", usecols=["統一編號"])
        grade = set(gdf["統一編號"].dropna().astype(str).str.strip())
    return basic & grade  # 交集：兩邊都有才算「完成」

def export_pending_ids(all_ids: Iterable[str], done_ids: Set[str], pending_path: str):
    pend = [cid for cid in all_ids if cid not in done_ids]
    # 原子寫入
    tmp = pending_path + ".tmp"
    pd.DataFrame({"統一編號": pend}).to_csv(tmp, index=False, encoding="utf-8-sig")
    os.replace(tmp, pending_path)
    logger.info(f"📄 未完成名單已輸出：{pending_path}（未完成 {len(pend)} 筆）")

# =========================
# 4) tradeAdmin：加強例外處理 + 空包也落檔
# =========================
class tradeAdmin:
    BASIC_API = "https://fbfh.trade.gov.tw/fb/common/popBasic.action"
    GRADE_API = "https://fbfh.trade.gov.tw/fb/common/popGrade.action"
    VERIFY_URL = "https://fbfh.trade.gov.tw/fb/web/queryBasicf.do"

    def __init__(self, exporter: BucketCsvExporter):
        self.http = HttpClient()
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://fbfh.trade.gov.tw/fb/web/queryBasicf.do",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://fbfh.trade.gov.tw",
            "Content-Type": "application/json;charset=UTF-8",
        }
        self.codesCom = ""          # 當前目標統編
        self.verifySHidden = ""     # 由 initialize() 取得
        self.api_payload: Dict[str, Any] = {}
        self.exporter = exporter

    def initialize(self, codesCom: str) -> bool:
        self.codesCom = codesCom
        logger.info(f"=== 處理公司：{self.codesCom} ===")
        try:
            payload = {
                "state": "queryAll",
                "verifyCode": "5408",
                "verifyCodeHidden": "5408",
                "verifySHidden": "",
                "q_BanNo": self.codesCom,
                "q_ieType": "E",
            }
            res = self.http.post_form(self.VERIFY_URL, data=payload, headers=self.headers)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")
            el = soup.find("input", {"name": "verifySHidden"})
            if not el or not el.get("value"):
                raise ValueError("verifySHidden 取得失敗")
            self.verifySHidden = el["value"]
            self.api_payload = {"banNo": self.codesCom, "verifySHidden": self.verifySHidden}
            logger.info(f"✅ verifySHidden 擷取成功：{mask_value(self.verifySHidden)}")
        except (Timeout, RequestException) as e:
            logger.error(f"❌ 初始化連線錯誤：{e}")
            self.exporter.add_basic([self.codesCom, "", "", "", "", now_ts()])
            self.exporter.add_grade([self.codesCom, "", "", "", 0, 0, "", now_ts()])
            return False
        except Exception as e:
            logger.error(f"❌ 初始化解析錯誤：{e}")
            self.exporter.add_basic([self.codesCom, "", "", "", "", now_ts()])
            self.exporter.add_grade([self.codesCom, "", "", "", 0, 0, "", now_ts()])
            return False
        return True

    def _refresh_token_and_retry(self, api: str, is_json=True, *, timeout=DEFAULT_TIMEOUT):
        try:
            payload = {
                "state": "queryAll",
                "verifyCode": "5408",
                "verifyCodeHidden": "5408",
                "verifySHidden": "",
                "q_BanNo": self.codesCom,
                "q_ieType": "E",
            }
            res = self.http.post_form(self.VERIFY_URL, data=payload, headers=self.headers, timeout=timeout)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")
            el = soup.find("input", {"name": "verifySHidden"})
            self.verifySHidden = el["value"]
            self.api_payload = {"banNo": self.codesCom, "verifySHidden": self.verifySHidden}
            logger.info(f"[{self.codesCom}] 🔄 token refreshed: {mask_value(self.verifySHidden)}")
        except Exception as e:
            logger.warning(f"[{self.codesCom}] token refresh failed: {e}")
            return None

        try:
            if is_json:
                r2 = self.http.post_json(api, json=self.api_payload, headers=self.headers, timeout=timeout)
            else:
                r2 = self.http.post_form(api, data=self.api_payload, headers=self.headers, timeout=timeout)
            r2.raise_for_status()
            return r2
        except Exception as e:
            logger.warning(f"[{self.codesCom}] retry after token refresh failed: {e}")
            return None

    def has_data(self) -> bool:
        try:
            res = self.http.session.post(
                self.BASIC_API,
                json=self.api_payload,
                headers=self.headers,
                timeout=DEFAULT_TIMEOUT,
                stream=True
            )
            # 也做大小限制
            self.http._enforce_limits(res)
            res.raise_for_status()
            data = res.json()
            time.sleep(0.3)
            return bool(data.get("retrieveDataList"))
        except (Timeout, RequestException) as e:
            logger.warning(f"[{self.codesCom}] 探測失敗（網路）：{e}")
            return False
        except Exception as e:
            logger.warning(f"[{self.codesCom}] 探測失敗（解析）：{e}")
            return False

    def get_basicData(self):
        try:
            res = self.http.session.post(
                self.BASIC_API,
                json=self.api_payload,
                headers=self.headers,
                timeout=DEFAULT_TIMEOUT,
                stream=True
            )
            self.http._enforce_limits(res)
            res.raise_for_status()
            data = res.json()

            if not isinstance(data, dict) or "retrieveDataList" not in data:
                r2 = self._refresh_token_and_retry(self.BASIC_API, is_json=True)
                if r2 is not None:
                    data = r2.json()

            lst = data.get("retrieveDataList") or []
            if not lst:
                self.exporter.add_basic([self.codesCom, "", "", "", "", now_ts()])
                logger.info(f"[{self.codesCom}] 無基本資料：已以空值落檔")
                return
            company = lst[0]
            row = [
                _safe_str(company[0]),
                _safe_str(company[1]),
                _safe_str(company[8]),
                _safe_str(company[19]),
                _safe_str(company[20]),
                now_ts()
            ]
            self.exporter.add_basic(row)
            logger.info(f"[{self.codesCom}] ✅ 基本資料完成")
        except (Timeout, RequestException) as e:
            logger.error(f"[{self.codesCom}] ❌ 基本資料連線錯誤：{e}")
            self.exporter.add_basic([self.codesCom, "", "", "", "", now_ts()])
        except Exception as e:
            logger.error(f"[{self.codesCom}] ❌ 基本資料解析錯誤：{e}")
            self.exporter.add_basic([self.codesCom, "", "", "", "", now_ts()])

    def get_gradeData(self):
        try:
            res = self.http.session.post(
                self.GRADE_API,
                json=self.api_payload,
                headers=self.headers,
                timeout=DEFAULT_TIMEOUT,
                stream=True
            )
            self.http._enforce_limits(res)
            res.raise_for_status()
            data = res.json()

            if not isinstance(data, dict) or "retrieveDataList" not in data:
                r2 = self._refresh_token_and_retry(self.BASIC_API, is_json=True)
                if r2 is not None:
                    data = r2.json()

            records = data.get("retrieveDataList") or []
            if not records:
                self.exporter.add_grade([self.codesCom, "", "", "", 0, 0, "", now_ts()])
                logger.info(f"[{self.codesCom}] 無實績資料：已以空值落檔")
                return

            for r in records:
                row = [
                    _safe_str(r[0]) if len(r) > 0 else self.codesCom,  # 統一編號
                    _safe_str(r[1]) if len(r) > 1 else "",             # 時間週期
                    _safe_str(r[2]) if len(r) > 2 else "",             # 公司名稱
                    _safe_str(r[3]) if len(r) > 3 else "",             # 公司名稱英文
                    normalize_band(r[4])[0] if len(r) > 4 else "",     # 總進口實績
                    normalize_band(r[5])[0] if len(r) > 5 else "",     # 總出口實績
                    _safe_str(r[6]) if len(r) > 6 else "",             # 統計時間年
                    now_ts()
                ]
                if not row[0]:
                    row[0] = self.codesCom
                self.exporter.add_grade(row)
            logger.info(f"[{self.codesCom}] ✅ 實績級距完成（{len(records)} 筆）")
        except (Timeout, RequestException) as e:
            logger.error(f"[{self.codesCom}] ❌ 實績級距連線錯誤：{e}")
            self.exporter.add_grade([self.codesCom, "", "", "", 0, 0, "", now_ts()])
        except Exception as e:
            logger.error(f"[{self.codesCom}] ❌ 實績級距解析錯誤：{e}")
            self.exporter.add_grade([self.codesCom, "", "", "", 0, 0, "", now_ts()])

# =========================
# 批次/續跑工具
# =========================
class Toolbox:
    @staticmethod
    def slice_by_range(ids: List[str], start: int | None, end: int | None) -> List[str]:
        n = len(ids)
        s = 0 if start is None else max(0, start)
        e = n if end is None else min(n, end)
        if s >= e:
            return []
        return ids[s:e]

    @staticmethod
    def slice_by_shard(ids: List[str], shard_idx: int, shard_total: int) -> List[str]:
        if shard_total <= 0 or shard_idx < 0 or shard_idx >= shard_total:
            raise ValueError("invalid shard config")
        out = []
        for uid in ids:
            if (abs(hash(uid)) % shard_total) == shard_idx:
                out.append(uid)
        return out

    @staticmethod
    def slice_by_mod_index(ids: List[str], mod: int, rem: int) -> List[str]:
        if mod <= 0 or rem < 0 or rem >= mod:
            raise ValueError("invalid mod/rem")
        return [uid for i, uid in enumerate(ids) if (i % mod) == rem]

    @staticmethod
    def read_marker(path: str) -> int | None:
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    return int(f.read().strip())
        except Exception:
            pass
        return None

    @staticmethod
    def write_marker(path: str, idx: int):
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                f.write(str(idx))
        except Exception as e:
            logger.warning(f"write marker failed: {e}")

    @staticmethod
    def filter_excluding_done(ids: List[str], done: set[str]) -> List[str]:
        return [uid for uid in ids if uid not in done]

# =========================
# Main：批次匯出 + 完成/未完成名單
# =========================
def main():
    INPUT_IDS = "./intput/250808需求/250808需求_169911筆目標統編_資本額1000萬up.csv"
    OUTPUT_DIR = "./output"
    PENDING_PATH = os.path.join(OUTPUT_DIR, "pending_ids.csv")

    exporter = BucketCsvExporter(output_dir=OUTPUT_DIR, flush_minutes=1)
    install_signal_handlers(exporter)
    ta = tradeAdmin(exporter)

    all_ids = load_target_ids(INPUT_IDS)
    hist_done = load_done_ids_from_outputs(OUTPUT_DIR)
    sess_done: set[str] = set()

    total = len(all_ids)
    logger.info(f"總目標 {total:,}，歷史已完成 {len(hist_done):,}，尚待 {total - len(hist_done):,}")

    for idx, uid in enumerate(all_ids, start=1):
        if uid in hist_done or uid in sess_done:
            continue

        ok_init = ta.initialize(uid)

        if ok_init:
            ta.get_basicData()
            ta.get_gradeData()

        sess_done.add(uid)
        exporter.export_if_due()

        if idx % 50 == 0:
            remaining = total - len(hist_done) - len(sess_done)
            logger.info(f"進度 {idx:,}/{total:,} | 本輪完成 {len(sess_done):,} | 尚待 {remaining:,}")

    exporter.flush()
    flush_all_handlers()

    all_done = hist_done | sess_done
    pend = [cid for cid in all_ids if cid not in all_done]
    tmp = PENDING_PATH + ".tmp"
    pd.DataFrame({"統一編號": pend}).to_csv(tmp, index=False, encoding="utf-8-sig")
    os.replace(tmp, PENDING_PATH)
    logger.info(f"🎉 全程完成。本輪完成 {len(sess_done):,}，歷史+本輪合計 {len(all_done):,}，未完成 {len(pend):,}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(f"Uncaught error: {e}")
        flush_all_handlers()
        raise

