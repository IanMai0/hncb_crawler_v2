# tradeAdmin_optimized.py
# -*- coding: utf-8 -*-
import os
import csv
import time
import json
import logging
import argparse
from datetime import datetime
from typing import List, Dict, Any, Iterable, Set

import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.exceptions import RequestException, Timeout
from zoneinfo import ZoneInfo

# =========================
# Logging 強化
# =========================
os.makedirs("./logs", exist_ok=True)
logger = logging.getLogger("tradeAdmin")
logger.setLevel(logging.INFO)

_fh = logging.FileHandler("./logs/tradeAdmin.log", encoding="utf-8")
_fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
_sh = logging.StreamHandler()
_sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

if not logger.handlers:
    logger.addHandler(_fh)
    logger.addHandler(_sh)

# =========================
# 小工具
# =========================
TZ = ZoneInfo("Asia/Taipei")
def now_ts() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

BASIC_HEADERS = ["統一編號", "公司名稱", "電話", "進口資格", "出口資格", "查詢時間"]
GRADE_HEADERS = ["統一編號", "時間週期", "公司名稱", "公司名稱英文", "總進口實績", "總出口實績", "統計時間年", "查詢時間"]

def _safe_str(v: Any) -> str:
    return "" if v is None else str(v).strip()

def normalize_band(val: Any):
    s = (str(val) if val is not None else "").strip().upper()
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
# 匯出器
# =========================
class BucketCsvExporter:
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

    def _ensure_headers(self):
        if not os.path.exists(self.basic_path):
            with open(self.basic_path, "w", newline="", encoding=self.encoding) as f:
                csv.writer(f).writerow(BASIC_HEADERS)
        if not os.path.exists(self.grade_path):
            with open(self.grade_path, "w", newline="", encoding=self.encoding) as f:
                csv.writer(f).writerow(GRADE_HEADERS)

    def add_basic(self, row: List[Any]): self.basic_buffer.append(row)
    def add_grade(self, row: List[Any]): self.grade_buffer.append(row)

    def _append_rows(self, path: str, rows: List[List[Any]], header_len: int):
        if not rows: return
        fixed = []
        for r in rows:
            if len(r) < header_len:
                fixed.append(r + [""] * (header_len - len(r)))
            else:
                fixed.append(r[:header_len])
        with open(path, "a", newline="", encoding=self.encoding) as f:
            csv.writer(f).writerows(fixed)

    def flush(self):
        self._append_rows(self.basic_path, self.basic_buffer, len(BASIC_HEADERS))
        self._append_rows(self.grade_path, self.grade_buffer, len(GRADE_HEADERS))
        self.basic_buffer.clear()
        self.grade_buffer.clear()
        self._last_export_ts = time.time()

    def export_if_due(self):
        if (time.time() - self._last_export_ts) >= self.flush_window_sec:
            self.flush()

# =========================
# I/O
# =========================
def load_target_ids(input_csv: str) -> List[str]:
    df = pd.read_csv(input_csv, dtype=str, encoding="utf-8-sig", usecols=["統一編號"])
    ids = df["統一編號"].dropna().astype(str).str.strip().tolist()
    seen, unique_ids = set(), []
    for x in ids:
        if x not in seen:
            unique_ids.append(x); seen.add(x)
    return unique_ids

def load_done_ids_from_outputs(output_dir: str) -> Set[str]:
    if not os.path.isdir(output_dir): return set()
    basic, grade = set(), set()
    bp = os.path.join(output_dir, "basic_info.csv")
    gp = os.path.join(output_dir, "export_import_grade.csv")
    if os.path.isfile(bp) and os.path.getsize(bp) > 0:
        basic = set(pd.read_csv(bp, dtype=str, encoding="utf-8-sig", usecols=["統一編號"])["統一編號"].dropna())
    if os.path.isfile(gp) and os.path.getsize(gp) > 0:
        grade = set(pd.read_csv(gp, dtype=str, encoding="utf-8-sig", usecols=["統一編號"])["統一編號"].dropna())
    return basic & grade

# =========================
# tradeAdmin
# =========================
class tradeAdmin:
    BASIC_API = "https://fbfh.trade.gov.tw/fb/common/popBasic.action"
    GRADE_API = "https://fbfh.trade.gov.tw/fb/common/popGrade.action"
    VERIFY_URL = "https://fbfh.trade.gov.tw/fb/web/queryBasicf.do"

    def __init__(self, exporter: BucketCsvExporter):
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://fbfh.trade.gov.tw/fb/web/queryBasicf.do",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://fbfh.trade.gov.tw",
            "Content-Type": "application/json;charset=UTF-8",
        }
        self.codesCom = ""
        self.verifySHidden = ""
        self.api_payload: Dict[str, Any] = {}
        self.exporter = exporter

    def initialize(self, codesCom: str, timeout: int = 10):
        self.codesCom = codesCom
        try:
            payload = {
                "state": "queryAll",
                "verifyCode": "5408",
                "verifyCodeHidden": "5408",
                "verifySHidden": "",
                "q_BanNo": self.codesCom,
                "q_ieType": "E",
            }
            res = self.session.post(self.VERIFY_URL, data=payload, headers=self.headers, timeout=timeout)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")
            el = soup.find("input", {"name": "verifySHidden"})
            if not el or not el.get("value"): raise ValueError("verifySHidden 取得失敗")
            self.verifySHidden = el["value"]
            self.api_payload = {"banNo": self.codesCom, "verifySHidden": self.verifySHidden}
            return True
        except Exception as e:
            logger.error(f"[{self.codesCom}] 初始化失敗：{e}")
            self.exporter.add_basic([self.codesCom, "", "", "", "", now_ts()])
            self.exporter.add_grade([self.codesCom, "", "", "", 0, 0, "", now_ts()])
            return False

    def get_basicData(self, timeout: int = 10):
        try:
            res = self.session.post(self.BASIC_API, json=self.api_payload, headers=self.headers, timeout=timeout)
            res.raise_for_status()
            data = res.json()
            lst = data.get("retrieveDataList") or []
            if not lst:
                self.exporter.add_basic([self.codesCom, "", "", "", "", now_ts()])
                return
            company = lst[0]
            row = [
                _safe_str(company[0]), _safe_str(company[1]), _safe_str(company[8]),
                _safe_str(company[19]), _safe_str(company[20]), now_ts()
            ]
            self.exporter.add_basic(row)
        except Exception as e:
            logger.error(f"[{self.codesCom}] 基本資料失敗：{e}")
            self.exporter.add_basic([self.codesCom, "", "", "", "", now_ts()])

    def get_gradeData(self, timeout: int = 10):
        try:
            res = self.session.post(self.GRADE_API, json=self.api_payload, headers=self.headers, timeout=timeout)
            res.raise_for_status()
            data = res.json()
            records = data.get("retrieveDataList") or []
            if not records:
                self.exporter.add_grade([self.codesCom, "", "", "", 0, 0, "", now_ts()])
                return
            for r in records:
                row = [
                    _safe_str(r[0]) if len(r) > 0 else self.codesCom,
                    _safe_str(r[1]) if len(r) > 1 else "",
                    _safe_str(r[2]) if len(r) > 2 else "",
                    _safe_str(r[3]) if len(r) > 3 else "",
                    normalize_band(r[4])[0] if len(r) > 4 else "",
                    normalize_band(r[5])[0] if len(r) > 5 else "",
                    _safe_str(r[6]) if len(r) > 6 else "",
                    now_ts()
                ]
                if not row[0]: row[0] = self.codesCom
                self.exporter.add_grade(row)
        except Exception as e:
            logger.error(f"[{self.codesCom}] 實績級距失敗：{e}")
            self.exporter.add_grade([self.codesCom, "", "", "", 0, 0, "", now_ts()])

# =========================
# Toolbox: 狀態管理 + 分段控制
# =========================
class Toolbox:
    STATE_FILE = "./logs/state.json"

    @classmethod
    def load_state(cls) -> int:
        if os.path.isfile(cls.STATE_FILE):
            try:
                with open(cls.STATE_FILE, "r", encoding="utf-8") as f:
                    return json.load(f).get("last_index", 0)
            except: return 0
        return 0

    @classmethod
    def save_state(cls, idx: int):
        with open(cls.STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"last_index": idx}, f)

    @staticmethod
    def apply_cli_slice(all_ids: List[str], args) -> List[str]:
        ids = all_ids
        if args.start or args.end:
            ids = ids[args.start:args.end]
        if args.shard_total and args.shard_idx is not None:
            ids = [cid for i, cid in enumerate(ids) if i % args.shard_total == args.shard_idx]
        if args.mod and args.rem is not None:
            ids = [cid for i, cid in enumerate(ids) if i % args.mod == args.rem]
        return ids

# =========================
# Main
# =========================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="./intput/250808需求/250808需求_169911筆目標統編_資本額1000萬up.csv")
    parser.add_argument("--output", type=str, default="./output")
    parser.add_argument("--flush-minutes", type=int, default=1)
    parser.add_argument("--start", type=int, default=None)
    parser.add_argument("--end", type=int, default=None)
    parser.add_argument("--shard-idx", type=int, default=None)
    parser.add_argument("--shard-total", type=int, default=None)
    parser.add_argument("--mod", type=int, default=None)
    parser.add_argument("--rem", type=int, default=None)
    args = parser.parse_args()

    exporter = BucketCsvExporter(output_dir=args.output, flush_minutes=args.flush_minutes)
    ta = tradeAdmin(exporter)

    all_ids = load_target_ids(args.input)
    hist_done = load_done_ids_from_outputs(args.output)
    all_ids = Toolbox.apply_cli_slice(all_ids, args)

    start_idx = Toolbox.load_state()
    sess_done: set[str] = set()
    total = len(all_ids)
    logger.info(f"總目標 {total:,}，歷史已完成 {len(hist_done):,}")

    for idx, uid in enumerate(all_ids[start_idx:], start=start_idx):
        if uid in hist_done or uid in sess_done:
            continue
        ok_init = ta.initialize(uid)
        if ok_init:
            ta.get_basicData(); ta.get_gradeData()
        sess_done.add(uid)
        exporter.export_if_due()
        Toolbox.save_state(idx)
        if idx % 50 == 0:
            logger.info(f"進度 {idx:,}/{total:,}")

    exporter.flush()
    Toolbox.save_state(0)
    logger.info("🎉 全程完成")

if __name__ == "__main__":
    main()
