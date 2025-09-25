# tradeAdmin_optimized.py
# -*- coding: utf-8 -*-
import os
import csv
import time
import logging
from datetime import datetime
from typing import List, Dict, Any, Iterable, Optional, Set

import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.exceptions import RequestException, Timeout

# =========================
# 1) Logging 強化（檔案 + 主控台）
# =========================
os.makedirs("./logs", exist_ok=True)
logger = logging.getLogger("tradeAdmin")
logger.setLevel(logging.INFO)

# 檔案
_fh = logging.FileHandler("./logs/tradeAdmin.log", encoding="utf-8")
_fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
# 主控台
_sh = logging.StreamHandler()
_sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

if not logger.handlers:
    logger.addHandler(_fh)
    logger.addHandler(_sh)

# =========================
# 共同常數與小工具
# =========================
def _minute_bucket(dt: datetime, step: int = 10) -> str:
    m = (dt.minute // step) * step
    return dt.strftime(f"%Y%m%d_%H{m:02d}")

BASIC_HEADERS = ["統一編號", "公司名稱", "電話", "進口資格", "出口資格", "查詢時間"]
GRADE_HEADERS = ["統一編號", "時間週期", "公司名稱", "公司名稱英文", "總進口實績", "總出口實績", "統計時間年", "查詢時間"]

def _safe_str(v: Any) -> str:
    return "" if v is None else str(v).strip()

# =========================
# 2) 匯出器：分桶、append、只在新檔寫表頭；同時支援 upsert done 名單
# =========================
class BucketCsvExporter:
    """
    固定檔名版本：
    - basic 寫入 ./output/basic_info.csv
    - grade 寫入 ./output/export_import_grade.csv
    - 仍維持每 10 分鐘 flush 緩存（避免太頻繁 I/O）
    """
    def __init__(self, output_dir="./output", flush_minutes=10, encoding="utf-8-sig"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.encoding = encoding

        # 固定檔名
        self.basic_path = os.path.join(self.output_dir, "basic_info.csv")
        self.grade_path = os.path.join(self.output_dir, "export_import_grade.csv")

        # 緩存 & flush 設定
        self.basic_buffer: List[List[Any]] = []
        self.grade_buffer: List[List[Any]] = []
        self.flush_window_sec = flush_minutes * 60
        self._last_export_ts = time.time()

        # 如果檔案不存在，先寫表頭
        self._ensure_headers()

    def _ensure_headers(self):
        if not os.path.exists(self.basic_path):
            with open(self.basic_path, "w", newline="", encoding=self.encoding) as f:
                csv.writer(f).writerow(BASIC_HEADERS)
        if not os.path.exists(self.grade_path):
            with open(self.grade_path, "w", newline="", encoding=self.encoding) as f:
                csv.writer(f).writerow(GRADE_HEADERS)

    def add_basic(self, row: List[Any]):
        self.basic_buffer.append(row)

    def add_grade(self, row: List[Any]):
        self.grade_buffer.append(row)

    def _append_rows(self, path: str, rows: List[List[Any]]):
        if not rows:
            return
        with open(path, "a", newline="", encoding=self.encoding) as f:
            w = csv.writer(f)
            w.writerows(rows)

    def flush(self):
        flushed_basic = len(self.basic_buffer)
        flushed_grade = len(self.grade_buffer)

        self._append_rows(self.basic_path, self.basic_buffer)
        self._append_rows(self.grade_path, self.grade_buffer)

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
    ids = (df["統一編號"]
           .dropna()
           .astype(str)
           .str.strip()
           .tolist())
    # 去重且保留原始順序
    seen = set()
    unique_ids = []
    for x in ids:
        if x not in seen:
            unique_ids.append(x)
            seen.add(x)
    return unique_ids

def load_done_ids_from_outputs(output_dir: str) -> Set[str]:
    """只從固定成品檔統計已完成統編，避免把 pending 或其他檔案算進來"""
    done: set[str] = set()
    targets = {"basic_info.csv", "export_import_grade.csv"}
    if not os.path.isdir(output_dir):
        return done

    for fn in os.listdir(output_dir):
        if fn not in targets:
            continue
        path = os.path.join(output_dir, fn)
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            continue
        try:
            # 這兩個檔一定都有「統一編號」欄
            df = pd.read_csv(path, dtype=str, encoding="utf-8-sig", usecols=["統一編號"])
            ids = df["統一編號"].dropna().astype(str).str.strip()
            done.update(ids)
        except Exception as e:
            logger.warning(f"略過 {fn}（讀檔失敗）：{e}")
    return done

def export_pending_ids(all_ids: Iterable[str], done_ids: Set[str], pending_path: str):
    pend = [cid for cid in all_ids if cid not in done_ids]
    pd.DataFrame({"統一編號": pend}).to_csv(pending_path, index=False, encoding="utf-8-sig")
    logger.info(f"📄 未完成名單已輸出：{pending_path}（未完成 {len(pend)} 筆）")

# =========================
# 4) tradeAdmin：加強例外處理 + 空包也落檔
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
        self.codesCom = ""          # 當前目標統編
        self.verifySHidden = ""     # 由 initialize() 取得
        self.api_payload: Dict[str, Any] = {}
        self.exporter = exporter    # 外部注入的分桶匯出器

    # --- 取得 verifySHidden ---
    def initialize(self, codesCom: str, timeout: int = 10):
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
            res = self.session.post(self.VERIFY_URL, data=payload, headers=self.headers, timeout=timeout)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")
            el = soup.find("input", {"name": "verifySHidden"})
            if not el or not el.get("value"):
                raise ValueError("verifySHidden 取得失敗")
            self.verifySHidden = el["value"]
            self.api_payload = {"banNo": self.codesCom, "verifySHidden": self.verifySHidden}
            logger.info(f"✅ verifySHidden 擷取成功：{self.verifySHidden}")
        except (Timeout, RequestException) as e:
            logger.error(f"❌ 初始化連線錯誤：{e}")
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # 空包也落檔（至少把統編寫出去）
            self.exporter.add_basic([now, self.codesCom, "", "", "", ""])
            self.exporter.add_grade([now, self.codesCom, "", "", "", 0, 0, ""])
            return False
        except Exception as e:
            logger.error(f"❌ 初始化解析錯誤：{e}")
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.exporter.add_basic([now, self.codesCom, "", "", "", ""])
            self.exporter.add_grade([now, self.codesCom, "", "", "", 0, 0, ""])
            return False
        return True

    # --- 先探測是否有資料（空包也要寫出統編） ---
    def has_data(self, timeout: int = 10) -> bool:
        try:
            res = self.session.post(self.BASIC_API, json=self.api_payload, headers=self.headers, timeout=timeout)
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

    # --- 基本資料 ---
    def get_basicData(self, timeout: int = 10):
        try:
            res = self.session.post(self.BASIC_API, json=self.api_payload, headers=self.headers, timeout=timeout)
            res.raise_for_status()
            data = res.json()
            lst = data.get("retrieveDataList") or []
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if not lst:  # 空包也要落檔
                self.exporter.add_basic([now, self.codesCom, "", "", ""])
                logger.info(f"[{self.codesCom}] 無基本資料：已以空值落檔")
                return
            company = lst[0]
            # 索引：0=統編, 1=公司名, 8=電話, 19/20=進出口資格
            row = [
                _safe_str(company[0]),
                _safe_str(company[1]),
                _safe_str(company[8]),
                _safe_str(company[19]),
                _safe_str(company[20]),
                now
            ]
            self.exporter.add_basic(row)
            logger.info(f"[{self.codesCom}] ✅ 基本資料完成")
        except (Timeout, RequestException) as e:
            logger.error(f"[{self.codesCom}] ❌ 基本資料連線錯誤：{e}")
            self.exporter.add_basic([self.codesCom, "", "", "", ""])
        except Exception as e:
            logger.error(f"[{self.codesCom}] ❌ 基本資料解析錯誤：{e}")
            self.exporter.add_basic([self.codesCom, "", "", "", ""])

    # --- 實績級距 ---
    def get_gradeData(self, timeout: int = 10):
        try:
            res = self.session.post(self.GRADE_API, json=self.api_payload, headers=self.headers, timeout=timeout)
            res.raise_for_status()
            data = res.json()
            records = data.get("retrieveDataList") or []
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if not records:
                # 空包也落檔：至少寫入統編一列
                self.exporter.add_grade([now, self.codesCom, "", "", 0, 0, ""])
                logger.info(f"[{self.codesCom}] 無實績資料：已以空值落檔")
                return
            # 你原本是直接 extend records，這裡保守轉成固定欄順序
            for r in records:
                # r 需能對應你原本 CSV 欄位的順序；若 API 結構不同，這裡做最小映射
                # 先盡量保有統編，如果 API 有年/期等欄位，可替換下方欄位
                row = [
                    _safe_str(r[0]) if len(r) > 0 else self.codesCom,  # 統一編號
                    _safe_str(r[1]) if len(r) > 1 else "",            # 時間週期
                    _safe_str(r[2]) if len(r) > 2 else "",            # 公司名稱
                    _safe_str(r[3]) if len(r) > 3 else "",            # 公司名稱英文
                    int(_safe_str(r[4])) if (len(r) > 4 and _safe_str(r[4]).isdigit()) else 0,  # 總進口實績
                    int(_safe_str(r[5])) if (len(r) > 5 and _safe_str(r[5]).isdigit()) else 0,  # 總出口實績
                    _safe_str(r[6]) if len(r) > 6 else "",            # 統計時間年
                    now
                ]
                # 若 API 並未帶統編，把 self.codesCom 回填
                if not row[0]:
                    row[0] = self.codesCom
                self.exporter.add_grade(row)
            logger.info(f"[{self.codesCom}] ✅ 實績級距完成（{len(records)} 筆）")
        except (Timeout, RequestException) as e:
            logger.error(f"[{self.codesCom}] ❌ 實績級距連線錯誤：{e}")
            self.exporter.add_grade([self.codesCom, "", "", "", 0, 0, ""])
        except Exception as e:
            logger.error(f"[{self.codesCom}] ❌ 實績級距解析錯誤：{e}")
            self.exporter.add_grade([self.codesCom, "", "", "", 0, 0, ""])

# =========================
# 5) Main：套用 10 分鐘批次匯出 + 完成/未完成名單
# =========================
def main():
    INPUT_IDS = "C:/crawler/input/250808需求/169919筆目標統編_1000萬up.csv"
    OUTPUT_DIR = "./output"
    PENDING_PATH = os.path.join(OUTPUT_DIR, "pending_ids.csv")

    exporter = BucketCsvExporter(output_dir=OUTPUT_DIR, flush_minutes=5)
    ta = tradeAdmin(exporter)

    all_ids = load_target_ids(INPUT_IDS)
    hist_done = load_done_ids_from_outputs(OUTPUT_DIR)  # 歷史已完成（舊檔）
    sess_done: set[str] = set()  # 本輪已完成

    total = len(all_ids)
    logger.info(f"總目標 {total:,}，歷史已完成 {len(hist_done):,}，尚待 {total - len(hist_done):,}")

    for idx, uid in enumerate(all_ids, start=1):
        if uid in hist_done or uid in sess_done:
            continue

        ok_init = ta.initialize(uid)
        if ok_init:
            if ta.has_data():
                ta.get_basicData()
                ta.get_gradeData()
            else:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                exporter.add_basic([now, uid, "", "", ""])  # 依你目前欄位順序放置
                exporter.add_grade([now, uid, "", "", 0, 0, ""])
        else:
            # initialize 失敗時，上面已經加空包；這裡不用重複加
            pass

        sess_done.add(uid)
        exporter.export_if_due()

        if idx % 50 == 0:
            remaining = total - len(hist_done) - len(sess_done)
            logger.info(f"進度 {idx:,}/{total:,} | 本輪完成 {len(sess_done):,} | 尚待 {remaining:,}")

    exporter.flush()

    # 以「歷史 + 本輪」為已完成集合，輸出未完成名單
    all_done = hist_done | sess_done
    pend = [cid for cid in all_ids if cid not in all_done]
    pd.DataFrame({"統一編號": pend}).to_csv(PENDING_PATH, index=False, encoding="utf-8-sig")
    logger.info(f"🎉 全程完成。本輪完成 {len(sess_done):,}，歷史+本輪合計 {len(all_done):,}，未完成 {len(pend):,}")

if __name__ == "__main__":
    main()
