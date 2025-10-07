# local_pipeline.py
from __future__ import annotations
from json import JSONDecodeError
import csv
import logging
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from datetime import datetime
import pandas as pd
import requests
from requests.exceptions import HTTPError, RequestException, Timeout
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)
from tenacity import RetryCallState

import os
import sys
import json
import argparse
import platform
import subprocess
from typing import Optional
import boto3
from botocore.exceptions import ClientError

from datetime import datetime, timezone, timedelta
from threading import Lock

# from 優化.lab_ETL_商公公司與商業打包 import df_result

# -----------------------------------------------------------------------------
# logging 基本設定（主控台 + 每日輪轉檔案）
# -----------------------------------------------------------------------------
import logging
from logging.handlers import TimedRotatingFileHandler
import os

os.makedirs("./logs", exist_ok=True)

logger = logging.getLogger("local_pipeline")
logger.setLevel(logging.INFO)

# 格式
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# 主控台
sh = logging.StreamHandler()
sh.setFormatter(fmt)
logger.addHandler(sh)

# 每日換檔（UTC 00:00；台灣看起來是早上 8:00 會 rollover；若想台灣午夜換檔，可改 'when=\"midnight\"', 並用 tz-aware handler 或自行切）
fh = TimedRotatingFileHandler(
    filename="./logs/local_pipeline.log",
    when="midnight",    # 每天
    interval=1,
    backupCount=14,     # 保留 14 份
    encoding="utf-8",
    utc=True            # 用 UTC 計時（建議）；若想用系統時區就改成 False
)
fh.setFormatter(fmt)
logger.addHandler(fh)

# 可選：抑制第三方 noisy logger 等級
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


class SwitchIP:
    """Rotate Elastic IP on current EC2 instance (IMDSv2), with robust logging & propagation checks."""

    # ---- 公用：查外網 IP（安全包裝）----
    def get_public_ip(self, timeout=5) -> str:
        try:
            return requests.get("https://ifconfig.me/ip", timeout=timeout).text.strip()
        except Exception as e:
            logger.warning(f"[IP] 讀取外網 IP 失敗：{e}")
            return "unknown"

    # ========= 工具函式：IMDSv2 取本機資訊 =========
    def imds_get(self, path: str, timeout=3) -> str:
        base = "http://169.254.169.254/latest/"
        try:
            t = requests.put(
                base + "api/token",
                headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
                timeout=timeout,
            ).text
            r = requests.get(
                base + path,
                headers={"X-aws-ec2-metadata-token": t},
                timeout=timeout,
            )
            r.raise_for_status()
            return r.text
        except Exception as e:
            logger.error(f"[IMDS] 取得 {path} 失敗：{e}")
            raise

    def get_instance_id(self) -> str:
        return self.imds_get("meta-data/instance-id")

    def get_region_from_imds(self) -> str:
        ident = self.imds_get("dynamic/instance-identity/document")
        try:
            return json.loads(ident)["region"]
        except Exception as e:
            logger.error(f"[IMDS] 解析 region 失敗：{e}")
            raise

    # ========= EC2 資源存取 =========
    def get_clients(self, region: Optional[str] = None):
        if not region:
            region = self.get_region_from_imds()
        try:
            ec2 = boto3.client("ec2", region_name=region)
            return ec2, region
        except Exception as e:
            logger.error(f"[AWS] 建立 EC2 client 失敗：{e}")
            raise

    def get_primary_eni_id(self, ec2, instance_id: str) -> str:
        try:
            resp = ec2.describe_instances(InstanceIds=[instance_id])
            inst = resp["Reservations"][0]["Instances"][0]
            for ni in inst.get("NetworkInterfaces", []):
                if ni.get("Attachment", {}).get("DeviceIndex") == 0:
                    return ni["NetworkInterfaceId"]
        except Exception as e:
            logger.error(f"[AWS] 取得主 ENI 失敗：{e}")
            raise
        raise RuntimeError("Primary ENI not found")

    def find_eip_by_eni(self, ec2, eni_id: str) -> Optional[dict]:
        try:
            addrs = ec2.describe_addresses(
                Filters=[{"Name": "network-interface-id", "Values": [eni_id]}]
            )["Addresses"]
            return addrs[0] if addrs else None
        except Exception as e:
            logger.error(f"[AWS] describe_addresses 失敗：{e}")
            raise

    # ========= 四步驟：1 解綁 → 2 釋放 → 3 申請 → 4 綁定 =========
    def step1_disassociate_old_eip(self, ec2, eip: Optional[dict], dry_run=False):
        if not eip or not eip.get("AssociationId"):
            logger.info("[STEP1] 無舊 EIP 關聯，略過")
            return
        logger.info(f"[STEP1] 解除關聯：{eip.get('PublicIp')} (AssocId={eip['AssociationId']})")
        ec2.disassociate_address(AssociationId=eip["AssociationId"], DryRun=dry_run)

    def step2_release_old_eip(self, ec2, eip: Optional[dict], dry_run=False):
        if not eip or not eip.get("AllocationId"):
            logger.info("[STEP2] 無舊 EIP Allocation，略過")
            return
        logger.info(f"[STEP2] 釋放 AllocationId={eip['AllocationId']}")
        ec2.release_address(AllocationId=eip["AllocationId"], DryRun=dry_run)

    def step3_allocate_new_eip(self, ec2, dry_run=False) -> dict:
        logger.info("[STEP3] 申請新 EIP ...")
        alloc = ec2.allocate_address(Domain="vpc", DryRun=dry_run)
        if dry_run:
            logger.info("[STEP3] DryRun 成功（具備 AllocateAddress 權限）")
            return {"AllocationId": "dryrun", "PublicIp": "0.0.0.0"}
        logger.info(f"[STEP3] 新 EIP：{alloc.get('PublicIp')} (AllocationId={alloc.get('AllocationId')})")
        return alloc

    def step4_associate_eip_to_instance(self, ec2, instance_id: str, allocation_id: str, dry_run=False) -> str:
        logger.info(f"[STEP4] 綁定 EIP 到 Instance {instance_id} ...")
        resp = ec2.associate_address(InstanceId=instance_id, AllocationId=allocation_id, DryRun=dry_run)
        if dry_run:
            logger.info("[STEP4] DryRun 成功（具備 AssociateAddress 權限）")
            return "0.0.0.0"
        assoc_id = resp.get("AssociationId")
        addr = ec2.describe_addresses(AllocationIds=[allocation_id])["Addresses"][0]
        logger.info(f"[STEP4] 完成綁定 AssociationId={assoc_id}")
        return addr["PublicIp"]

    # =========（可選）重啟服務 =========
    def restart_service_if_requested(self, service: Optional[str]):
        if not service:
            return
        try:
            system = platform.system().lower()
            logger.info(f"[SERVICE] 重啟服務：{service} ({system})")
            if "windows" in system:
                subprocess.run(["sc", "stop", service], check=False)
                time.sleep(1)
                subprocess.run(["sc", "start", service], check=True)
            else:
                subprocess.run(["sudo", "systemctl", "restart", service], check=True)
            logger.info("[SERVICE] 服務重啟完成")
        except Exception as e:
            logger.warning(f"[WARN] 服務重啟失敗：{e}")

    # ========= 主流程 =========
    def rotate_eip_main(self, region: Optional[str], dry_run=False, service_to_restart: Optional[str] = None,
                        tag_eip: Optional[dict] = None, confirm_external_ip=True, confirm_timeout=60):
        ec2, region = self.get_clients(region)
        instance_id = self.get_instance_id()
        eni_id = self.get_primary_eni_id(ec2, instance_id)
        logger.info(f"[MAIN] Region={region}  InstanceId={instance_id}  PrimaryENI={eni_id}")

        old_pub = self.get_public_ip()
        logger.info(f"[IP] 換前外網 IP = {old_pub}")

        old_eip = self.find_eip_by_eni(ec2, eni_id)
        if old_eip:
            logger.info(f"[MAIN] 目前 EIP：{old_eip.get('PublicIp')} (Alloc={old_eip.get('AllocationId')})")
        else:
            logger.info("[MAIN] 目前未綁定 EIP")

        # 1) 解綁 → 2) 釋放
        self.step1_disassociate_old_eip(ec2, old_eip, dry_run=dry_run)
        self.step2_release_old_eip(ec2, old_eip, dry_run=dry_run)

        # 3) 申請 → 4) 綁定
        alloc = self.step3_allocate_new_eip(ec2, dry_run=dry_run)
        new_ip = self.step4_associate_eip_to_instance(ec2, instance_id, alloc["AllocationId"], dry_run=dry_run)

        # 標籤
        if (not dry_run) and tag_eip:
            try:
                ec2.create_tags(Resources=[alloc["AllocationId"]],
                                Tags=[{"Key": k, "Value": v} for k, v in tag_eip.items()])
                logger.info(f"[TAG] 已為新 EIP({alloc['AllocationId']}) 加上標籤：{tag_eip}")
            except ClientError as e:
                logger.warning(f"[WARN] 打標籤失敗：{e}")

        # （可選）重啟服務
        self.restart_service_if_requested(service_to_restart)

        # 確認外網 IP 已變更（傳播等待）
        if not dry_run and confirm_external_ip:
            deadline = time.time() + confirm_timeout
            observed = old_pub
            while time.time() < deadline:
                time.sleep(2)
                observed = self.get_public_ip()
                if observed == "unknown":
                    continue
                if observed == new_ip:
                    break
            logger.info(f"[IP] 換後外網 IP = {observed} (目標 EIP = {new_ip})")
            if observed != new_ip:
                logger.warning("[IP] 外網 IP 與新 EIP 尚未一致，可能仍在傳播中或出口 NAT 有差異")

        logger.info(f"[DONE] 新的 EIP = {new_ip}")
        return new_ip

    # ========= CLI 入口 =========
    def parse_args(self):
        p = argparse.ArgumentParser(description="Rotate Elastic IP on current EC2 instance (IMDSv2).")
        p.add_argument("--region", help="AWS region（預設讀 IMDS）")
        p.add_argument("--dry-run", action="store_true", help="只驗證權限，不真的建立/綁定/釋放")
        p.add_argument("--restart-service", help="換 IP 後要重啟的服務名稱（Windows sc / Linux systemd）")
        p.add_argument("--tag", nargs="*", help='替新 EIP 打標籤，格式：Key=Value（可多組）例：--tag Group=WebCrawler Owner=Crawler')
        p.add_argument("--confirm-timeout", type=int, default=60, help="等待外網 IP 與新 EIP 一致的最長秒數")
        return p.parse_args()

    def main(self):
        args = self.parse_args()
        tags = None
        if args.tag:
            tags = {}
            for kv in args.tag:
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    tags[k] = v
        try:
            self.rotate_eip_main(
                region=args.region,
                dry_run=args.dry_run,
                service_to_restart=args.restart_service,
                tag_eip=tags,
                confirm_external_ip=not args.dry_run,
                confirm_timeout=args.confirm_timeout,
            )
        except ClientError as e:
            msg = e.response.get("Error", {}).get("Message", str(e))
            code = e.response.get("Error", {}).get("Code", "ClientError")
            logger.error(f"[ERROR] {code}: {msg}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"[ERROR] 未預期錯誤：{e}")
            sys.exit(1)



class DailyQuota:
    """
    用 JSON 檔紀錄 { "date": "YYYY-MM-DD", "count": n }
    每到新的一天自動重置 count
    """
    def __init__(self, path: str, limit: int = 5, tz_offset_hours: int = 8):
        self.path = path
        self.limit = limit
        self.tz = timezone(timedelta(hours=tz_offset_hours))  # 台灣用 +8
        self._lock = Lock()
        self._ensure_file()

    def _ensure_file(self):
        if not os.path.exists(self.path):
            self._write({"date": self._today_str(), "count": 0})

    def _read(self):
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            # 壞檔就重置
            return {"date": self._today_str(), "count": 0}

    def _write(self, data):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, self.path)

    def _today_str(self):
        return datetime.now(self.tz).strftime("%Y-%m-%d")

    def allow_and_increment(self) -> bool:
        """
        若今天尚未達到上限，遞增一次並回傳 True；超過上限回傳 False。
        """
        with self._lock:
            data = self._read()
            today = self._today_str()
            if data.get("date") != today:
                data = {"date": today, "count": 0}
            if data["count"] >= self.limit:
                return False
            data["count"] += 1
            self._write(data)
            return True

    def remaining(self) -> int:
        with self._lock:
            data = self._read()
            today = self._today_str()
            if data.get("date") != today:
                return self.limit
            return max(self.limit - data.get("count", 0), 0)

# === 自訂錯誤 ===
class NetworkError(Exception):
    """無法連線（DNS 失敗 / 斷網 / 逾時）"""
class RateLimitError(Exception):
    """HTTP 429：API 流量限制"""
class APIError(Exception):
    """其他非 200 的 HTTP 錯誤"""
class BlockedError(Exception):
    """IP 被封鎖 / 需要更換 IP"""


# --- 準備各錯誤對應的等待策略（單位都是秒）---
# 建議值：429 等 30s 起跳，指數退避到 10 分鐘；網路錯誤 2s 起跳到 30s；
# API 異常/維護可拉長到 12h；預設 1~10s。
WAIT_RATE_LIMIT = wait_exponential(multiplier=1, min=30,  max=600)  + wait_random(0, 5)
WAIT_NETWORK    = wait_exponential(multiplier=1, min=2,   max=30)   + wait_random(0, 1)
WAIT_API        = wait_exponential(multiplier=1, min=60,  max=43200) + wait_random(0, 5)  # 12h 上限
WAIT_DEFAULT    = wait_exponential(multiplier=1, min=60,   max=43200)   + wait_random(0, 2)

# --- 動態 wait：依例外型別選策略 ---
def wait_by_error(retry_state: RetryCallState) -> float:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    if isinstance(exc, RateLimitError):
        return WAIT_RATE_LIMIT(retry_state)
    if isinstance(exc, NetworkError):
        return WAIT_NETWORK(retry_state)
    if isinstance(exc, (APIError, BlockedError)):
        return WAIT_API(retry_state)
    return WAIT_DEFAULT(retry_state)

# --- 每日換 IP 配額（JSON 持久化）---
quota = DailyQuota(path="./.ip_switch_quota.json", limit=5, tz_offset_hours=8)
def before_sleep_handler(retry_state: RetryCallState):
    # 額外記錄 retry log（等同 before_sleep_log）
    try:
        wait_s = retry_state.next_action.sleep if retry_state.next_action else None
    except Exception:
        wait_s = None
    logger.warning(
        f"[retry] attempt={retry_state.attempt_number} "
        f"exc={type(retry_state.outcome.exception()).__name__ if retry_state.outcome else 'NA'} "
        f"sleep={wait_s}"
    )

    exc = retry_state.outcome.exception() if retry_state.outcome else None
    # 只有 BlockedError 會嘗試換 IP（且受每日配額限制）
    if isinstance(exc, BlockedError):
        if quota.allow_and_increment():
            left = quota.remaining()
            logger.warning(f"IP 被封鎖，觸發換 IP（今日剩餘可換 {left} 次）…")
            try:
                SwitchIP().main()  # 你的敏感流程
                logger.info("換 IP 完成，將於等待後重試")
            except Exception as e:
                logger.exception(f"換 IP 失敗：{e}（仍按重試策略等待後再試）")
        else:
            logger.error("今日換 IP 次數已用罄，改為純等待重試（不再換 IP）")

# --- 動態 stop：依例外型別決定最多嘗試次數（含第一次呼叫）---
MAX_ATTEMPTS_BY_ERROR = {
    RateLimitError: 10,   # 429 容忍多試幾次
    NetworkError:   12,   # 網路短暫異常
    APIError:       6,    # API 非 200/維護等
    BlockedError:   12,   # 封鎖時通常要等較久
}
def stop_by_error(retry_state: RetryCallState) -> bool:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    max_attempts = MAX_ATTEMPTS_BY_ERROR.get(type(exc), 3)  # 預設 3 次
    # attempt_number 從 1 開始；當 >= max_attempts 時「停止再重試」
    return retry_state.attempt_number >= max_attempts

# 新增 紀錄疑似錯誤處裡

# =============================================================================
# GCIS Client
# =============================================================================
class GcisClient:
    # ---------------- 常數 -----------------
    # 商工 Company
    INFO_c  = "5F64D864-61CB-4D0D-8AD9-492047CC1EA6"
    ITEMS_c = "236EE382-4942-41A9-BD03-CA0709025E7C"
    DIR_c   = "4E5F7653-1B91-4DDC-99D5-468530FAE396"
    # 商業 Business
    ITEMS_b = "426D5542-5F05-43EB-83F9-F1300F14E1F1"  # 應用三
    INFO_b  = "7E6AFA72-AD6A-46D3-8681-ED77951D912D"  # 應用一, 這部分既可以抓 info 也可以抓 dir

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/119.0 Safari/537.36"
            )
        })

    # --- URL helpers ---
    # 公司應用一 (info)
    def _url_company(self, eid: str, cid: str) -> str:
        return (
            f"https://data.gcis.nat.gov.tw/od/data/api/{eid}"
            f"?$format=json&$filter=Business_Accounting_NO eq {cid}&$top=50"
        )
    # 商業應用三
    def _url_business3(self, eid: str, cid: str) -> str:
        return (
            f"https://data.gcis.nat.gov.tw/od/data/api/{eid}"
            f"?$format=json&$filter=President_No eq {cid}&$skip=0&$top=50"
        )
    # 商業應用一、應用三 (url 主要架構皆相同)
    def _url_business(self, eid: str, cid: str, agency: str) -> str:
        return (
            f"https://data.gcis.nat.gov.tw/od/data/api/{eid}"
            f"?$format=json&$filter=President_No eq {cid} and Agency eq {agency}"
            f"&$skip=0&$top=50"
        )

    # === 核心 GET：帶動態 retry / 前置換 IP ===
    @retry(
        wait=wait_by_error,
        stop=stop_by_error,
        retry=retry_if_exception_type((NetworkError, RateLimitError, APIError, BlockedError)),
        before_sleep=before_sleep_handler,
        reraise=True
    )
    def _get(self, url: str) -> Any:
        try:
            resp = self.session.get(url, timeout=10)
        except (Timeout, RequestException) as e:
            raise NetworkError(str(e)) from e

        # 429：速率限制
        if resp.status_code == 429:
            raise RateLimitError("HTTP 429 Too Many Requests")

        text = resp.text or ""

        # 封鎖 / 流量保護訊息 → 交給 BlockedError，會觸發 before_sleep 換 IP + 長等待
        if ("非常態介接次數" in text) or ("非授權介接之IP" in text) or ("超出本日最大介接次數" in text):
            raise BlockedError("疑似 IP 被封鎖或達上限")
        if ("本平臺介接服務達上限" in text) or ("超出同時最大連線數量" in text) or ("請稍候再試" in text):
            raise BlockedError("平臺流量保護觸發")

        # 非 200
        if not resp.ok:
            raise APIError(f"HTTP {resp.status_code}: {text[:200]}")
        # 維護/不開放：視為 APIError（走長等待）
        if "資料庫維護中" in text:
            raise APIError("資料庫維護中，暫停服務")
        if "此API不存在，請查明後繼續。" in text:
            raise APIError("API 不存在")
        if "此API不開放使用" in text or "此API不開放使用" in text.replace("\t", ""):
            raise APIError("API 不開放使用")

        # 解析 JSON
        try:
            return resp.json()
        except JSONDecodeError as e:
            raise APIError("回應不是有效 JSON") from e

    # ---------------- Public API：商工公司 -----------------
    def fetch_info_c(self, cid: str) -> Optional[Dict[str, Any]]:
        data = self._get(self._url_company(self.INFO_c, cid))
        return data[0] if data else None

    def fetch_items_c(self, cid: str) -> Optional[Dict[str, Any]]:
        return self._get(self._url_company(self.ITEMS_c, cid))

    def fetch_directors_c(self, cid: str) -> Optional[List[Dict[str, Any]]]:
        return self._get(self._url_company(self.DIR_c, cid))

    # ---------------- Public API：商工商業 -----------------
    def fetch_agency_b(
        self, cid: str
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        取得商業登記『應用三』資料，並抽出 Agency 代碼。
        回傳 (record or None, agency or None)
        """
        data: List[Dict[str, Any]] = self._get(self._url_business3(self.ITEMS_b, cid))
        if not data:
            return None, None
        rec = data[0]
        return rec, rec.get("Agency")

    def fetch_info_b(self, cid: str, agency: str) -> Optional[Dict[str, Any]]:
        data = self._get(self._url_business(self.INFO_b, cid, agency))
        return data[0] if data else None

    def fetch_items_b(self, cid: str, agency: str) -> Optional[Dict[str, Any]]:
        data = self._get(self._url_business(self.ITEMS_b, cid, agency))
        return data[0] if data else None

# =============================================================================
# ETL class
# =============================================================================
class ETl:
    def __init__(self, csv_path: Path):
        self.csv_path = Path(csv_path)

    # ---------- 小工具：空值轉 "null" -------------
    @staticmethod
    def _nz(val: str | None) -> str:
        return "null" if val in (None, "", "NULL", "null", "       ") else val

    # 商工（公司）流程  ─ info / items / directors
    def crawler_company_info(self, cid: str) -> None:
        client = GcisClient()

        try:
            info = client.fetch_info_c(cid)  # ① 公司基本資料
        except (NetworkError, RateLimitError, APIError) as e:
            logger.error("crawler_company_info(%s) failed: %s", cid, e)
            # warn = f"無資料;API失敗;{e.__class__.__name__};疑似為商號;疑似為錯誤統編"  # 公司用
            warn = f"{e.__class__.__name__}疑似統編錯誤"
            self._upsert_row(new_values={"統一編號": cid, "狀態": warn})
            return

        # ------------------------------------------------------------
        # 查無資料：只更新「狀態」欄位
        # ------------------------------------------------------------
        if not info:
            warn = "無資料;疑似為商號;疑似統編錯誤"
            self._upsert_row(
                new_values={"統一編號": cid, "狀態": warn},
            )
            logger.info("%s → %s", cid, warn)
            return

        # ------------------------------------------------------------
        # 有資料：正常組裝欄位
        # ------------------------------------------------------------
        row = {
            "統一編號": self._nz(info.get("Business_Accounting_NO")),
            "狀態": "成功",
            "類別": "公司",
            "現況說明": self._nz(info.get("Company_Status_Desc")),
            "名稱": self._nz(info.get("Company_Name")),
            "資本總額": self._nz(info.get("Capital_Stock_Amount")),
            "實收資本額": self._nz(info.get("Paid_In_Capital_Amount")),
            "負責人": self._nz(info.get("Responsible_Name")),
            "地址": self._nz(info.get("Company_Location")),
            "申登機關": self._nz(info.get("Register_Organization_Desc")),
            "核准設立日期": self._nz(info.get("Company_Setup_Date")),
            "最後核准異動日": self._nz(info.get("Change_Of_Approval_Data")),
            "廢止登記申請日期": self._nz(info.get("Revoke_App_Date")),
            "案件狀態代碼": self._nz(info.get("Case_Status")),
            "案件狀態說明": self._nz(info.get("Case_Status_Desc")),
            "停止營業申請日期": self._nz(info.get("Sus_App_Date")),
            "停止營業啟始日期": self._nz(info.get("Sus_Beg_Date")),
            "停止營業結束日期": self._nz(info.get("Sus_End_Date")),
            "查詢時間": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        # 多於 6 欄是「無資料」→ 標註異常
        if sum(v == "無資料" for k, v in row.items() if k != "狀態") >= 7:
            row["狀態"] = "c1;多欄位無資料;疑似為商號;疑似統編錯誤"

        self._upsert_row(  # ← 與前面保持同一函式名
            new_values=row,
        )
    # 處裡 商工商業 info
    def crawler_business_info(self, cid: str) -> None:  # 尚未新增判斷空值功能
        client = GcisClient()
        # --- 應用三 ---
        try:
            item_b, agency = client.fetch_agency_b(cid)
        except (NetworkError, RateLimitError, APIError) as e:
            logger.error("crawler_business(b3, %s) failed: %s", cid, e)
            warn = "疑似統編錯誤"
            self._upsert_row(
                new_values={"統一編號": cid, "狀態": warn},
            )
            return

        # 查無資料: 只更新 "狀態" 欄位
        if not item_b:
            warn = "疑似統編錯誤"
            self._upsert_row(
                new_values={"統一編號": cid, "狀態": warn},
            )
            logger.info("%s → %s", cid, warn)
            return

        print("=== 應用三 ===")
        print(item_b)
        # 有資料: 正常組裝欄位

        # --- 打包 b3 資料 ---
        b3_row = {
            "統一編號": cid,
            "申登機關代碼": agency,
            "申登機關名稱": item_b.get("Agency_Desc"),
            "設立核准日期": item_b.get("Business_Setup_Approve_Date"),
            "狀態": "b3",
        }
        self._upsert_row(  # ← 與前面保持同一函式名
            new_values=b3_row,
        )

        # ---------- 應用一 ----------
        try:
            info_b = client.fetch_info_b(cid, agency)
        except (NetworkError, RateLimitError, APIError) as e:
            logger.error("crawler_business(b1, %s) failed: %s", cid, e)
            return
        if not info_b:
            logger.warning("應用一無資料 %s", cid)
            return
        print('=== 應用一 ===')
        print(info_b)

        b1_row = {
            "統一編號": cid,
            "商號名稱": info_b.get("Business_Name"),
            "營業狀態代碼": info_b.get("Business_Current_Status"),
            "營業狀態說明": info_b.get("Business_Current_Status_Desc"),
            "登記資本額": info_b.get("Business_Register_Funds"),
            "負責人姓名": info_b.get("Responsible_Name"),
            "組織型態代碼": info_b.get("Business_Organization_Type"),
            # "董監事": ,
            "組織型態說明": info_b.get("Business_Organization_Type_Desc"),
            "商業登記地址": info_b.get("Business_Address"),
            "最後核准異動日期": info_b.get("Business_Last_Change_Date"),
            "狀態": "成功",
            "類別": "商業",
            "查詢時間": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        self._upsert_row(  # ← 與前面保持同一函式名
            new_values=b1_row,
        )

    # === 處裡商工商業 營業項目部分 ===
    def parse_business_items(self, raw: str):
        """
        更高容錯的分段法，支援一0、一０、一十、十一...，並分割每段後配對code/name
        """
        raw = self.strQ2B(raw.strip())  # 全形轉半形，確保一致性

        # (1) 強化分段規則
        # 國字+全半形數字+、，如「一0、」「一０、」「十一、」
        # 一般「一、」「二、」也要支援
        # 例如：「一0、」「一10、」「十一、」「一、」等等
        s = raw
        s = re.sub(r'(一[0-9]{1,2}|一[０-９]{1,2}|十[一二三四五六七八九]?|[一二三四五六七八九十])、', '|', s)
        # 若有「一0、」這種沒處理到的，再補一次
        s = re.sub(r'(一0|一10|一11|一12|一13|一14|一15|一16|一17|一18|一19|一20)、', '|', s)
        # 開頭或多餘分隔
        if s.startswith('|'):
            s = s[1:]
        # 再以 | 分割
        segments = [x.strip() for x in s.split('|') if x.strip()]

        codes, names = [], []
        for seg in segments:
            # 對每段抓 code+name
            m = re.match(r'^([A-Z][A-Z]?\d{5,7})(.+)$', seg)
            if m:
                codes.append(m.group(1).strip())
                names.append(m.group(2).strip())
            else:
                codes.append('')
                names.append(seg.strip())
        return codes, names

    # 清理異常值
    def clean_tail_num(self, s: str) -> str:
        # 把名稱結尾的國字/數字去掉
        s = re.sub(r'[一二三四五六七八九十０-９\d]+$', '', s)
        return s.strip(" ;，,")  # 再順便去除多餘標點和空白
    # 全形轉半形
    def strQ2B(self, ustring: str) -> str:
        """全形字元轉半形"""
        rstring = ""
        for uchar in ustring:
            inside_code = ord(uchar)
            if inside_code == 0x3000:  # 全形空格直接轉
                inside_code = 32
            elif 0xFF01 <= inside_code <= 0xFF5E:
                inside_code -= 0xfee0
            rstring += chr(inside_code)
        return rstring
    # 商工商業爬蟲_營業項目
    def crawler_business_items(self, cid: str) -> None:  # 尚未新增判斷空值功能
        client = GcisClient()

        # --- 應用三 ---
        try:
            item_b, agency = client.fetch_agency_b(cid)
        except (NetworkError, RateLimitError, APIError) as e:
            logger.error("crawler_business(b3, %s) failed: %s", cid, e)
            # 多個異常處理都失敗，基本上就是統編出錯
            warn = "疑似統編錯誤"
            warn2 = "異常"
            self._upsert_row(
                new_values={"統一編號": cid, "狀態": warn, "類別": warn2},
            )
            return

        # 查無資料: 只更新 "狀態" 欄位
        if not item_b:
            warn = "疑似統編錯誤"
            warn2 = "異常"
            self._upsert_row(
                new_values={"統一編號": cid, "狀態": warn, "類別": warn2},
            )
            logger.info("%s → %s", cid, warn)
            return

        print("=== 應用三 ===")
        print(item_b)  # output res json

        # 有資料: 正常組裝欄位
        # 處理 "營業項目" 欄位
        item_biz = item_b.get("Business_Item_Old")
        item_codes, item_names = [], []

        # 假設含有 list, 就不處裡
        if isinstance(item_biz, list):
            for d in item_biz:
                if isinstance(d, dict):
                    item_codes.append(d.get("Business_Item", ""))
                    item_names.append(d.get("Business_Item_Desc", ""))
        elif isinstance(item_biz, str) and item_biz.strip():
            item_codes, item_names = self.parse_business_items(item_biz)

        b3_row = {
            "統一編號": cid,
            "申登機關代碼": agency,
            "營業項目代碼": ";".join([x for x in item_codes if x]),
            "代號名稱": ";".join([x for x in item_names if x]),
            "狀態": "成功",
            "類別": "商業",
            "查詢時間": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        self._upsert_row(
            new_values=b3_row,
        )
    # 商工商業爬蟲_董監事
    def crawler_business_directors(self, cid: str) -> None:  # 尚未新增判斷空值功能
        client = GcisClient()

        # --- 應用三 ---
        try:
            item_b, agency = client.fetch_agency_b(cid)
        except (NetworkError, RateLimitError, APIError) as e:
            logger.error("crawler_business(b3, %s) failed: %s", cid, e)
            # 多個異常處理都失敗，基本上就是統編出錯
            warn = "疑似統編錯誤"
            warn2 = "異常"
            self._upsert_row(
                new_values={"統一編號": cid, "狀態": warn, "類別": warn2},
            )
            return

        # 查無資料: 只更新 "狀態" 欄位
        if not item_b:
            warn = "疑似統編錯誤"
            warn2 = "異常"
            self._upsert_row(
                new_values={"統一編號": cid, "狀態": warn, "類別": warn2},
            )
            logger.info("%s → %s", cid, warn)
            return

        print("=== 應用三 ===")
        print(item_b)  # output res json

        # 有資料: 正常組裝欄位
        # 處理 "營業項目" 欄位
        item_biz = item_b.get("Business_Item_Old")
        item_codes, item_names = [], []

        # 假設含有 list, 就不處裡
        if isinstance(item_biz, list):
            for d in item_biz:
                if isinstance(d, dict):
                    item_codes.append(d.get("Business_Item", ""))
                    item_names.append(d.get("Business_Item_Desc", ""))
        elif isinstance(item_biz, str) and item_biz.strip():
            item_codes, item_names = self.parse_business_items(item_biz)

        b3_row = {
            "統一編號": cid,
            "申登機關代碼": agency,
            "營業項目代碼": ";".join([x for x in item_codes if x]),
            "代號名稱": ";".join([x for x in item_names if x]),
            "狀態": "成功",
            "類別": "商業",
            "查詢時間": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        self._upsert_row(
            new_values=b3_row,
        )

    # =====================================================================
    # 核心：依統編 upsert
    # =====================================================================
    def _upsert_row(self, new_values: Dict[str, str]) -> None:
        cid = str(new_values.get("統一編號", "")).strip()

        # 若檔案不存在則建立空檔案
        if not self.csv_path.exists():
            cols = ["序號", "統一編號"] + [k for k in new_values.keys() if k != "統一編號"]
            pd.DataFrame(columns=cols).to_csv(
                self.csv_path, index=False, encoding="utf-8-sig"
            )

        # 讀取現有資料
        df = pd.read_csv(self.csv_path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
        df["統一編號"] = df["統一編號"].astype(str).str.strip()
        mask = df["統一編號"] == cid

        if not mask.any():
            # v 開頭 + 6位數流水號
            idx = len(df) + 1
            new_row = {"序號": f"v{idx:06d}", "統一編號": cid}
            for k, v in new_values.items():
                if k not in ["序號", "統一編號"]:  # 避免重複
                    new_row[k] = v
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        else:
            # 更新現有資料 (僅填空欄位或覆蓋特定欄位)
            for k, v in new_values.items():
                if k not in df.columns:
                    df[k] = ""
                if k == "狀態":
                    df.loc[mask, k] = v
                else:
                    empty = df.loc[mask, k] == ""
                    df.loc[mask & empty, k] = v

        # 確保查詢時間一定在欄位內
        if "查詢時間" not in df.columns:
            df["查詢時間"] = ""
        df["查詢時間"] = df["查詢時間"].replace("", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        # 寫回檔案
        df.to_csv(self.csv_path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)

# =============================================================================
# Demo / CLI
# =============================================================================
# 執行抓取 商工商業基本資訊
def run_crawler_business_info(path: str, out_b: str):
    # path = "./input/疑似為商號.csv"  # read 目標統編
    df = pd.read_csv(path, dtype=str)
    # out_b = Path("./output/business_infos.csv")  # 商業數據匯出 path

    target_ids = df["統一編號"].tolist()

    # 檢查已完成名單, 避免重複處裡
    try:
        df_result = pd.read_csv(out_b, dtype=str)
        done_ids = set(df_result["統一編號"].tolist())
    except FileNotFoundError:
        done_ids = set()

    logger.info(f"總目標: {len(target_ids)}, 已完成: {len(done_ids)}, 剩下: {len(target_ids) - len(done_ids)}")

    etl_b = ETl(out_b)
    error_count = 0
    max_error = 10
    # 設計最多連續錯誤次數, 防止進入無限錯誤迴圈

    for idx, cid in enumerate(target_ids):
        if cid in done_ids:
            continue  # 已經處裡過的直接跳過
        try:
            etl_b.crawler_business_items(cid)
            logger.info(f"[{idx + 1}/{len(target_ids)}] 處理 {cid} 完成")
            error_count = 0  # 成功歸零
        except Exception as e:
            logger.error(f"統編 {cid} 發生異常: {e}")
            error_count += 1
            if error_count > max_error:
                logger.error("連續異常次數超過上限, 終止程序")
                break
            continue  # 出錯 直接跳下一筆
        time.sleep(0.5)

    logger.info("=== 執行結束 ===")

# 執行抓取 商工商業營業項目
def run_crawler_business_items():
    # === read 處裡商工商業 目標統一編號 ===
    path = "./input/疑似為商號.csv"  # 目標處裡檔案
    df = pd.read_csv(path, dtype=str)
    out_b = Path("./output/business_items_250807.csv")  # 商業營業項目數據匯出 path

    target_ids = df["統一編號"].tolist()

    # 檢查已完成名單, 避免重複處裡
    try:
        df_result = pd.read_csv(out_b, dtype=str)
        done_ids = set(df_result["統一編號"].tolist())
    except FileNotFoundError:
        done_ids = set()

    print(f"總目標: {len(target_ids)}, 已完成: {len(done_ids)}, 剩下: {len(target_ids)-len(done_ids)}")

    etl_b = ETl(out_b)
    error_count = 0
    max_error = 10
    # 設計最多連續錯誤次數, 防止進入無限錯誤迴圈

    for idx, cid in enumerate(target_ids):
        if cid in done_ids:
            continue  # 已經處裡過的直接跳過
        try:
            etl_b.crawler_business_items(cid)
            print(f"[{idx+1}/{len(target_ids)}] 處理 {cid} 完成")
            error_count = 0  # 成功歸零
        except Exception as e:
            logger.error(f"統編 {cid} 發生異常: {e}")
            error_count += 1
            if error_count > max_error:
                logger.error("連續異常次數超過上限, 終止程序")
                break
            continue  # 出錯 直接跳下一筆
        time.sleep(0.5)

    print("=== 執行結束 ===")

# 執行抓取 商工商業董監事
def run_crawler_business_dirs():
    pass

# 執行抓取 商工公司基本資訊
def run_crawler_company_info(path, out_c_path):
    # === read 處裡商工公司 目標統一編號 ===
    # path = "./input/250808需求/169919筆目標統編_1000萬up.csv"  # 目標處裡統編
    df = pd.read_csv(path, dtype=str)
    # out_c = Path("./output/company_infos_250808.csv")  # 公司數據匯出 path
    out_c = Path(out_c_path)

    target_ids = df["統一編號"].tolist()

    # 檢查已完成名單, 避免重複處裡
    try:
        df_result = pd.read_csv(out_c, dtype=str)
        done_ids = set(df_result["統一編號"].tolist())
    except FileNotFoundError:
        done_ids = set()

    print(f"總目標: {len(target_ids)}, 已完成: {len(done_ids)}, 剩下: {len(target_ids) - len(done_ids)}")

    etl_c = ETl(out_c)
    error_count = 0
    max_error = 50
    # 設計最多連續錯誤次數, 防止進入無限錯誤迴圈

    for idx, cid in enumerate(target_ids):
        if cid in done_ids:
            continue  # 已經處裡過的直接跳過
        try:
            etl_c.crawler_company_info(cid)
            print(f"[{idx + 1}/{len(target_ids)}] 處理 {cid} 完成")
            error_count = 0  # 成功歸零
        except Exception as e:
            logger.error(f"統編 {cid} 發生異常: {e}")
            error_count += 1
            if error_count > max_error:
                logger.error("連續異常次數超過上限, 終止程序")
                break
            continue  # 出錯 直接跳下一筆
        time.sleep(0.5)

    print("=== 執行結束 ===")

# 尚未完成名單註記
def export_pending_ids(input_path, result_path, pending_path):
    """
    產生尚未處理完成的「統一編號」名單
    """
    df_all = pd.read_csv(input_path, dtype=str)
    try:
        df_result = pd.read_csv(result_path, dtype=str)
        done_ids = set(df_result["統一編號"].tolist())
    except FileNotFoundError:
        done_ids = set()

    df_pending = df_all[~df_all["統一編號"].isin(done_ids)]
    df_pending.to_csv(pending_path, index=False, encoding="utf-8-sig")
    logger.info(f"總共 {len(df_all)} 筆，已完成 {len(done_ids)} 筆，未完成 {len(df_pending)} 筆，已存 {pending_path}")

# 初版後 "疑似統編錯誤" 處裡
def etl_noneValue(file_path: str):
    # 讀檔
    df = pd.read_csv(
        file_path,
        encoding="utf-8-sig",  # 自動處理 UTF‑8 BOM
        dtype=str,  # 全欄位先視為字串，避免前導 0 遺失
        keep_default_na=False  # 空白欄位保持空字串，不轉成 NaN
    )

    # === 篩出「疑似錯誤統編」列
    mask = df["狀態"].str.contains("疑似統編錯誤", na=False)  # 向量化運算 較快
    df_no_data = df.loc[mask, ["序號", "統一編號", "狀態", "類別"]]

    # === 輸出目標檔
    out_path = "targetBusID.csv"
    df_no_data.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info(f"已輸出 {len(df_no_data):,} 筆 → {out_path}")


if __name__ == "__main__":
    # # 商工商業_info主程式
    # run_crawler_business_info(
    #     path="./input/250808需求/疑似為商號.csv",
    #     out_b="./output/business_items_250808.csv",
    # )
    # # 輸出未完成名單
    # export_pending_ids(
    #     input_path="./input/250808需求/疑似為商號_250818.csv",
    #     result_path="./output/business_infos_250818.csv",
    #     pending_path="./output/business_infos_pending_250818.csv"
    # )

    # 商工公司_info主程式
    run_crawler_company_info(
        # path = "./input/250808需求/169919筆目標統編_1000萬up.csv",  # 目標處裡統編
        path="./input/250808需求/疑似為商號.csv",  # 目標處裡統編
        out_c_path = "./output/company_infos_250818.csv"         # 公司數據匯出 path
    )
    # 輸出未完成名單
    export_pending_ids(
        input_path="./input/250808需求/疑似為商號.csv",  # 計算目標檔案數量
        result_path="./output/company_infos_250818.csv",  # 計算匯出檔案數量
        pending_path="./output/company_infos_pending.csv"  # IP 狀態
    )

    # # 初版後 疑似錯誤統編處裡 抓出疑似商業 目標
    # file_path = "C:/Users/wits/Downloads/HNCB/tests/crawler_hncb/優化/input/250808需求/company_infos_250808_2508181015_Update.csv"
    # etl_noneValue(file_path)

    # # 商工商業_items主程式
    # run_crawler_business_items()
    # # 輸出未完成名單
    # export_pending_ids(
    #     input_path="./input/疑似為商號.csv",
    #     result_path="./output/business_items.csv",
    #     pending_path="./output/business_items_pending.csv"
    # )


# 寫商業模組的時候，再進來公司這邊讀檔
# 透過第一個 CSV 來去跑第二個 code
# 公司的 code 兩個都壓，第二個 code，只針對
# 商業自己整理一包，商業就不會有疑似的打錯，假設有錯誤 就是在表1裡面打錯誤，等於商號這邊只會有正確的

# 接到模組即可，
# 多產的兩個檔案，雖然可能沒用，但可以 MEMO 起來，
