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


class switchIP:
    # ========= 工具函式：IMDSv2 取本機資訊 =========
    def imds_get(self, path: str, timeout=3) -> str:
        """
        透過 IMDSv2 取得本機中繼資料（169.254.169.254）。
        path 例如：'meta-data/instance-id'
        """
        base = "http://169.254.169.254/latest/"
        # 取 token
        t = requests.put(
            base + "api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            timeout=timeout,
        ).text
        # 取指定路徑
        r = requests.get(
            base + path,
            headers={"X-aws-ec2-metadata-token": t},
            timeout=timeout,
        )
        r.raise_for_status()
        return r.text

    def get_instance_id(self) -> str:
        return self.imds_get("meta-data/instance-id")

    def get_region_from_imds(self) -> str:
        # 讀動態實例識別文件（含 region）
        ident = self.imds_get("dynamic/instance-identity/document")
        return json.loads(ident)["region"]

    # ========= EC2 資源存取 =========
    def get_clients(self, region: Optional[str] = None):
        if not region:
            region = self.get_region_from_imds()
        ec2 = boto3.client("ec2", region_name=region)
        return ec2, region

    def get_primary_eni_id(self, ec2, instance_id: str) -> str:
        """
        取得主網卡（device index 0）的 ENI ID（用於精準查舊 EIP）。
        """
        resp = ec2.describe_instances(InstanceIds=[instance_id])
        inst = resp["Reservations"][0]["Instances"][0]
        for ni in inst.get("NetworkInterfaces", []):
            if ni.get("Attachment", {}).get("DeviceIndex") == 0:
                return ni["NetworkInterfaceId"]
        raise RuntimeError("Primary ENI not found")

    def find_eip_by_eni(self, ec2, eni_id: str) -> Optional[dict]:
        """
        查詢目前綁在該 ENI 的 EIP（若無則回 None）。
        回傳例：{'PublicIp','AllocationId','AssociationId',...}
        """
        addrs = ec2.describe_addresses(
            Filters=[{"Name": "network-interface-id", "Values": [eni_id]}]
        )["Addresses"]
        return addrs[0] if addrs else None

    # ========= 四步驟：1 解綁 → 2 釋放 → 3 申請 → 4 綁定 =========
    def step1_disassociate_old_eip(self, ec2, eip: Optional[dict], dry_run=False):
        """1) DisassociateAddress（解除舊 EIP 關聯）"""
        if not eip or not eip.get("AssociationId"):
            print("[STEP1] 無舊 EIP 關聯，略過")
            return
        print(f"[STEP1] 解除關聯：{eip.get('PublicIp')} (AssocId={eip['AssociationId']})")
        ec2.disassociate_address(AssociationId=eip["AssociationId"], DryRun=dry_run)

    def step2_release_old_eip(self, ec2, eip: Optional[dict], dry_run=False):
        """2) ReleaseAddress（釋放舊 EIP，避免閒置收費）"""
        if not eip or not eip.get("AllocationId"):
            print("[STEP2] 無舊 EIP Allocation，略過")
            return
        print(f"[STEP2] 釋放 AllocationId={eip['AllocationId']}")
        ec2.release_address(AllocationId=eip["AllocationId"], DryRun=dry_run)

    def step3_allocate_new_eip(self, ec2, dry_run=False) -> dict:
        """3) AllocateAddress（申請新 EIP），回傳 allocation 回應"""
        print("[STEP3] 申請新 EIP ...")
        alloc = ec2.allocate_address(Domain="vpc", DryRun=dry_run)
        if dry_run:
            print("[STEP3] DryRun 成功（具備 AllocateAddress 權限）")
            return {"AllocationId": "dryrun", "PublicIp": "0.0.0.0"}
        print(f"[STEP3] 新 EIP：{alloc.get('PublicIp')} (AllocationId={alloc.get('AllocationId')})")
        return alloc

    def step4_associate_eip_to_instance(self, ec2, instance_id: str, allocation_id: str, dry_run=False) -> str:
        """4) AssociateAddress（把新 EIP 綁到實例），回傳 PublicIp"""
        print(f"[STEP4] 綁定 EIP 到 Instance {instance_id} ...")
        resp = ec2.associate_address(InstanceId=instance_id, AllocationId=allocation_id, DryRun=dry_run)
        if dry_run:
            print("[STEP4] DryRun 成功（具備 AssociateAddress 權限）")
            return "0.0.0.0"
        assoc_id = resp.get("AssociationId")
        addr = ec2.describe_addresses(AllocationIds=[allocation_id])["Addresses"][0]
        print(f"[STEP4] 完成綁定 AssociationId={assoc_id}")
        return addr["PublicIp"]

    # =========（可選）重啟服務 =========
    def restart_service_if_requested(self, service: Optional[str]):
        """
        可選：重啟你的 crawler 服務。
        Windows：使用 'sc'；Linux：使用 'systemctl'。
        """
        if not service:
            return
        try:
            system = platform.system().lower()
            print(f"[SERVICE] 重啟服務：{service} ({system})")
            if "windows" in system:
                subprocess.run(["sc", "stop", service], check=False)
                time.sleep(1)
                subprocess.run(["sc", "start", service], check=True)
            else:
                subprocess.run(["sudo", "systemctl", "restart", service], check=True)
            print("[SERVICE] 服務重啟完成")
        except Exception as e:
            print(f"[WARN] 服務重啟失敗：{e}")

    # ========= 主流程 =========
    def rotate_eip_main(self, region: Optional[str], dry_run=False, service_to_restart: Optional[str] = None,
                        tag_eip: Optional[dict] = None):
        """
        主流程：找到本機 & 主網卡 → 1 解綁 → 2 釋放 → 3 申請新 EIP → 4 綁定 → （可選）重啟服務。
        """
        ec2, region = self.get_clients(region)
        instance_id = self.get_instance_id()
        eni_id = self.get_primary_eni_id(ec2, instance_id)
        print(f"[MAIN] Region={region}  InstanceId={instance_id}  PrimaryENI={eni_id}")

        old_eip = self.find_eip_by_eni(ec2, eni_id)
        if old_eip:
            print(f"[MAIN] 目前 EIP：{old_eip.get('PublicIp')} (Alloc={old_eip.get('AllocationId')})")
        else:
            print("[MAIN] 目前未綁定 EIP")

        # 1) 解綁 → 2) 釋放
        self.step1_disassociate_old_eip(ec2, old_eip, dry_run=dry_run)
        self.step2_release_old_eip(ec2, old_eip, dry_run=dry_run)

        # 3) 申請 → 4) 綁定
        alloc = self.step3_allocate_new_eip(ec2, dry_run=dry_run)
        new_ip = self.step4_associate_eip_to_instance(ec2, instance_id, alloc["AllocationId"], dry_run=dry_run)

        # （可選）給新 EIP 打標籤（例如 Group=WebCrawler / InstanceId=xxx）
        if (not dry_run) and tag_eip:
            try:
                ec2.create_tags(Resources=[alloc["AllocationId"]],
                                Tags=[{"Key": k, "Value": v} for k, v in tag_eip.items()])
                print(f"[TAG] 已為新 EIP 加上標籤：{tag_eip}")
            except ClientError as e:
                print(f"[WARN] 打標籤失敗：{e}")

        # （可選）重啟服務
        self.restart_service_if_requested(service_to_restart)

        print(f"[DONE] 新的 Public IP = {new_ip}")
        return new_ip

    # ========= CLI 入口 =========
    def parse_args(self):
        p = argparse.ArgumentParser(description="Rotate Elastic IP on current EC2 instance (IMDSv2).")
        p.add_argument("--region", help="AWS region（預設讀 IMDS）")
        p.add_argument("--dry-run", action="store_true", help="只驗證權限，不真的建立/綁定/釋放")
        p.add_argument("--restart-service", help="換 IP 後要重啟的服務名稱（Windows sc / Linux systemd）")
        p.add_argument("--tag", nargs="*",
                       help='替新 EIP 打標籤，格式：Key=Value（可多組）例：--tag Group=WebCrawler Owner=Crawler')
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
            self.rotate_eip_main(region=args.region, dry_run=args.dry_run, service_to_restart=args.restart_service,
                            tag_eip=tags)
        except ClientError as e:
            # 友善顯示常見錯
            msg = e.response.get("Error", {}).get("Message", str(e))
            code = e.response.get("Error", {}).get("Code", "ClientError")
            print(f"[ERROR] {code}: {msg}")
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
    print(f"總共 {len(df_all)} 筆，已完成 {len(done_ids)} 筆，未完成 {len(df_pending)} 筆，已存 {pending_path}")


