import requests
import time
import random
import csv
import os
import pandas as pd
import logging
from bs4 import BeautifulSoup
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)
import json
from typing import Any, Dict, List, Optional, Tuple
import boto3
import platform
import subprocess
from botocore.exceptions import ClientError
import argparse
from datetime import datetime, timezone, timedelta
import sys
from threading import Lock
from tenacity import RetryCallState

# === 初始化 Logging 系統 ===
os.makedirs("./logs", exist_ok=True)
logging.basicConfig(
    filename="./logs/log.txt",
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# -----------------------------------------------------------------------------
# logging 基本設定
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("local_pipeline")

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
                switchIP().main()  # 你的敏感流程
                logger.info("換 IP 完成，將於等待後重試")
            except Exception as e:
                logger.exception(f"換 IP 失敗：{e}（仍按重試策略等待後再試）")
        else:
            logger.error("今日換 IP 次數已用罄，改為純等待重試（不再換 IP）")

# --- 動態 stop：依例外型別決定最多嘗試次數（含第一次呼叫）---
MAX_ATTEMPTS_BY_ERROR = {
    RateLimitError: 5,   # 429 容忍多試幾次
    NetworkError:   12,   # 網路短暫異常
    APIError:       10,    # API 非 200/維護等
    BlockedError:   12,   # 封鎖時通常要等較久
}
def stop_by_error(retry_state: RetryCallState) -> bool:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    max_attempts = MAX_ATTEMPTS_BY_ERROR.get(type(exc), 3)  # 預設 3 次
    # attempt_number 從 1 開始；當 >= max_attempts 時「停止再重試」
    return retry_state.attempt_number >= max_attempts

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

        self.verifySHidden = ""  # 預設為空
        # self.verifySHidden = "1DrSTL1zk6mFB1+dp/C65A=="  # 250710 USE
        # self.verifySHidden = "1DrSTL1zk6kT+4/gIe2wDQ=="  # 250711 USE
        # self.verifySHidden = "1DrSTL1zk6l/j67phw5ncw=="  # 250714 USE

        self.codesCom = codesCom
        self.api_payload = {}
        self.basicData = []
        self.gradeData = []

    def initialize(self):
        logging.info("初始化 Session")
        print(f"\n初始化 Session, 目標統一編號: {self.codesCom}")
        logging.info(f"=== 處理公司：{self.codesCom} ===")
        # self.session.get("https://fbfh.trade.gov.tw/fb/web/queryBasicf.do", headers=self.headers)
        # time.sleep(random.uniform(2.0, 3.0))

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
            # time.sleep(random.uniform(2.0, 4.5))  # 模擬人類點擊時間
            time.sleep(1)

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

    def get_basicData(self):
        logging.info(f"[{self.codesCom}] 抓取基本資料中...")
        api_url = "https://fbfh.trade.gov.tw/fb/common/popBasic.action"
        try:
            res = self.session.post(api_url, json=self.api_payload, headers=self.headers)
            data = res.json()
            print(f"=== 抓取1_聯絡電話/進出口資格 ===\nAPI 回應: JSON: {data}")
            time.sleep(1)

            company_data = data["retrieveDataList"][0]
            print("✅ 統一編號：", company_data[0])
            print("✅ 公司名稱：", company_data[1])
            print("✅ 電話：", company_data[8])
            print("✅ 進口資格：", company_data[19])
            print("✅ 出口資格：", company_data[20])
            self.basicData.append([company_data[0], company_data[1], company_data[8], company_data[19], company_data[20]])

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

    def export_to_csv(self, output_dir: str = "./output"):
        os.makedirs(output_dir, exist_ok=True)
        try:
            basic_path = os.path.join(output_dir, "basic_info.csv")
            grade_path = os.path.join(output_dir, "export_import_grade.csv")

            with open(basic_path, "w", newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(["統一編號", "公司名稱", "電話", "進口資格", "出口資格"])
                for row in self.basicData:
                    writer.writerow(row)

            with open(grade_path, "w", newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(["統一編號", "時間週期", "公司名稱", "公司名稱英文", "總進口實績", "總出口實績", "統計時間年"])
                for row in self.gradeData:
                    writer.writerow(row)

            logging.info("✅ 資料成功匯出至 CSV")

        except Exception as e:
            logging.error(f"❌ 匯出 CSV 時發生錯誤：{e}")

def read_DB():
    df = pd.read_csv(
        "C:/Users/wits/Downloads/HNCB/tests/crawler_hncb/國貿局資料開發/output/top_5000_companies_11404.csv",
        dtype=str,
        encoding="utf-8"
    )
    codesCom = df["統一編號"]
    return codesCom

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

    print(f"總目標: {len(target_ids)}, 已完成: {len(done_ids)}, 剩下: {len(target_ids) - len(done_ids)}")

    etl_b = ETl(out_b)
    error_count = 0
    max_error = 10
    # 設計最多連續錯誤次數, 防止進入無限錯誤迴圈

    for idx, cid in enumerate(target_ids):
        if cid in done_ids:
            continue  # 已經處裡過的直接跳過
        try:
            etl_b.crawler_business_items(cid)
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
    print(f"總共 {len(df_all)} 筆，已完成 {len(done_ids)} 筆，未完成 {len(df_pending)} 筆，已存 {pending_path}")

if __name__ == '__main__':
    codesCom = read_DB()  # 讀取目標統一編號

    start_index = 3901
    end_index = 4201
    selected_codesCom = codesCom[start_index:end_index]
    logging.info("\n")
    ta = tradeAdmin("")

    for num in selected_codesCom:
        ta.codesCom = str(num)
        # ta.api_payload["banNo"] = ta.codesCom
        ta.initialize()
        ta.get_basicData()
        ta.get_gradeData()

    ta.export_to_csv("./output")
    logging.info("🎉 所有公司資料處理完畢")
