import csv
import datetime
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pymysql
import requests
import urllib3
from pandas import DataFrame
# optional: load .env into environment (so MYSQL_* can be read)
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    # python-dotenv not installed or .env missing; ignore
    pass

"""
行政院工程委員會 PCC Pipeline（合併版）
目前策略：
1. 下載 Excel
2. Excel 轉 CSV
3. CSV 寫入 MySQL 暫存表
4. 從暫存表比對後寫入正式表

注意：
- 目前先維持單檔，不拆 daily / db_loader / ETL
- 目的是先確認既有流程與結構問題
"""

# =========================
# 全域設定
# =========================

URL_EXCELLENT = "https://web.pcc.gov.tw/vms/emlm/emlmPublicSearch/queryEMFile/xls"
URL_BLACKLIST = "https://web.pcc.gov.tw/vms/rvlm/rvlmPublicSearch/queryRVFile/xls"

LOG_FILE = "./logs/etl_run.log"
DEFAULT_OUTPUT_DIR = "./data"

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
os.makedirs(DEFAULT_OUTPUT_DIR, exist_ok=True)


# =========================
# Logger
# =========================
def get_logger(name: str = "pcc_pipeline") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )

    file_handler = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


logger = get_logger()


# =========================
# DB 設定 / 連線
# =========================
def get_mysql_settings_from_env() -> Dict[str, Any]:
    host = os.getenv("MYSQL_HOST")
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    db = os.getenv("MYSQL_DB")
    port = int(os.getenv("MYSQL_PORT", "3306"))

    if not host or not user or not password or not db:
        raise ValueError("MySQL 環境變數不足：MYSQL_HOST / MYSQL_USER / MYSQL_PASSWORD / MYSQL_DB 必填。")

    return {
        "host": host,
        "user": user,
        "password": password,
        "database": db,
        "port": port,
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
        "autocommit": False,
    }


def connect_mysql(cfg: Dict[str, Any]):
    return pymysql.connect(**cfg)

def safe_insert(cursor, insert_sql: str, rows: List[Tuple], logger: logging.Logger, table_name: str = "") -> int:
    """
    安全寫入 MySQL：
    - 先嘗試 executemany（快）
    - 若失敗，逐 row insert
    - 壞資料寫入 logger，不中斷 pipeline
    """

    if not rows:
        return 0

    try:
        cursor.executemany(insert_sql, rows)
        return len(rows)

    except Exception as batch_error:

        logger.warning(
            f"[safe_insert] batch insert failed for table={table_name}, fallback to row-by-row. error={batch_error}"
        )

        success = 0
        failed = 0

        for idx, row in enumerate(rows):

            try:
                cursor.execute(insert_sql, row)
                success += 1

            except Exception as row_error:

                failed += 1

                logger.error(
                    f"[safe_insert] row insert failed table={table_name} row_index={idx} data={row} error={row_error}"
                )

        logger.warning(
            f"[safe_insert] partial insert result table={table_name} success={success} failed={failed}"
        )

        return success


def msdb_to_df(mydb_conn, sql: str) -> DataFrame:
    with mydb_conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()

    # DictCursor 下 fetchall() 回來通常是 list[dict]
    return DataFrame(rows)


def _current_db_name(conn) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT DATABASE() AS db")
        row = cur.fetchone()
        return str(row.get("db") or "").strip()


def table_exists(conn, table_name: str, schema_name: Optional[str] = None) -> bool:
    schema = (schema_name or _current_db_name(conn) or "crawlerdb").strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 AS ok
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
            LIMIT 1
            """,
            (schema, table_name),
        )
        return cur.fetchone() is not None


# =========================
# 小工具
# =========================
def _tw_roc_to_iso(s: Any) -> Optional[str]:
    """民國 yyyMMdd -> 西元 yyyy-MM-dd；空字串或格式不對回傳 None"""
    if s is None:
        return None

    s = str(s).strip().replace("　", "")
    if not s:
        return None

    if len(s) >= 7 and s[:3].isdigit():
        y = int(s[:3]) + 1911
        mm = s[3:5]
        dd = s[5:7]
        return f"{y:04d}-{mm}-{dd}"

    return None


def _to_int_or_none(v: Any) -> Optional[int]:
    v = str(v or "").strip()
    return int(v) if v.isdigit() else None


def count_csv_rows(path: str) -> Tuple[int, int]:
    if not os.path.exists(path):
        logger.warning(f"CSV 檔案不存在: {path}")
        return 0, 0

    total = 0
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        for _ in r:
            total += 1

    header = 1 if total > 0 else 0
    data_rows = max(0, total - header)

    logger.info(f"CSV 總列數(含表頭): {total}")
    logger.info(f"CSV 資料列數(不含表頭): {data_rows}")
    return total, data_rows


# =========================
# 下載 / 轉檔
# =========================
def download_and_convert_to_csv(url: str, category_name: str, output_dir: str = DEFAULT_OUTPUT_DIR) -> Optional[Tuple[str, str]]:
    current_ts = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    os.makedirs(output_dir, exist_ok=True)

    temp_excel = os.path.join(output_dir, f"temp_{category_name}_{current_ts}.xls")
    csv_path = os.path.join(output_dir, f"{category_name}_{current_ts}.csv")

    try:
        logger.info(f"📥 開始下載 [{category_name}]，已略過 SSL 驗證...")
        with requests.get(url, stream=True, timeout=60, verify=False) as resp:
            resp.raise_for_status()

            with open(temp_excel, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        if not os.path.exists(temp_excel) or os.path.getsize(temp_excel) == 0:
            raise ValueError(f"下載失敗或檔案為空：{temp_excel}")

        logger.info(f"🔄 轉換中：{category_name} (Excel -> CSV)")
        df = pd.read_excel(temp_excel, engine="xlrd")
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        logger.info(f"✅ 轉換成功：{csv_path}")

        return temp_excel, csv_path

    except Exception as e:
        logger.exception(f"❌ 處理 [{category_name}] 時發生錯誤：{e}")
        return None


# =========================
# 自動異常判斷（先保留）
# =========================
class AutomaticAbnormalJudgment:
    def __init__(self, url_checker, api_checker, crawler_checker):
        self.api_checker = api_checker
        self.crawler_checker = crawler_checker
        self.url_checker = url_checker

    def judgmen(self, source_id: str) -> bool:
        """
        綜合評估是否應啟動自動下載任務
        待資料庫與雲端部署整合後，繼續開發
        """
        if self.api_checker.has_issue(source_id):
            return False
        if self.crawler_checker.has_issue(source_id):
            return False
        if not self.url_checker.is_valid(source_id):
            return False
        return True

    def send_telegram(self, message: str, token: str, chat_id: str):
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message}
        requests.post(url, data=payload, timeout=30)


# =========================
# daily cleanup（先保留）
# =========================
def truncate_daily_tables(conn, logger: Optional[logging.Logger] = None) -> None:
    """
    目前保留 legacy cleanup 結構。
    注意：這兩張表未必和 PCC 有關，先保留但不主流程呼叫。
    """
    logger = logger or get_logger()

    with conn.cursor() as cur:
        if table_exists(conn, "pcc_excellent_tmp", "crawlerdb"):
            cur.execute("TRUNCATE TABLE crawlerdb.pcc_excellent_tmp")
        if table_exists(conn, "pcc_expire_tmp", "crawlerdb"):
            cur.execute("TRUNCATE TABLE crawlerdb.pcc_expire_tmp")

    conn.commit()
    logger.info("🧹 已清空 legacy 日批暫存表（若存在）：tmp_rawData")


# =========================
# CSV -> pcc_excellent_tmp
# =========================
def pcc_excellent_to_db(conn, csv_path: str) -> int:
    """
    目前先沿用 CSV 中介層。
    後續可再改成 xls -> df -> db，省去 CSV 落地。
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"找不到 CSV：{csv_path}")

    with open(csv_path, newline="", encoding="utf-8-sig") as csvfile:
        rows = list(csv.reader(csvfile))

    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE crawlerdb.pcc_excellent_tmp")

        if not rows:
            logger.warning("預計寫入批次內容為空，已清空 pcc_excellent_tmp。")
            conn.commit()
            return 0

        batch: List[Tuple[Any, ...]] = []

        for idx, r in enumerate(rows):
            if idx == 0:
                continue

            # 防呆：欄位不足時補空字串，避免 index error
            if len(r) < 14:
                r = r + [""] * (14 - len(r))

            r = [(r[i] or "").replace(" ", "").replace("\u3000", "") for i in range(len(r))]

            # 廠商相關
            corporation_number = _to_int_or_none(r[0])

            if corporation_number is None:  # 如果統編不存在 → 直接跳過
                logger.warning(f"Skip row: corporation_number missing -> {r}")
                continue
            corporation_name = r[1] or None
            corporation_address = r[2] or None
            # 機關相關
            announce_agency_code = r[3] or None
            announce_agency_name = r[4] or None
            announce_agency_address = r[5] or None
            contact_person = r[6] or None
            contact_phone = r[7] or None
            announce_agency_mail = r[8] or None
            announce_agency_no = r[12] or None
            # 標案/依據
            judgment_no = r[9] or None
            # 時間
            effective_date = _tw_roc_to_iso(r[10]) or None
            expire_date = _tw_roc_to_iso(r[11]) or None
            # 其他
            remark = r[13] or None

            batch.append((
                corporation_number,
                corporation_name,
                corporation_address,
                announce_agency_code,
                announce_agency_name,
                announce_agency_address,
                contact_person,
                contact_phone,
                announce_agency_mail,
                judgment_no,
                effective_date,
                expire_date,
                announce_agency_no,
                remark,
            ))

        insert_sql = """
            INSERT INTO crawlerdb.pcc_excellent_tmp
            (
                Corporation_number,
                Corporation_name,
                Corporation_address,
                Announce_agency_code,
                Announce_agency_name,
                Announce_agency_address,
                Contact_person,
                Contact_phone,
                Announce_agency_mail,
                Judgment_no,
                Effective_date,
                Expire_date,
                Announce_agency_no,
                Remark
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        # cursor.executemany(insert_sql, batch)
        inserted = safe_insert(
            cursor,
            insert_sql,
            batch,
            logger,
            table_name="pcc_excellent_tmp"
        )

    # conn.commit()
    # logger.info(f"✅ pcc_excellent_tmp 寫入 {len(batch)} 筆")
    # return len(batch)
    conn.commit()
    logger.info(f"✅ pcc_excellent_tmp 寫入 {inserted} 筆 (original batch={len(batch)})")
    return inserted


# =========================
# CSV -> pcc_blacklist_tmp
# 先做同樣骨架，欄位對應之後再精修
# =========================
def pcc_expire_to_db(conn, csv_path: str) -> int:
    """
    blacklist 欄位結構和 excellent 不同。
    這裡先保留入口，避免主流程卡死。
    等你提供 blacklist tmp table schema 後再精準 mapping。
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"找不到 CSV：{csv_path}")

    with open(csv_path, newline="", encoding="utf-8-sig") as csvfile:
        rows = list(csv.reader(csvfile))

    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE crawlerdb.pcc_expire_tmp")

        if not rows:
            logger.warning("預計寫入批次內容為空，已清空 pcc_expire_tmp。")
            conn.commit()
            return 0

        batch: List[Tuple[Any, ...]] = []

        for idx, r in enumerate(rows):
            if idx == 0:
                continue

            # 防呆：欄位不足時補空字串，避免 index error
            if len(r) < 14:
                r = r + [""] * (14 - len(r))

            r = [(r[i] or "").replace(" ", "").replace("\u3000", "") for i in range(len(r))]

            # 廠商相關
            corporation_number = _to_int_or_none(r[0])

            if corporation_number is None:  # 如果統編不存在 → 直接跳過
                logger.warning(f"Skip row: corporation_number missing -> {r}")
                continue
            corporation_name = r[2] or None
            corporation_address = r[3] or None
            corporation_country = r[4] or None
            corporation_principal = r[10] or None
            corporation_principal_id = r[11] or None
            # 機關相關
            announce_agency_name = r[6] or None
            announce_agency_no = r[5] or None
            announce_agency_address = r[7]  or None
            contact_person = r[8] or None
            contact_no = r[9] or None
            # 標案/依據
            case_name = r[12] or None
            case_no = r[1] or None
            case_appeal_result = r[19] or None
            # 法律
            judgment_doc_no = r[20] or None
            judgment_gpa101_clause = r[13] or None
            judgment_effective_duration = r[17] or None
            judgment_no = r[21] or None
            judgment_range_date = r[22] or None
            judgment_info = r[23] or None
            # 時間
            original_announce_date = _tw_roc_to_iso(r[14]) or None
            announce_date = _tw_roc_to_iso(r[15]) or None
            effective_date = _tw_roc_to_iso(r[16]) or None
            expire_date = _tw_roc_to_iso(r[18]) or None
            # 其他
            remark = r[24] or None

            batch.append((
                corporation_number,
                case_no,
                corporation_name,
                corporation_address,
                corporation_country,
                announce_agency_no,
                announce_agency_name,
                announce_agency_address,
                contact_person,
                contact_no,
                corporation_principal,
                corporation_principal_id,
                case_name,
                judgment_gpa101_clause,
                original_announce_date,
                announce_date,
                effective_date,
                judgment_effective_duration,
                expire_date,
                case_appeal_result,
                judgment_doc_no,
                judgment_no,
                judgment_range_date,
                judgment_info,
                remark
            ))

        insert_sql = """
            INSERT INTO crawlerdb.pcc_expire_tmp
            (
                Corporation_number,
                Case_no,
                Corporation_name,
                Corporation_address,
                Corporation_country,
                Announce_agency_no,
                Announce_agency_name,
                Announce_agency_address,
                Contact_person,
                Contact_no,
                Corporation_principal,
                Corporation_principal_id,
                Case_name,
                Judgment_gpa101_clause,
                Original_announce_date,
                Announce_date,
                Effective_date,
                Judgment_effective_duration,
                Expire_date,
                Case_appeal_result,
                Judgment_doc_no,
                Judgment_no,
                Judgment_range_date,
                Judgment_info,
                Remark
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
        """
        # cursor.executemany(insert_sql, batch)
        inserted = safe_insert(
            cursor,
            insert_sql,
            batch,
            logger,
            table_name="pcc_expire_tmp"
        )

    # conn.commit()
    # logger.info(f"✅ pcc_expire_tmp 寫入 {len(batch)} 筆")
    # return len(batch)
    conn.commit()
    logger.info(f"✅ pcc_expire_tmp 寫入 {inserted} 筆 (original batch={len(batch)})")
    return inserted


# =========================
# excellent 異動比對 -> 正式表
# =========================
def sync_pcc_excellent_to_main(conn) -> int:
    """
    以 Corporation_number 為主鍵概念，將暫存表與正式表最新一筆比對，
    新增 / 異動者寫入正式表。
    """
    with conn.cursor() as cursor:
        count_sql = """
        SELECT COUNT(*) AS cnt
        FROM crawlerdb.pcc_excellent_tmp a
        LEFT JOIN crawlerdb.PccExcellent t
          ON t.Corporation_number = a.Corporation_number
        LEFT JOIN crawlerdb.PccExcellent t2
          ON t2.Corporation_number = t.Corporation_number
         AND t2.Update_Time > t.Update_Time
        WHERE
          t2.Corporation_number IS NULL
          AND (
            t.Corporation_number IS NULL OR
            NOT (
              a.Corporation_number      <=> t.Corporation_number AND
              a.Corporation_name        <=> t.Corporation_name AND
              a.Corporation_address     <=> t.Corporation_address AND
              a.Announce_agency_code    <=> t.Announce_agency_code AND
              a.Announce_agency_name    <=> t.Announce_agency_name AND
              a.Announce_agency_address <=> t.Announce_agency_address AND
              a.Contact_person          <=> t.Contact_person AND
              a.Contact_phone           <=> t.Contact_phone AND
              a.Announce_agency_mail    <=> t.Announce_agency_mail AND
              a.Judgment_no             <=> t.Judgment_no AND
              a.Effective_date          <=> t.Effective_date AND
              a.Expire_date             <=> t.Expire_date AND
              a.Announce_agency_no      <=> t.Announce_agency_no AND
              a.Remark                  <=> t.Remark
            )
          );
        """
        cursor.execute(count_sql)
        cnt_row = cursor.fetchone()
        changed_count = int(cnt_row["cnt"] or 0)
        logger.info(f"PCC Excellent 本日異動數（含新統編與內容變更）: {changed_count}")

        insert_sql = """
        INSERT INTO crawlerdb.PccExcellent
        (
            Corporation_number,
            Corporation_name,
            Corporation_address,
            Announce_agency_code,
            Announce_agency_name,
            Announce_agency_address,
            Contact_person,
            Contact_phone,
            Announce_agency_mail,
            Judgment_no,
            Effective_date,
            Expire_date,
            Announce_agency_no,
            Remark
        )
        SELECT
            a.Corporation_number,
            a.Corporation_name,
            a.Corporation_address,
            a.Announce_agency_code,
            a.Announce_agency_name,
            a.Announce_agency_address,
            a.Contact_person,
            a.Contact_phone,
            a.Announce_agency_mail,
            a.Judgment_no,
            a.Effective_date,
            a.Expire_date,
            a.Announce_agency_no,
            a.Remark
        FROM crawlerdb.pcc_excellent_tmp a
        LEFT JOIN crawlerdb.PccExcellent t
          ON t.Corporation_number = a.Corporation_number
        LEFT JOIN crawlerdb.PccExcellent t2
          ON t2.Corporation_number = t.Corporation_number
         AND t2.Update_Time > t.Update_Time
        WHERE
          t2.Corporation_number IS NULL
          AND (
            t.Corporation_number IS NULL OR
            NOT (
              a.Corporation_number      <=> t.Corporation_number AND
              a.Corporation_name        <=> t.Corporation_name AND
              a.Corporation_address     <=> t.Corporation_address AND
              a.Announce_agency_code    <=> t.Announce_agency_code AND
              a.Announce_agency_name    <=> t.Announce_agency_name AND
              a.Announce_agency_address <=> t.Announce_agency_address AND
              a.Contact_person          <=> t.Contact_person AND
              a.Contact_phone           <=> t.Contact_phone AND
              a.Announce_agency_mail    <=> t.Announce_agency_mail AND
              a.Judgment_no             <=> t.Judgment_no AND
              a.Effective_date          <=> t.Effective_date AND
              a.Expire_date             <=> t.Expire_date AND
              a.Announce_agency_no      <=> t.Announce_agency_no AND
              a.Remark                  <=> t.Remark
            )
          );
        """
        inserted_rows = cursor.execute(insert_sql)

    conn.commit()
    logger.info(f"✅ PccExcellent 新增/異動寫入筆數: {inserted_rows}")
    return inserted_rows


# =========================
# expire 異動比對 -> 正式表
# =========================
def sync_pcc_expire_to_main(conn) -> int:
    """
    以 Corporation_number 為主鍵概念，將暫存表與正式表最新一筆比對，
    新增 / 異動者寫入正式表。
    """
    with conn.cursor() as cursor:
        count_sql = """
        SELECT COUNT(*) AS cnt
        FROM crawlerdb.pcc_expire_tmp a
        LEFT JOIN crawlerdb.PccExpire t
          ON t.Corporation_number = a.Corporation_number
        LEFT JOIN crawlerdb.PccExpire t2
          ON t2.Corporation_number = t.Corporation_number
         AND t2.Update_Time > t.Update_Time
        WHERE
          t2.Corporation_number IS NULL
          AND (
            t.Corporation_number IS NULL OR
            NOT (          
              a.Corporation_number           <=> t.Corporation_number AND
              a.Case_no                      <=> t.Case_no AND
              a.Corporation_name             <=> t.Corporation_name AND
              a.Corporation_address          <=> t.Corporation_address AND
              a.Corporation_country          <=> t.Corporation_country AND
              a.Announce_agency_no           <=> t.Announce_agency_no AND
              a.Announce_agency_name         <=> t.Announce_agency_name AND
              a.Announce_agency_address      <=> t.Announce_agency_address AND
              a.Contact_person               <=> t.Contact_person AND
              a.Contact_no                   <=> t.Contact_no AND
              a.Corporation_principal        <=> t.Corporation_principal AND
              a.Corporation_principal_id     <=> t.Corporation_principal_id AND
              a.Case_name                    <=> t.Case_name AND
              a.Judgment_gpa101_clause       <=> t.Judgment_gpa101_clause AND
              a.Original_announce_date       <=> t.Original_announce_date AND
              a.Announce_date                <=> t.Announce_date AND
              a.Effective_date               <=> t.Effective_date AND
              a.Judgment_effective_duration  <=> t.Judgment_effective_duration AND
              a.Expire_date                  <=> t.Expire_date AND
              a.Case_appeal_result           <=> t.Case_appeal_result AND
              a.Judgment_doc_no              <=> t.Judgment_doc_no AND
              a.Judgment_no                  <=> t.Judgment_no AND
              a.Judgment_range_date          <=> t.Judgment_range_date AND
              a.Judgment_info                <=> t.Judgment_info AND
              a.Remark                       <=> t.Remark
            )
          );
        """
        cursor.execute(count_sql)
        cnt_row = cursor.fetchone()
        changed_count = int(cnt_row["cnt"] or 0)
        logger.info(f"PCC Expire 本日異動數（含新統編與內容變更）: {changed_count}")

        insert_sql = """
        INSERT INTO crawlerdb.PccExpire
        (
            Corporation_number,
            Case_no,
            Corporation_name,
            Corporation_address,
            Corporation_country,
            Announce_agency_no,
            Announce_agency_name,
            Announce_agency_address,
            Contact_person,
            Contact_no,
            Corporation_principal,
            Corporation_principal_id,
            Case_name,
            Judgment_gpa101_clause,
            Original_announce_date,
            Announce_date,
            Effective_date,
            Judgment_effective_duration,
            Expire_date,
            Case_appeal_result,
            Judgment_doc_no,
            Judgment_no,
            Judgment_range_date,
            Judgment_info,
            Remark
        )
        SELECT
            a.Corporation_number,
            a.Case_no,
            a.Corporation_name,
            a.Corporation_address,
            a.Corporation_country,
            a.Announce_agency_no,
            a.Announce_agency_name,
            a.Announce_agency_address,
            a.Contact_person,
            a.Contact_no,
            a.Corporation_principal,
            a.Corporation_principal_id,
            a.Case_name,
            a.Judgment_gpa101_clause,
            a.Original_announce_date,
            a.Announce_date,
            a.Effective_date,
            a.Judgment_effective_duration,
            a.Expire_date,
            a.Case_appeal_result,
            a.Judgment_doc_no,
            a.Judgment_no,
            a.Judgment_range_date,
            a.Judgment_info,
            a.Remark
        FROM crawlerdb.pcc_expire_tmp a
        LEFT JOIN crawlerdb.PccExpire t
          ON t.Corporation_number = a.Corporation_number
        LEFT JOIN crawlerdb.PccExpire t2
          ON t2.Corporation_number = t.Corporation_number
         AND t2.Update_Time > t.Update_Time
        WHERE
          t2.Corporation_number IS NULL
          AND (
            t.Corporation_number IS NULL OR
            NOT (
              a.Corporation_number           <=> t.Corporation_number AND
              a.Case_no                      <=> t.Case_no AND
              a.Corporation_name             <=> t.Corporation_name AND
              a.Corporation_address          <=> t.Corporation_address AND
              a.Corporation_country          <=> t.Corporation_country AND
              a.Announce_agency_no           <=> t.Announce_agency_no AND
              a.Announce_agency_name         <=> t.Announce_agency_name AND
              a.Announce_agency_address      <=> t.Announce_agency_address AND
              a.Contact_person               <=> t.Contact_person AND
              a.Contact_no                   <=> t.Contact_no AND
              a.Corporation_principal        <=> t.Corporation_principal AND
              a.Corporation_principal_id     <=> t.Corporation_principal_id AND
              a.Case_name                    <=> t.Case_name AND
              a.Judgment_gpa101_clause       <=> t.Judgment_gpa101_clause AND
              a.Original_announce_date       <=> t.Original_announce_date AND
              a.Announce_date                <=> t.Announce_date AND
              a.Effective_date               <=> t.Effective_date AND
              a.Judgment_effective_duration  <=> t.Judgment_effective_duration AND
              a.Expire_date                  <=> t.Expire_date AND
              a.Case_appeal_result           <=> t.Case_appeal_result AND
              a.Judgment_doc_no              <=> t.Judgment_doc_no AND
              a.Judgment_no                  <=> t.Judgment_no AND
              a.Judgment_range_date          <=> t.Judgment_range_date AND
              a.Judgment_info                <=> t.Judgment_info AND
              a.Remark                       <=> t.Remark
            )
          );
        """
        inserted_rows = cursor.execute(insert_sql)

    conn.commit()
    logger.info(f"✅ PccExpire 新增/異動寫入筆數: {inserted_rows}")
    return inserted_rows

# =========================
# main
# =========================
def main():
    logger.info("========== PCC Pipeline Start ==========")

    # 1. 下載與轉檔
    excellent_files = download_and_convert_to_csv(URL_EXCELLENT, "pcc_excellent", output_dir=DEFAULT_OUTPUT_DIR)
    blacklist_files = download_and_convert_to_csv(URL_BLACKLIST, "pcc_blacklist", output_dir=DEFAULT_OUTPUT_DIR)

    if not excellent_files:
        raise RuntimeError("pcc_excellent 下載/轉檔失敗")
    if not blacklist_files:
        raise RuntimeError("pcc_blacklist 下載/轉檔失敗")

    excellent_xls, excellent_csv = excellent_files
    blacklist_xls, blacklist_csv = blacklist_files

    count_csv_rows(excellent_csv)
    count_csv_rows(blacklist_csv)

    # 2. DB 連線
    mysql_cfg = get_mysql_settings_from_env()
    conn = connect_mysql(mysql_cfg)

    try:
        # 3. excellent -> tmp
        pcc_excellent_to_db(conn, excellent_csv)

        # 4. blacklist -> tmp（目前僅保留流程）
        pcc_expire_to_db(conn, blacklist_csv)

        # 5. excellent tmp -> 正式表
        sync_pcc_excellent_to_main(conn)

        # 6. blacklist tmp -> 正式表
        sync_pcc_expire_to_main(conn)

        logger.info("🚀 [DONE] PCC Pipeline 執行完成")

    finally:
        conn.close()
        logger.info("🔌 MySQL connection closed")

    logger.info("========== PCC Pipeline End ==========")


if __name__ == "__main__":
    main()

