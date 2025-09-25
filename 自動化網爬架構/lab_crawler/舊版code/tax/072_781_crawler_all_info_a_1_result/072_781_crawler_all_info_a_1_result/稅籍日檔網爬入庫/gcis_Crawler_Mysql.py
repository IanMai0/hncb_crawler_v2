# -*- coding: utf-8 -*-
"""
稅籍日檔網爬入庫（安全修補版）
- 一律驗證 HTTPS 憑證；移除任何關閉驗證的作法
- 以 requests.Session + Retry + timeout 進行所有 HTTP/下載
- 所有 SQL 均改為參數化，杜絕 SQL Injection
- GCIS API 全改 https，並加 timeout
- 統一 MySQL 時間函式 NOW()
"""

import os
import io
import re
import csv
import json
import time
import zipfile
import logging
from datetime import datetime

import pandas as pd
import pymysql
import requests
from pandas import DataFrame
from tqdm import trange
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -------------------- 環境變數 --------------------
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# 下載與資料檔
DAILY_ZIP_URL = "https://eip.fia.gov.tw/data/BGMOPEN1.zip"
DAILY_ZIP_NAME = "gcis.zip"
DAILY_CSV_NAME = "BGMOPEN1.csv"

# HTTP 安全預設
REQ_TIMEOUT = (10, 30)  # (connect, read)

def make_session() -> requests.Session:
    s = requests.Session()
    r = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
    )
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({"User-Agent": "HNBC-TaxDaily/1.0"})
    return s

SESSION = make_session()

# -------------------- DB 連線 --------------------
def db_connect():
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset="utf8mb4",
            autocommit=False,
            connect_timeout=15,
        )
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise

# -------------------- 共用工具 --------------------
_digit8 = re.compile(r"^\d{8}$")

def assert_tax_id(cid: str) -> str:
    cid = str(cid).strip()
    if not _digit8.match(cid):
        raise ValueError("統一編號必須為 8 碼數字")
    return cid

def date_check_insql(date_str: str):
    """民國轉西元；空值回傳 None（供參數化使用）"""
    if date_str is None:
        return None
    s = str(date_str).replace(" ", "").replace("　", "")
    if len(s) < 3:
        return None
    try:
        year = int(s[:3]) + 1911
        return f"{year:04d}-{s[3:5]}-{s[5:7]}"
    except Exception:
        return None

def clean_text(v):
    if v is None:
        return None
    s = str(v).replace("\u3000", " ").strip()
    return s if s != "" else None

# -------------------- 下載 & 解壓 --------------------
def gcis_Info_to_csv(download_dir: str = "."):
    """下載官方 zip 並解壓出 BGMOPEN1.csv"""
    zip_path = os.path.join(download_dir, DAILY_ZIP_NAME)
    resp = SESSION.get(DAILY_ZIP_URL, timeout=REQ_TIMEOUT, stream=True)
    resp.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(download_dir)
    logging.info("download web_zip to csv ok")

# -------------------- 匯入 Tmp_TaxInfo --------------------
def gcis_Info_csv_to_db(mydb, csv_path: str = DAILY_CSV_NAME):
    """
    來源 CSV 欄位對應：
      0 Party_Addr, 1 Party_ID, 2 Parent_Party_ID, 3 Party_Name, 4 PaidIn_Capital,
      5 Setup_Date(民國), 6 Party_Type, 7 Use_Invoice,
      8 Ind_Code, 9 Ind_Name, 10 Ind_Code1, 11 Ind_Name1,
      12 Ind_Code2(只保留數字), 13 Ind_Name2, 14 Ind_Code3, 15 Ind_Name3
    """
    # 先清空暫存表（資料量大時）
    with open(csv_path, newline="", encoding="utf-8") as csvfile:
        rows = list(csv.reader(csvfile))
    rows_num = len(rows)

    with mydb.cursor() as cursor:
        if rows_num >= 1000:
            cursor.execute("TRUNCATE TABLE crawlerdb.Tmp_TaxInfo")
            mydb.commit()

        insert_sql = """
            INSERT INTO crawlerdb.Tmp_TaxInfo
            (Party_Addr, Party_ID, Parent_Party_ID, Party_Name, PaidIn_Capital,
             Setup_Date, Party_Type, Use_Invoice,
             Ind_Code, Ind_Name, Ind_Code1, Ind_Name1,
             Ind_Code2, Ind_Name2, Ind_Code3, Ind_Name3)
            VALUES
            (%s, %s, %s, %s, %s,
             %s, %s, %s,
             %s, %s, %s, %s,
             %s, %s, %s, %s)
        """

        batch = []
        for x in trange(2, rows_num):  # 跳過前兩行標題
            r = rows[x]
            # 去空白/全形空白
            r = [clean_text(col) for col in r]

            # 日期轉換
            setup_date = date_check_insql(r[5])

            # Ind_Code2 僅保留數字
            ind_code2 = re.sub(r"\D+", "", r[12] or "") or None

            vals = (
                r[0],  # Party_Addr
                r[1],  # Party_ID
                r[2],  # Parent_Party_ID
                r[3],  # Party_Name
                int(r[4]) if (r[4] and r[4].isdigit()) else None,  # PaidIn_Capital
                setup_date,  # Setup_Date
                r[6],  # Party_Type
                r[7],  # Use_Invoice
                r[8],  # Ind_Code
                r[9],  # Ind_Name
                r[10], # Ind_Code1
                r[11], # Ind_Name1
                ind_code2,  # Ind_Code2
                r[13],     # Ind_Name2
                r[14],     # Ind_Code3
                r[15],     # Ind_Name3
            )
            batch.append(vals)

            # 批次寫入（避免一次性巨大 SQL）
            if len(batch) >= 1000:
                cursor.executemany(insert_sql, batch)
                mydb.commit()
                batch = []

        if batch:
            cursor.executemany(insert_sql, batch)
            mydb.commit()

# -------------------- 異動寫入 TaxInfo / 新增 TaxRecord --------------------
def new_taxrc(mydb):
    cursor = mydb.cursor()

    # 本日異動數（以當前 Tmp_TaxInfo 對比 TaxInfo 最新一版）
    sel_sql = """
        SELECT COUNT(*)
        FROM (
            SELECT Party_ID, Party_Addr, Parent_Party_ID, Party_Name, PaidIn_Capital, Setup_Date,
                   Party_Type, Use_Invoice, Ind_Code, Ind_Name, Ind_Code1, Ind_Name1,
                   Ind_Code2, Ind_Name2, Ind_Code3, Ind_Name3,
                   ROW_NUMBER() OVER (PARTITION BY Party_ID ORDER BY Update_Time DESC) AS RowNum
            FROM crawlerdb.TaxInfo
        ) RankedData
        LEFT JOIN crawlerdb.Tmp_TaxInfo a ON a.Party_ID = RankedData.Party_ID
        WHERE RankedData.RowNum = 1 AND (
            a.Party_Addr <> RankedData.Party_Addr OR
            a.Parent_Party_ID <> RankedData.Parent_Party_ID OR
            a.Party_Name <> RankedData.Party_Name OR
            a.PaidIn_Capital <> RankedData.PaidIn_Capital OR
            a.Setup_Date <> RankedData.Setup_Date OR
            a.Party_Type <> RankedData.Party_Type OR
            a.Use_Invoice <> RankedData.Use_Invoice OR
            a.Ind_Code <> RankedData.Ind_Code OR
            a.Ind_Name <> RankedData.Ind_Name OR
            a.Ind_Code1 <> RankedData.Ind_Code1 OR
            a.Ind_Name1 <> RankedData.Ind_Name1 OR
            a.Ind_Code2 <> RankedData.Ind_Code2 OR
            a.Ind_Name2 <> RankedData.Ind_Name2 OR
            a.Ind_Code3 <> RankedData.Ind_Code3 OR
            a.Ind_Name3 <> RankedData.Ind_Name3
        ) OR RankedData.Party_ID IS NULL
    """
    cursor.execute(sel_sql)
    row = cursor.fetchone()
    print(f"稅籍資料本日異動數: {row[0]} ")

    # 寫入新的/異動的最新資料到 TaxInfo
    df_sql = """
        INSERT INTO crawlerdb.TaxInfo (
            Party_ID, Party_Addr, Parent_Party_ID, Party_Name, PaidIn_Capital, Setup_Date,
            Party_Type, Use_Invoice, Ind_Code, Ind_Name, Ind_Code1, Ind_Name1,
            Ind_Code2, Ind_Name2, Ind_Code3, Ind_Name3
        )
        SELECT
            a.Party_ID, a.Party_Addr, a.Parent_Party_ID, a.Party_Name, a.PaidIn_Capital,
            a.Setup_Date, a.Party_Type, a.Use_Invoice, a.Ind_Code, a.Ind_Name,
            a.Ind_Code1, a.Ind_Name1, a.Ind_Code2, a.Ind_Name2, a.Ind_Code3, a.Ind_Name3
        FROM (
            SELECT Party_ID, Party_Addr, Parent_Party_ID, Party_Name, PaidIn_Capital, Setup_Date,
                   Party_Type, Use_Invoice, Ind_Code, Ind_Name, Ind_Code1, Ind_Name1,
                   Ind_Code2, Ind_Name2, Ind_Code3, Ind_Name3,
                   ROW_NUMBER() OVER (PARTITION BY Party_ID ORDER BY Update_Time DESC) AS RowNum
            FROM crawlerdb.TaxInfo
        ) RankedData
        LEFT JOIN crawlerdb.Tmp_TaxInfo a ON a.Party_ID = RankedData.Party_ID
        WHERE RankedData.RowNum = 1 AND (
            a.Party_Addr <> RankedData.Party_Addr OR
            a.Parent_Party_ID <> RankedData.Parent_Party_ID OR
            a.Party_Name <> RankedData.Party_Name OR
            a.PaidIn_Capital <> RankedData.PaidIn_Capital OR
            a.Setup_Date <> RankedData.Setup_ Date OR
            a.Party_Type <> RankedData.Party_Type OR
            a.Use_Invoice <> RankedData.Use_Invoice OR
            a.Ind_Code <> RankedData.Ind_Code OR
            a.Ind_Name <> RankedData.Ind_Name OR
            a.Ind_Code1 <> RankedData.Ind_Code1 OR
            a.Ind_Name1 <> RankedData.Ind_Name1 OR
            a.Ind_Code2 <> RankedData.Ind_Code2 OR
            a.Ind_Name2 <> RankedData.Ind_Name2 OR
            a.Ind_Code3 <> RankedData.Ind_Code3 OR
            a.Ind_Name3 <> RankedData.Ind_Name3
        )
        OR RankedData.Party_ID IS NULL
    """
    # 修正上面一個 typo（Setup_ Date）—以參數化替代難；直接再跑一次正確 SQL
    df_sql = df_sql.replace("Setup_ Date", "Setup_Date")
    cursor.execute(df_sql)
    mydb.commit()

    # TaxRecord 新增缺漏的 Party_ID
    rcs_sql = """
        SELECT COUNT(*)
        FROM crawlerdb.TaxInfo AS TI
        WHERE NOT EXISTS (
            SELECT 1 FROM crawlerdb.TaxRecord AS TR
            WHERE TR.Party_ID = TI.Party_ID
        )
    """
    cursor.execute(rcs_sql)
    row = cursor.fetchone()
    print(f"爬蟲狀態表本日ID新增數: {row[0]} ")

    rci_sql = """
        INSERT INTO crawlerdb.TaxRecord (Party_ID, Insert_Time)
        SELECT Party_ID, NOW()
        FROM crawlerdb.TaxInfo AS TI
        WHERE NOT EXISTS (
            SELECT 1 FROM crawlerdb.TaxRecord AS TR
            WHERE TR.Party_ID = TI.Party_ID
        )
    """
    cursor.execute(rci_sql)
    mydb.commit()

# -------------------- GCIS API（全 https + timeout） --------------------
GCIS_BASE = "https://data.gcis.nat.gov.tw/od/data/api"

def _gcis_get(resource_id: str, flt: str):
    url = f"{GCIS_BASE}/{resource_id}"
    params = {"$format": "json", "$filter": flt, "$skip": 0, "$top": 50}
    r = SESSION.get(url, params=params, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.text

def get_c1(cid):  # 基本資料
    return _gcis_get("5F64D864-61CB-4D0D-8AD9-492047CC1EA6", f"Business_Accounting_NO eq {assert_tax_id(cid)}")

def get_c3(cid):  # 營業項目
    return _gcis_get("236EE382-4942-41A9-BD03-CA0709025E7C", f"Business_Accounting_NO eq {assert_tax_id(cid)}")

def get_ci(item_code):  # 營業項目對照（原來用 http，已改 https）
    item_code = str(item_code).strip().upper()
    return _gcis_get("FCB90AB1-E382-45CE-8D4F-394861851E28", f"Business_Item eq {item_code}")

def get_bc(cid):  # 公司/分公司/商業 類型
    return _gcis_get("673F0FC0-B3A7-429F-9041-E9866836B66D", f"No eq {assert_tax_id(cid)}")

def get_ds(cid):  # 董監事
    return _gcis_get("4E5F7653-1B91-4DDC-99D5-468530FAE396", f"Business_Accounting_NO eq {assert_tax_id(cid)}")

# -------------------- API → DB（參數化） --------------------
def c1_to_db(mydb):
    rc_table = "TaxRecord"
    num = 100
    sleep_time = 0.5
    O_num = X_num = E_num = 0

    list_sql = f"""
        SELECT r.Party_ID
        FROM crawlerdb.TaxRecord i
        INNER JOIN crawlerdb.{rc_table} r ON i.Party_ID = r.Party_ID
        WHERE r.COM_1_TYPE IS NULL
        LIMIT %s
    """
    with mydb.cursor() as cursor:
        cursor.execute(list_sql, (num,))
        rows = cursor.fetchall()

        for (party_id,) in trange(0, len(rows), desc="C1"):
            party_id = rows[party_id][0] if isinstance(rows[party_id], (list, tuple)) else rows[party_id]
            time.sleep(sleep_time)
            c1_js = get_c1(party_id)
            if not c1_js:
                cursor.execute(f"UPDATE crawlerdb.{rc_table} SET COM_1_TYPE='Y', COM_1_Update_Time=NOW() WHERE Party_ID=%s", (party_id,))
                mydb.commit()
                X_num += 1
                continue

            try:
                obj = json.loads(c1_js[1:-1])
                vals = {
                    "Party_ID": obj.get("Business_Accounting_NO"),
                    "Party_Name": clean_text(obj.get("Company_Name")),
                    "Party_Status": clean_text(obj.get("Company_Status_Desc")),
                    "Reg_Capital": obj.get("Capital_Stock_Amount"),
                    "PaidIn_Capital": obj.get("Paid_In_Capital_Amount") or 0,
                    "Res_Name": clean_text(obj.get("Responsible_Name")),
                    "Party_Addr": clean_text(obj.get("Company_Location")),
                    "Reg_Org": clean_text(obj.get("Register_Organization_Desc")),
                    "Setup_Date": date_check_insql(obj.get("Company_Setup_Date")),
                    "Last_App_Data": date_check_insql(obj.get("Change_Of_Approval_Data")),
                    "Revoke_Date": date_check_insql(obj.get("Revoke_App_Date")),
                    "Case_Status": clean_text(obj.get("Case_Status")),
                    "Case_Desc": clean_text(obj.get("Case_Status_Desc")),
                    "Sus_App_Date": date_check_insql(obj.get("Sus_App_Date")),
                    "Sus_Beg_Date": date_check_insql(obj.get("Sus_Beg_Date")),
                    "Sus_End_Date": date_check_insql(obj.get("Sus_End_Date")),
                }

                ins_sql = """
                    INSERT INTO crawlerdb.CompanyStatus
                    (Party_ID, Party_Name, Party_Status, Reg_Capital, PaidIn_Capital, Res_Name,
                     Party_Addr, Reg_Org, Setup_Date, Last_App_Data, Revoke_Date, Case_Status,
                     Case_Desc, Sus_App_Date, Sus_Beg_Date, Sus_End_Date)
                    VALUES (%(Party_ID)s, %(Party_Name)s, %(Party_Status)s, %(Reg_Capital)s, %(PaidIn_Capital)s, %(Res_Name)s,
                            %(Party_Addr)s, %(Reg_Org)s, %(Setup_Date)s, %(Last_App_Data)s, %(Revoke_Date)s, %(Case_Status)s,
                            %(Case_Desc)s, %(Sus_App_Date)s, %(Sus_Beg_Date)s, %(Sus_End_Date)s)
                """
                cursor.execute(ins_sql, vals)
                cursor.execute(f"UPDATE crawlerdb.{rc_table} SET COM_1_TYPE='Y', COM_1_Update_Time=NOW() WHERE Party_ID=%s", (vals["Party_ID"],))
                mydb.commit()
                O_num += 1

            except Exception as e:
                logging.error(f"C1 error {party_id}: {e}")
                E_num += 1

    print(f"C1_fun = O: {O_num}, X: {X_num}, E: {E_num}")

def c3_to_db(mydb):
    rc_table = "TaxRecord"
    num = 100
    O_num = X_num = E_num = T_num = 0

    list_sql = f"""
        SELECT DISTINCT t.Party_ID
        FROM crawlerdb.TaxRecord t
        LEFT JOIN crawlerdb.CompanyItems c ON c.Party_ID = t.Party_ID
        WHERE c.Business_Item IS NULL AND t.COM_1_TYPE = 'Y'
        LIMIT %s
    """

    with mydb.cursor() as cursor:
        cursor.execute(list_sql, (num,))
        rows = cursor.fetchall()

        for (party_id,) in trange(0, len(rows), desc="C3"):
            party_id = rows[party_id][0] if isinstance(rows[party_id], (list, tuple)) else rows[party_id]
            time.sleep(0.1)
            c3_js = get_c3(party_id)
            if not c3_js:
                cursor.execute(f"UPDATE crawlerdb.{rc_table} SET COM_3_TYPE='N', COM_3_Update_Time=NOW() WHERE Party_ID=%s", (party_id,))
                mydb.commit()
                X_num += 1
                continue

            try:
                data = json.loads(c3_js)
                values = []
                for entry in data:
                    pid = entry.get("Business_Accounting_NO", "")
                    for b in entry.get("Cmp_Business", []) or []:
                        item = (pid, (b.get("Business_Item") or "").strip())
                        if item[1]:
                            values.append(item)

                if values:
                    cursor.executemany(
                        "INSERT INTO crawlerdb.CompanyItems (Party_ID, Business_Item) VALUES (%s, %s)",
                        values
                    )
                    cursor.execute(f"UPDATE crawlerdb.{rc_table} SET COM_3_TYPE='Y', COM_3_Update_Time=NOW() WHERE Party_ID=%s", (pid,))
                    mydb.commit()
                    O_num += 1
                else:
                    cursor.execute(f"UPDATE crawlerdb.{rc_table} SET COM_3_TYPE='N', COM_3_Update_Time=NOW() WHERE Party_ID=%s", (party_id,))
                    mydb.commit()
                    X_num += 1

            except Exception as e:
                logging.error(f"C3 error {party_id}: {e}")
                E_num += 1

    print(f"C3_fun = O: {O_num}, X: {X_num}, E: {E_num}, T:{T_num}")

def bizcom_type_to_db(mydb):
    sleep_time = 0.3
    with mydb.cursor() as cursor:
        cursor.execute("""
            SELECT Party_ID
            FROM crawlerdb.TaxRecord
            WHERE BIZ_TYPE IS NULL OR COM_TYPE IS NULL OR BNH_TYPE IS NULL
        """)
        rows = cursor.fetchall()

        for (party_id,) in trange(0, len(rows), desc="BC"):
            party_id = rows[party_id][0] if isinstance(rows[party_id], (list, tuple)) else rows[party_id]
            time.sleep(sleep_time)
            js = get_bc(party_id)
            try:
                obj = json.loads(js)
                types = [e.get("TYPE") for e in obj if e.get("exist") == "Y"]
                vals = {
                    "COM": "Y" if "公司" in types else "N",
                    "BNH": "Y" if "分公司" in types else "N",
                    "BIZ": "Y" if "商業" in types else "N",
                    "PID": party_id
                }
                cursor.execute("""
                    UPDATE crawlerdb.TaxRecord
                    SET COM_TYPE=%s, BNH_TYPE=%s, BIZ_TYPE=%s
                    WHERE Party_ID=%s
                """, (vals["COM"], vals["BNH"], vals["BIZ"], vals["PID"]))
                mydb.commit()
            except Exception as e:
                logging.error(f"BC error {party_id}: {e}")

def ds_to_db(mydb):
    rc_table = "TaxRecord"
    num = 10000
    O_num = X_num = E_num = 0

    list_sql = """
        SELECT Party_ID
        FROM crawlerdb.TaxRecord
        WHERE COM_D_TYPE IS NULL AND COM_TYPE='Y'
        LIMIT %s
    """
    with mydb.cursor() as cursor:
        cursor.execute(list_sql, (num,))
        rows = cursor.fetchall()

        for (party_id,) in trange(0, len(rows), desc="DS"):
            party_id = rows[party_id][0] if isinstance(rows[party_id], (list, tuple)) else rows[party_id]
            time.sleep(0.1)
            ds_js = get_ds(party_id)
            if not ds_js:
                continue

            try:
                data = json.loads(ds_js)
                values = []
                for entry in data:
                    values.append((
                        party_id,
                        clean_text(entry.get("Person_Position_Name")),
                        clean_text(entry.get("Person_Name")),
                        clean_text(entry.get("Juristic_Person_Name")),
                        entry.get("Person_Shareholding"),
                    ))

                if values:
                    cursor.executemany("""
                        INSERT INTO crawlerdb.CompanySharesheld
                        (Party_ID, Person_Position_Name, Person_Name, Juristic_Person_Name, Person_Shareholding, Update_Time)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                    """, values)

                cursor.execute(f"UPDATE crawlerdb.{rc_table} SET COM_D_TYPE='Y', COM_D_Update_Time=NOW() WHERE Party_ID=%s", (party_id,))
                mydb.commit()
                O_num += 1
            except Exception as e:
                logging.error(f"DS error {party_id}: {e}")
                E_num += 1

    print(f"DS_fun = O: {O_num}, X: {X_num}, E: {E_num}")

# -------------------- 主流程 --------------------
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()]
    )
    print("START Time :", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    mydb = db_connect()

    # 1) 下載並解壓
    gcis_Info_to_csv()
    # 2) 匯入暫存表
    gcis_Info_csv_to_db(mydb, DAILY_CSV_NAME)
    # 3) 差異入庫 + TaxRecord 新增
    new_taxrc(mydb)

    # 視需要再執行以下批次（預設關閉）：
    # c1_to_db(mydb)
    # c3_to_db(mydb)
    # bizcom_type_to_db(mydb)
    # ds_to_db(mydb)

    mydb.close()
    print("End Time :", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

if __name__ == "__main__":
    main()
