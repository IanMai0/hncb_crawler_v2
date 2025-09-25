# -*- coding: utf-8 -*-
"""
Spyder Editor
This is a data quality check system
input columns check logic
output columns count num in csv

定義
總表=gcis_Info，由 TaxInfo
"""

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

import warnings
warnings.filterwarnings("ignore", category=UserWarning)

import os
import csv
import json
import time
import urllib
import requests
import pymysql
import pandas as pd

from zipfile import ZipFile
from pandas import DataFrame
from datetime import datetime
from time import sleep
from tqdm import trange

# =========================
# 資料庫連線
# =========================
mydb = pymysql.connect(
    host="X.amazonaws.com",
    user="X",
    password="X",
    database="X",
    charset="X",
    cursorclass=pymysql.cursors.Cursor,
    autocommit=False
)
mydbcursor = mydb.cursor()

# =========================
# HTTP Session（timeout + 簡易重試）
# =========================
HTTP_TIMEOUT = 15
MAX_RETRY = 3
SLEEP_BETWEEN_RETRY = 0.8

session = requests.Session()
session.headers.update({
    "User-Agent": "HNBC-Crawler/1.0 (+https://example.com)"
})

def http_get_text(url: str, params=None):
    for attempt in range(1, MAX_RETRY + 1):
        try:
            r = session.get(url, params=params, timeout=HTTP_TIMEOUT)
            if r.ok:
                return r.text
            else:
                print(f"[HTTP] {url} status={r.status_code}, attempt={attempt}")
        except requests.RequestException as e:
            print(f"[HTTP] {url} error={e}, attempt={attempt}")
        time.sleep(SLEEP_BETWEEN_RETRY)
    return ""

# =========================
# 小工具
# =========================
# 小工具_把 SQL 查詢結果轉成 Pandas DataFrame
def msdb_to_df(mydb_conn, sql):
    cursor = mydb_conn.cursor()
    cursor.execute(sql)
    df = DataFrame(cursor.fetchall())
    return df

# 小工具_民國轉西元
def _tw_roc_to_iso(s):
    """民國 yyyMMdd -> 西元 yyyy-MM-dd；空字串或格式不對回傳 None"""
    if s is None:
        return None
    s = str(s).strip().replace('　', '')
    if not s:
        return None
    if len(s) >= 7 and s[:3].isdigit():
        y = int(s[:3]) + 1911
        mm = s[3:5]
        dd = s[5:7]
        return f"{y:04d}-{mm}-{dd}"
    return None

# ETL 小工具
def _to_int_or_none(v):
    v = (v or "").strip()
    return int(v) if v.isdigit() else None

def count_csv_rows(path='BGMOPEN1.csv'):
    if not os.path.exists(path):
        print(f"CSV 檔案不存在: {path}")
        return 0, 0
    with open(path, newline='', encoding='utf-8') as f:
        r = csv.reader(f)
        rows = list(r)
    header = 1 if rows else 0
    total = len(rows)
    data_rows = max(0, total - header)
    print(f"CSV 總列數(含表頭): {total}")
    print(f"CSV 資料列數(不含表頭): {data_rows}")
    return total, data_rows

# =========================
# 下載與展開 CSV
# =========================
def gcis_Info_to_csv():
    url = "https://eip.fia.gov.tw/data/BGMOPEN1.zip"  # 這不知道是什麼東東？？？
    local_zip = "gcis.zip"
    urllib.request.urlretrieve(url, local_zip)
    with ZipFile(local_zip, "r") as z:
        z.extractall()
    print("download web_zip to csv ok")

# =========================
# CSV -> 暫存表（參數化、批次）
# =========================
def gcis_Info_csv_to_db(csv_path='BGMOPEN1.csv'):
    with open(csv_path, newline='', encoding="utf-8") as csvfile:
        rows = list(csv.reader(csvfile))

    # 先清空暫存表
    mydbcursor.execute("TRUNCATE TABLE crawlerdb.Tmp_TaxInfo")
    mydb.commit()

    if not rows:
        print("CSV 內容為空，已清空 Tmp_TaxInfo。")
        return

    # 準備批次資料（跳第一列表頭）
    batch = []
    for idx, r in enumerate(rows):
        if idx == 0:
            continue
        # 防全形空白 & 去除一般空白
        r = [(r[i] or '').replace(' ', '').replace('\u3000', '') for i in range(len(r))]

        party_addr  = r[0] or None
        party_id    = r[1] or None
        parent_id   = r[2] or None
        party_name  = r[3] or None
        paidin      = _to_int_or_none(r[4])
        setup_date  = _tw_roc_to_iso(r[5])
        party_type  = r[6] or None
        use_invoice = r[7] or None
        ind_code    = r[8] or None
        ind_name    = r[9] or None
        ind_code1   = r[10] or None
        ind_name1   = r[11] or None
        # 只保留數字
        ind_code2   = ''.join(c for c in r[12] if c.isdigit()) or None
        ind_name2   = r[13] or None
        ind_code3   = r[14] or None
        ind_name3   = r[15] or None

        batch.append((
            party_addr, party_id, parent_id, party_name, paidin, setup_date,
            party_type, use_invoice, ind_code, ind_name, ind_code1, ind_name1,
            ind_code2, ind_name2, ind_code3, ind_name3
        ))

    insert_sql = """
        INSERT INTO crawlerdb.Tmp_TaxInfo
        (Party_Addr, Party_ID, Parent_Party_ID, Party_Name, PaidIn_Capital, Setup_Date,
         Party_Type, Use_Invoice, Ind_Code, Ind_Name, Ind_Code1, Ind_Name1,
         Ind_Code2, Ind_Name2, Ind_Code3, Ind_Name3)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    mydbcursor.executemany(insert_sql, batch)
    mydb.commit()
    print(f"寫入暫存表 {len(batch)} 筆")

# =========================
# 比對異動 & 寫入 TaxInfo（以暫存為主）
# =========================
def new_taxrc():
    cursor = mydb.cursor()

    # 1) 計數：Tmp_TaxInfo 對 TaxInfo 最新一筆的差異（含新統編）
    count_sql = """
    SELECT COUNT(*)
    FROM crawlerdb.Tmp_TaxInfo a
    LEFT JOIN crawlerdb.TaxInfo t
      ON t.Party_ID = a.Party_ID
    LEFT JOIN crawlerdb.TaxInfo t2
      ON t2.Party_ID = t.Party_ID AND t2.Update_Time > t.Update_Time
    WHERE
      t2.Party_ID IS NULL  -- t 就是該 Party_ID 的最新一筆；若 t 為 NULL 代表新統編
      AND (
        t.Party_ID IS NULL OR
        NOT (
          a.Party_Addr       <=> t.Party_Addr AND
          a.Parent_Party_ID  <=> t.Parent_Party_ID AND
          a.Party_Name       <=> t.Party_Name AND
          a.PaidIn_Capital   <=> t.PaidIn_Capital AND
          a.Setup_Date       <=> t.Setup_Date AND
          a.Party_Type       <=> t.Party_Type AND
          a.Use_Invoice      <=> t.Use_Invoice AND
          a.Ind_Code         <=> t.Ind_Code AND
          a.Ind_Name         <=> t.Ind_Name AND
          a.Ind_Code1        <=> t.Ind_Code1 AND
          a.Ind_Name1        <=> t.Ind_Name1 AND
          a.Ind_Code2        <=> t.Ind_Code2 AND
          a.Ind_Name2        <=> t.Ind_Name2 AND
          a.Ind_Code3        <=> t.Ind_Code3 AND
          a.Ind_Name3        <=> t.Ind_Name3
        )
      );
    """
    cursor.execute(count_sql)
    cnt = cursor.fetchone()[0]
    print(f"稅籍資料本日異動數(含新統編與異動): {cnt}")

    # 2) 寫入差異到 TaxInfo（新統編 + 異動）
    insert_sql = """
    INSERT INTO crawlerdb.TaxInfo (
      Party_ID, Party_Addr, Parent_Party_ID, Party_Name, PaidIn_Capital, Setup_Date,
      Party_Type, Use_Invoice, Ind_Code, Ind_Name, Ind_Code1, Ind_Name1, Ind_Code2,
      Ind_Name2, Ind_Code3, Ind_Name3
    )
    SELECT
      a.Party_ID, a.Party_Addr, a.Parent_Party_ID, a.Party_Name, a.PaidIn_Capital, a.Setup_Date,
      a.Party_Type, a.Use_Invoice, a.Ind_Code, a.Ind_Name, a.Ind_Code1, a.Ind_Name1, a.Ind_Code2,
      a.Ind_Name2, a.Ind_Code3, a.Ind_Name3
    FROM crawlerdb.Tmp_TaxInfo a
    LEFT JOIN crawlerdb.TaxInfo t
      ON t.Party_ID = a.Party_ID
    LEFT JOIN crawlerdb.TaxInfo t2
      ON t2.Party_ID = t.Party_ID AND t2.Update_Time > t.Update_Time
    WHERE
      t2.Party_ID IS NULL
      AND (
        t.Party_ID IS NULL OR
        NOT (
          a.Party_Addr       <=> t.Party_Addr AND
          a.Parent_Party_ID  <=> t.Parent_Party_ID AND
          a.Party_Name       <=> t.Party_Name AND
          a.PaidIn_Capital   <=> t.PaidIn_Capital AND
          a.Setup_Date       <=> t.Setup_Date AND
          a.Party_Type       <=> t.Party_Type AND
          a.Use_Invoice      <=> t.Use_Invoice AND
          a.Ind_Code         <=> t.Ind_Code AND
          a.Ind_Name         <=> t.Ind_Name AND
          a.Ind_Code1        <=> t.Ind_Code1 AND
          a.Ind_Name1        <=> t.Ind_Name1 AND
          a.Ind_Code2        <=> t.Ind_Code2 AND
          a.Ind_Name2        <=> t.Ind_Name2 AND
          a.Ind_Code3        <=> t.Ind_Code3 AND
          a.Ind_Name3        <=> t.Ind_Name3
        )
      );
    """
    cursor.execute(insert_sql)
    mydb.commit()

    # 3) 把新出現的 Party_ID 補進 TaxRecord
    rcs_sql = """
      SELECT COUNT(*)
      FROM crawlerdb.TaxInfo TI
      WHERE NOT EXISTS (
        SELECT 1 FROM crawlerdb.TaxRecord TR WHERE TR.Party_ID = TI.Party_ID
      )
    """
    cursor.execute(rcs_sql)
    row = cursor.fetchone()
    print(f"爬蟲狀態表本日ID新增數(預估): {row[0]} ")

    rci_sql = """
      INSERT INTO crawlerdb.TaxRecord (Party_ID, Insert_Time)
      SELECT Party_ID, NOW()
      FROM crawlerdb.TaxInfo TI
      WHERE NOT EXISTS (
        SELECT 1 FROM crawlerdb.TaxRecord TR WHERE TR.Party_ID = TI.Party_ID
      )
    """
    cursor.execute(rci_sql)
    mydb.commit()


# =========================
# GCIS API 封裝（全部 https + timeout）
# =========================
def get_c1(cid):
    url = "https://data.gcis.nat.gov.tw/od/data/api/5F64D864-61CB-4D0D-8AD9-492047CC1EA6"
    params = {"$format":"json", "$filter": f"Business_Accounting_NO eq {cid}", "$skip": 0, "$top": 50}
    return http_get_text(url, params=params)

def get_c3(cid):
    url = "https://data.gcis.nat.gov.tw/od/data/api/236EE382-4942-41A9-BD03-CA0709025E7C"
    params = {"$format":"json", "$filter": f"Business_Accounting_NO eq {cid}", "$skip": 0, "$top": 50}
    return http_get_text(url, params=params)

def get_ci(item_code):
    url = "https://data.gcis.nat.gov.tw/od/data/api/FCB90AB1-E382-45CE-8D4F-394861851E28"
    params = {"$format":"json", "$filter": f"Business_Item eq {item_code}", "$skip": 0, "$top": 50}
    return http_get_text(url, params=params)

def get_bc(cid):
    url = "https://data.gcis.nat.gov.tw/od/data/api/673F0FC0-B3A7-429F-9041-E9866836B66D"
    params = {"$format":"json", "$filter": f"No eq {cid}", "$skip": 0, "$top": 50}
    return http_get_text(url, params=params)

def get_ds(cid):
    url = "https://data.gcis.nat.gov.tw/od/data/api/4E5F7653-1B91-4DDC-99D5-468530FAE396"
    params = {"$format":"json", "$filter": f"Business_Accounting_NO eq {cid}", "$skip": 0, "$top": 50}
    return http_get_text(url, params=params)

# =========================
# C1: 公司登記基本資料（應用一）
# =========================
def c1_to_db(num=100, sleep_time=0.5):
    rc_tablb = 'TaxRecord'
    cursor = mydb.cursor()

    list_sql = f"""
        SELECT r.Party_ID
        FROM crawlerdb.{rc_tablb} r
        WHERE r.COM_1_TYPE IS NULL
        LIMIT {num};
    """
    cursor.execute(list_sql)
    rows = cursor.fetchall()

    O_num = X_num = E_num = 0
    al_num = len(rows)

    for i in trange(al_num):
        party_id = rows[i][0]
        time.sleep(sleep_time)

        c1_txt = get_c1(party_id)
        if not c1_txt:
            # 無資料或請求失敗 -> 標 N
            try:
                cursor.execute(f"UPDATE crawlerdb.{rc_tablb} SET COM_1_TYPE='N', COM_1_Update_Time=NOW() WHERE Party_ID=%s", (party_id,))
                mydb.commit()
                X_num += 1
            except Exception as e:
                print(f"Error {party_id} : {e}")
                E_num += 1
            continue

        try:
            data = json.loads(c1_txt)
            if isinstance(data, list) and data:
                obj = data[0]
            elif isinstance(data, dict):
                obj = data
            else:
                obj = None
        except Exception as e:
            print(f"JSON parse error {party_id}: {e}")
            obj = None

        if not obj:
            try:
                cursor.execute(f"UPDATE crawlerdb.{rc_tablb} SET COM_1_TYPE='N', COM_1_Update_Time=NOW() WHERE Party_ID=%s", (party_id,))
                mydb.commit()
                X_num += 1
            except Exception as e:
                print(f"Error {party_id} : {e}")
                E_num += 1
            continue

        # 取欄位
        Party_ID = obj.get("Business_Accounting_NO") or party_id
        Party_Name = obj.get("Company_Name")
        Party_Status = obj.get("Company_Status_Desc")
        Reg_Capital = obj.get("Capital_Stock_Amount")
        PaidIn_Capital = obj.get("Paid_In_Capital_Amount") or 0
        Res_Name = obj.get("Responsible_Name") or ""
        Party_Addr = obj.get("Company_Location")
        Reg_Org = obj.get("Register_Organization_Desc")

        Setup_Date     = _tw_roc_to_iso(obj.get("Company_Setup_Date"))
        Last_App_Data  = _tw_roc_to_iso(obj.get("Change_Of_Approval_Data"))
        Revoke_Date    = _tw_roc_to_iso(obj.get("Revoke_App_Date"))
        Case_Status    = (obj.get("Case_Status") or "").strip() or None
        Case_Desc_raw  = (obj.get("Case_Status_Desc") or "").strip()
        Case_Desc      = Case_Desc_raw if Case_Desc_raw else None
        Sus_App_Date   = _tw_roc_to_iso(obj.get("Sus_App_Date"))
        Sus_Beg_Date   = _tw_roc_to_iso(obj.get("Sus_Beg_Date"))
        Sus_End_Date   = _tw_roc_to_iso(obj.get("Sus_End_Date"))

        try:
            insert_sql = """
                INSERT INTO crawlerdb.CompanyStatus
                (Party_ID, Party_Name, Party_Status, Reg_Capital, PaidIn_Capital, Res_Name, Party_Addr, Reg_Org,
                 Setup_Date, Last_App_Data, Revoke_Date, Case_Status, Case_Desc, Sus_App_Date, Sus_Beg_Date, Sus_End_Date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            cursor.execute(insert_sql, (
                Party_ID, Party_Name, Party_Status, Reg_Capital, PaidIn_Capital, Res_Name, Party_Addr, Reg_Org,
                Setup_Date, Last_App_Data, Revoke_Date, Case_Status, Case_Desc, Sus_App_Date, Sus_Beg_Date, Sus_End_Date
            ))

            cursor.execute(f"UPDATE crawlerdb.{rc_tablb} SET COM_1_TYPE='Y', COM_1_Update_Time=NOW() WHERE Party_ID=%s", (Party_ID,))
            mydb.commit()
            O_num += 1
        except Exception as e:
            print(f"Error insert CompanyStatus {Party_ID}: {e}")
            mydb.rollback()
            E_num += 1

    print(f"C1_fun = O:{O_num}, X:{X_num}, E:{E_num}, ALL:{al_num}")

# =========================
# C3: 可營項目
# =========================
def c3_to_db(num=100):
    rc_tablb = 'TaxRecord'
    cursor = mydb.cursor()

    list3_sql = f"""
        SELECT DISTINCT t.Party_ID
        FROM crawlerdb.{rc_tablb} t
        LEFT JOIN crawlerdb.CompanyItems c ON c.Party_ID = t.Party_ID
        WHERE c.Business_Item IS NULL AND t.COM_1_TYPE='Y'
        LIMIT {num};
    """
    cursor.execute(list3_sql)
    rows = cursor.fetchall()

    O_num = X_num = E_num = T_num = 0
    al_num = len(rows)

    for i in trange(al_num):
        party_id = rows[i][0]
        time.sleep(0.1)

        c3_txt = get_c3(party_id)
        if not c3_txt:
            # API 無資料/失敗
            try:
                cursor.execute(f"UPDATE crawlerdb.{rc_tablb} SET COM_3_TYPE='N', COM_3_Update_Time=NOW() WHERE Party_ID=%s", (party_id,))
                mydb.commit()
                X_num += 1
            except Exception as e:
                print(f"Error mark COM_3 N {party_id}: {e}")
                E_num += 1
            continue

        try:
            data = json.loads(c3_txt)
        except Exception as e:
            print(f"C3 JSON parse error {party_id}: {e}")
            data = []

        insert_batch = []
        for entry in data:
            p_id = entry.get("Business_Accounting_NO")
            bizs = entry.get("Cmp_Business", []) or []
            for business in bizs:
                item = (business.get("Business_Item") or "").strip()
                if item:
                    insert_batch.append((p_id, item))

        if insert_batch:
            try:
                cursor.executemany(
                    "INSERT INTO crawlerdb.CompanyItems (Party_ID, Business_Item) VALUES (%s,%s)",
                    insert_batch
                )
                cursor.execute(f"UPDATE crawlerdb.{rc_tablb} SET COM_3_TYPE='Y', COM_3_Update_Time=NOW() WHERE Party_ID=%s", (party_id,))
                mydb.commit()
                O_num += 1
            except Exception as e:
                print(f"Error insert CompanyItems {party_id}: {e}")
                mydb.rollback()
                E_num += 1
        else:
            try:
                cursor.execute(f"UPDATE crawlerdb.{rc_tablb} SET COM_3_TYPE='N', COM_3_Update_Time=NOW() WHERE Party_ID=%s", (party_id,))
                mydb.commit()
                X_num += 1
            except Exception as e:
                print(f"Error mark COM_3 N {party_id}: {e}")
                E_num += 1

    print(f"C3_fun = O:{O_num}, X:{X_num}, E:{E_num}, T:{T_num}, ALL:{al_num}")

# =========================
# ItemsDescription（依 CompanyItems 補充說明）
# =========================
def item_to_db():
    cursor = mydb.cursor()
    list_sql = """
        SELECT DISTINCT CI.Business_Item
        FROM crawlerdb.CompanyItems CI
        LEFT JOIN crawlerdb.ItemsDescription ID ON CI.Business_Item = ID.Business_Item
        WHERE ID.Business_Item IS NULL
    """
    try:
        cursor.execute(list_sql)
        item_list = [row[0] for row in cursor.fetchall()]
        A_num = len(item_list)
        print('本日新類別:', A_num, '（提醒：商工部分子 API 可能有空值）')

        for item_code in trange(A_num):
            code = item_list[item_code]
            ci_txt = get_ci(code)
            time.sleep(1)

            if not ci_txt:
                continue

            try:
                data = json.loads(ci_txt)
            except Exception as e:
                print(f"CI JSON parse error {code}: {e}")
                continue

            for record in data:
                Category              = record.get('Category')
                Category_Name         = record.get('Category_Name')
                Classes               = record.get('Classes')
                Classes_Name          = record.get('Classes_Name')
                Subcategory           = record.get('Subcategory')
                Subcategories_Name    = record.get('Subcategories_Name')
                Business_Item         = record.get('Business_Item')
                Business_Item_Desc    = record.get('Business_Item_Desc')
                Business_Item_Content = record.get('Business_Item_Content')
                dgbas_raw             = record.get('Dgbas') or ''

                Dgbas = None
                Dgbas_Desc = None
                if '\t' in dgbas_raw:
                    parts = dgbas_raw.split('\t', 1)
                    Dgbas = parts[0]
                    Dgbas_Desc = parts[1]
                else:
                    # 有時候是空或無法分割
                    Dgbas = dgbas_raw or None

                try:
                    cursor.execute("""
                        INSERT INTO crawlerdb.ItemsDescription
                        (Category, Category_Name, Classes, Classes_Name, Subcategory, Subcategories_Name,
                         Business_Item, Business_Item_Desc, Business_Item_Content, Dgbas, Dgbas_Desc)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        Category, Category_Name, Classes, Classes_Name, Subcategory, Subcategories_Name,
                        Business_Item, Business_Item_Desc, Business_Item_Content, Dgbas, Dgbas_Desc
                    ))
                    mydb.commit()
                except Exception as e:
                    # 可能重複或其他錯誤
                    mydb.rollback()
                    print(f"Insert ItemsDescription error {Business_Item}: {e}")
    except Exception as e:
        print(f"本日無新類別或查詢失敗: {e}")

# =========================
# 公司/分公司/商業 類型旗標
# =========================
def bizcom_type_to_db():
    cursor = mydb.cursor()
    list_sql = """
        SELECT Party_ID
        FROM crawlerdb.TaxRecord
        WHERE BIZ_TYPE IS NULL OR COM_TYPE IS NULL OR BNH_TYPE IS NULL
    """
    cursor.execute(list_sql)
    rows = cursor.fetchall()

    for i in trange(len(rows)):
        pid = rows[i][0]
        time.sleep(0.3)
        bc_txt = get_bc(pid)
        try:
            arr = json.loads(bc_txt) if bc_txt else []
        except Exception as e:
            print(f"BC JSON parse error {pid}: {e}")
            arr = []

        types = [entry.get("TYPE") for entry in arr if entry.get("exist") == "Y"]
        is_com = 'Y' if '公司' in types else 'N'
        is_bnh = 'Y' if '分公司' in types else 'N'
        is_biz = 'Y' if '商業' in types else 'N'

        try:
            cursor.execute("""
                UPDATE crawlerdb.TaxRecord
                SET COM_TYPE=%s, BNH_TYPE=%s, BIZ_TYPE=%s
                WHERE Party_ID=%s
            """, (is_com, is_bnh, is_biz, pid))
            mydb.commit()
        except Exception as e:
            mydb.rollback()
            print(f"Update biz/com type error {pid}: {e}")

# =========================
# 董監/持股（CompanySharesheld）
# =========================
def ds_to_db(num=10000):
    rc_tablb = 'TaxRecord'
    cursor = mydb.cursor()

    list_sql = f"""
        SELECT Party_ID
        FROM crawlerdb.{rc_tablb}
        WHERE COM_D_TYPE IS NULL AND COM_TYPE='Y'
        LIMIT {num}
    """
    cursor.execute(list_sql)
    rows = cursor.fetchall()

    O_num = X_num = E_num = 0
    al_num = len(rows)

    for i in trange(al_num):
        pid = rows[i][0]
        time.sleep(0.1)
        ds_txt = get_ds(pid)
        try:
            data = json.loads(ds_txt) if ds_txt else []
        except Exception as e:
            print(f"DS JSON parse error {pid}: {e}")
            data = []

        inserted = 0
        for entry in data:
            position_name = entry.get('Person_Position_Name') or None
            person_name = entry.get('Person_Name') or None
            juristic_person_name = entry.get('Juristic_Person_Name') or None
            shareholding = entry.get('Person_Shareholding')
            shareholding = shareholding if shareholding is not None else 0

            try:
                cursor.execute("""
                    INSERT INTO crawlerdb.CompanySharesheld
                    (Party_ID, Person_Position_Name, Person_Name, Juristic_Person_Name, Person_Shareholding, Update_Time)
                    VALUES (%s,%s,%s,%s,%s,NOW())
                """, (pid, position_name, person_name, juristic_person_name, shareholding))
                inserted += 1
            except Exception as e:
                print(f"Insert sharesheld error {pid}: {e}")
                mydb.rollback()

        try:
            if inserted > 0:
                cursor.execute(f"UPDATE crawlerdb.{rc_tablb} SET COM_D_TYPE='Y', COM_D_Update_Time=NOW() WHERE Party_ID=%s", (pid,))
                O_num += 1
            else:
                cursor.execute(f"UPDATE crawlerdb.{rc_tablb} SET COM_D_TYPE='N', COM_D_Update_Time=NOW() WHERE Party_ID=%s", (pid,))
                X_num += 1
            mydb.commit()
        except Exception as e:
            mydb.rollback()
            print(f"Update COM_D_TYPE error {pid}: {e}")
            E_num += 1

    print(f"DS_fun = O:{O_num}, X:{X_num}, E:{E_num}, ALL:{al_num}")

# =========================
# 驗證：CSV vs 暫存 vs 異動 vs TaxInfo 今日落筆
# =========================
def verify_ingest():
    cursor = mydb.cursor()

    # 暫存表筆數
    cursor.execute("SELECT COUNT(*) FROM crawlerdb.Tmp_TaxInfo")
    tmp_rows = cursor.fetchone()[0]

    # TaxInfo 今日筆數
    cursor.execute("SELECT COUNT(*) FROM crawlerdb.TaxInfo WHERE DATE(Update_Time)=CURRENT_DATE")
    taxinfo_today = cursor.fetchone()[0]

    # 差異（新統編與異動統編各多少）
    diff_sql = """
    WITH latest AS (
      SELECT t.*
      FROM crawlerdb.TaxInfo t
      JOIN (
        SELECT Party_ID, MAX(Update_Time) AS max_ut
        FROM crawlerdb.TaxInfo
        GROUP BY Party_ID
      ) m ON m.Party_ID = t.Party_ID AND m.max_ut = t.Update_Time
    )
    SELECT
      SUM(CASE WHEN lt.Party_ID IS NULL THEN 1 ELSE 0 END) AS new_party_ids,
      SUM(CASE WHEN lt.Party_ID IS NOT NULL AND (
        NULLIF(a.Party_Addr,'')     <> NULLIF(lt.Party_Addr,'') OR
        NULLIF(a.Parent_Party_ID,'')<> NULLIF(lt.Parent_Party_ID,'') OR
        NULLIF(a.Party_Name,'')     <> NULLIF(lt.Party_Name,'') OR
        NULLIF(a.PaidIn_Capital,0)  <> NULLIF(lt.PaidIn_Capital,0) OR
        NULLIF(a.Setup_Date,NULL)   <> NULLIF(lt.Setup_Date,NULL) OR
        NULLIF(a.Party_Type,'')     <> NULLIF(lt.Party_Type,'') OR
        NULLIF(a.Use_Invoice,'')    <> NULLIF(lt.Use_Invoice,'') OR
        NULLIF(a.Ind_Code,'')       <> NULLIF(lt.Ind_Code,'') OR
        NULLIF(a.Ind_Name,'')       <> NULLIF(lt.Ind_Name,'') OR
        NULLIF(a.Ind_Code1,'')      <> NULLIF(lt.Ind_Code1,'') OR
        NULLIF(a.Ind_Name1,'')      <> NULLIF(lt.Ind_Name1,'') OR
        NULLIF(a.Ind_Code2,'')      <> NULLIF(lt.Ind_Code2,'') OR
        NULLIF(a.Ind_Name2,'')      <> NULLIF(lt.Ind_Name2,'') OR
        NULLIF(a.Ind_Code3,'')      <> NULLIF(lt.Ind_Code3,'') OR
        NULLIF(a.Ind_Name3,'')      <> NULLIF(lt.Ind_Name3,'')
      ) THEN 1 ELSE 0 END) AS changed_party_ids
    FROM crawlerdb.Tmp_TaxInfo a
    LEFT JOIN latest lt ON lt.Party_ID = a.Party_ID
    """
    cursor.execute(diff_sql)
    new_cnt, chg_cnt = cursor.fetchone()

    print(f"[核對] Tmp_TaxInfo 筆數：{tmp_rows}")
    print(f"[核對] TaxInfo 今日落筆：{taxinfo_today}")
    print(f"[核對] 今日 CSV 相對最新 TaxInfo 新統編：{new_cnt}、異動：{chg_cnt}")

    # 列示前 50 筆「CSV 有但 TaxInfo 沒有」的新統編
    sample_sql = """
    WITH latest AS (
      SELECT t.Party_ID
      FROM crawlerdb.TaxInfo t
      JOIN (
        SELECT Party_ID, MAX(Update_Time) AS max_ut
        FROM crawlerdb.TaxInfo GROUP BY Party_ID
      ) m ON m.Party_ID = t.Party_ID AND m.max_ut = t.Update_Time
    )
    SELECT a.Party_ID
    FROM crawlerdb.Tmp_TaxInfo a
    LEFT JOIN latest l ON l.Party_ID = a.Party_ID
    WHERE l.Party_ID IS NULL
    LIMIT 50
    """
    cursor.execute(sample_sql)
    missing_ids = [r[0] for r in cursor.fetchall()]
    if missing_ids:
        print(f"[抽樣] CSV 新統編（TaxInfo 尚無）：{len(missing_ids)} 筆，前幾個：{missing_ids[:10]}")

# =========================
# 測試：列 100 筆 TaxInfo
# =========================
def test():
    with mydb.cursor() as cursor:
        cursor.execute("SELECT * FROM crawlerdb.TaxInfo LIMIT 100")
        for row in cursor.fetchall():
            print(row)

# =========================
# main
# =========================
def main():
    """
    BY TaxInfo Step
    總表 Step
    """
    gcis_Info_to_csv()
    total, data_rows = count_csv_rows('BGMOPEN1.csv')
    print(total, data_rows)
    gcis_Info_csv_to_db('BGMOPEN1.csv')
    new_taxrc()


    # 驗證一次
    #verify_ingest()

    # 若需跑以下 API 入庫，解除註解即可
    # bizcom_type_to_db()

    """
    api 1 入庫
    公司登記基本資料-應用一
    營副業、資本額
    """
    # c1_to_db()

    """
    api 2 入庫
    公司登記基本資料-應用三
    可營項目
    """
    # c3_to_db()
    # item_to_db()

    """
    api 3 入庫
    公司董監事/持股
    """
    # ds_to_db()


if __name__ == '__main__':
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("START Time : ", now)
    try:
        main()
    finally:
        print("End Time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        # mydb.close()
