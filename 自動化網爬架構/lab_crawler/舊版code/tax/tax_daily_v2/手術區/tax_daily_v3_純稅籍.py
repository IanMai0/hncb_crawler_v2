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
def msdb_to_df(mydb_conn, sql):
    cursor = mydb_conn.cursor()
    cursor.execute(sql)
    df = DataFrame(cursor.fetchall())
    return df

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
    url = "https://eip.fia.gov.tw/data/BGMOPEN1.zip"
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
    # 當日異動
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


if __name__ == '__main__':
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("START Time : ", now)
    try:
        main()
    finally:
        print("End Time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        # mydb.close()
