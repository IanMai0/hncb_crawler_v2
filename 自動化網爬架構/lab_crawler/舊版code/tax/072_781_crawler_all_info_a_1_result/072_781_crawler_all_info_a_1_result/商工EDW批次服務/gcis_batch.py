# -*- coding: utf-8 -*-
"""
gcis_batch.py（已修補版）
- 一律 HTTPS 與憑證驗證
- requests 共用 Session（Retry + timeout）
- S3 操作改 boto3（避免命令注入）
- SQL 全面參數化
- 移除硬編碼憑證，改讀環境變數
"""

import os
import re
import csv
import json
import time
from datetime import datetime

import chardet
import pandas as pd
import pymysql
import requests
from bs4 import BeautifulSoup
from tqdm import trange

# ---------- 連線/安全預設 ----------
REQ_TIMEOUT = (10, 30)   # (connect, read)
GCIS_BASE = "https://data.gcis.nat.gov.tw/od/data/api"

from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

def make_session() -> requests.Session:
    s = requests.Session()
    r = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
    )
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({"User-Agent": "HNBC-GCIS-BATCH/1.0"})
    return s

SESSION = make_session()

# ---------- S3：以 boto3 取代 subprocess ----------
import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse

AWS_REGION   = os.getenv("AWS_REGION", "ap-northeast-1")
S3_INPUT_PATH  = os.getenv("S3_INPUT_PATH")   # s3://bucket/prefix/
S3_OUTPUT_PATH = os.getenv("S3_OUTPUT_PATH")  # s3://bucket/prefix/
_s3 = boto3.client("s3", region_name=AWS_REGION)

def _parse_s3_uri(s3_uri: str):
    u = urlparse(s3_uri)
    if u.scheme != "s3":
        raise ValueError("S3 路徑需為 s3:// 開頭")
    bucket = u.netloc
    key = u.path.lstrip("/")
    return bucket, key

def aws_s3_ls(s3_path):
    """列出 prefix 物件，回傳 True/False 與模擬 aws cli 的輸出文字（相容舊流程）。"""
    try:
        bucket, prefix = _parse_s3_uri(s3_path)
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        paginator = _s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        lines = []
        for page in pages:
            for o in page.get("Contents", []):
                ts = o["LastModified"].strftime("%Y-%m-%d %H:%M:%S")
                lines.append(f"{ts} {o['Size']} {o['Key'].split('/')[-1]}")
        return True, "\n".join(lines)
    except ClientError as e:
        return False, f"S3 列舉失敗：{e}"

def aws_s3_download(s3_path, local_dir):
    """下載單一檔案（s3://bucket/prefix/file.csv -> local_dir/file.csv）"""
    try:
        bucket, key = _parse_s3_uri(s3_path)
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, os.path.basename(key))
        _s3.download_file(bucket, key, dest)
        return True, f"下載成功：{dest}"
    except Exception as e:
        return False, f"下載失敗：{e}"

def aws_s3_upload(local_path, s3_dir):
    """上傳單一檔案到指定 prefix（保留原檔名）"""
    try:
        bucket, prefix = _parse_s3_uri(s3_dir)
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        key = prefix + os.path.basename(local_path)
        _s3.upload_file(local_path, bucket, key)
        return True, f"上傳成功：s3://{bucket}/{key}"
    except Exception as e:
        return False, f"上傳失敗：{e}"

def aws_s3_delete(s3_path):
    """刪除單一檔案"""
    try:
        bucket, key = _parse_s3_uri(s3_path)
        _s3.delete_object(Bucket=bucket, Key=key)
        return True, f"刪除成功：s3://{bucket}/{key}"
    except Exception as e:
        return False, f"刪除失敗：{e}"

# ---------- GCIS API（全部 https + timeout） ----------
_digit8 = re.compile(r"^\d{8}$")

def _assert_tax_id(cid: str) -> str:
    cid = str(cid).strip()
    if not _digit8.match(cid):
        raise ValueError("統一編號必須為 8 碼數字")
    return cid

def get_c1(cid):   # 基本資料
    time.sleep(0.5)
    cid = _assert_tax_id(cid)
    url = f"{GCIS_BASE}/5F64D864-61CB-4D0D-8AD9-492047CC1EA6"
    params = {"$format": "json", "$filter": f"Business_Accounting_NO eq {cid}", "$skip": 0, "$top": 50}
    r = SESSION.get(url, params=params, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.text

def get_c3(cid):   # 營業項目
    time.sleep(0.5)
    cid = _assert_tax_id(cid)
    url = f"{GCIS_BASE}/236EE382-4942-41A9-BD03-CA0709025E7C"
    params = {"$format": "json", "$filter": f"Business_Accounting_NO eq {cid}", "$skip": 0, "$top": 50}
    r = SESSION.get(url, params=params, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.text

def get_ci(cid):   # 董監事
    time.sleep(0.5)
    cid = _assert_tax_id(cid)
    # 原程式使用 http（被掃描到），已改 https
    url = f"{GCIS_BASE}/4E5F7653-1B91-4DDC-99D5-468530FAE396"
    params = {"$format": "json", "$filter": f"Business_Accounting_NO eq {cid}", "$skip": 0, "$top": 50}
    r = SESSION.get(url, params=params, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.text

# ---------- S3 列表文字轉 DataFrame（相容舊邏輯） ----------
def last_source_pro(output: str):
    lines = output.strip().split('\n')
    data = [line.split(maxsplit=3) for line in lines if len(line.split(maxsplit=3)) == 4]
    df = pd.DataFrame(data, columns=['Date', 'Time', 'Size', 'FileName'])
    df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
    df = df.sort_values(by=['DateTime', 'FileName'], ascending=[True, False]).reset_index(drop=True)
    df['Order'] = range(1, len(df) + 1)
    return df

# ---------- 檔名與內容檢核 ----------
def file_name_check(file_name):
    file_name = file_name.strip()
    pattern = r'^(\d{3})_(\d{3})_(crawler)_([a-z]+)_([a-z]+)_([a-z])_(\d+)\.csv$'
    m = re.match(pattern, file_name)
    if not m:
        return False, f"檔名格式不符：{file_name}", None

    unit, department, validation, source, item, mode, sequence_number = (
        m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7)
    )
    try:
        if not (len(unit) == 3 and unit.isdigit()):            raise ValueError("部門編號應為三位數字")
        if not (len(department) == 3 and department.isdigit()):raise ValueError("科別編號應為三位數字")
        if validation != "crawler":                            raise ValueError("驗證字段應為 'crawler'")
        if not (len(source) <= 10 and source.isalpha()):       raise ValueError("來源網站應為 <=10 英文字母")
        if not (len(item) <= 10 and item.isalpha()):           raise ValueError("來源資料應為 <=10 英文字母")
        if not (len(mode) == 1 and mode.isalpha()):            raise ValueError("模式應為 1 位英文字母")
        if not sequence_number.isdigit():                      raise ValueError("序號應為數字")
    except ValueError as e:
        return False, str(e), None

    return True, "filename-OK", (unit, department, validation, source, item, mode, sequence_number)

def file_data_check(csv_path):
    csv_path = csv_path.strip()
    try:
        with open(csv_path, 'rb') as f:
            enc = chardet.detect(f.read()).get('encoding') or 'utf-8'
        df = pd.read_csv(csv_path, encoding=enc, dtype=str)
        return True, "OK", df
    except Exception as e:
        return False, f"CSV 檔案檢查失敗：{e}", None

# ---------- 結果組裝 ----------
def gcis_info_record(json_object, dfix, dfid):
    try:
        return {
            '序號': dfix, '統一編號': dfid, '狀態': '成功',
            '公司統一編號': json_object.get('Business_Accounting_NO', 'null'),
            '公司狀況描述': json_object.get('Company_Status_Desc', 'null/A'),
            '公司名稱': json_object.get('Company_Name', 'null/A'),
            '資本總額(元)': json_object.get('Capital_Stock_Amount', 'null'),
            '實收資本額(元)': json_object.get('Paid_In_Capital_Amount', 'null'),
            '代表人姓名': json_object.get('Responsible_Name', 'null'),
            '公司登記地址': json_object.get('Company_Location', 'null'),
            '登記機關名稱': json_object.get('Register_Organization_Desc', 'null'),
            '核准設立日期': json_object.get('Company_Setup_Date', 'null'),
            '最後核准變更日期': json_object.get('Change_Of_Approval_Data', 'null'),
            '撤銷日期': json_object.get('Revoke_App_Date', 'null'),
            '停復業狀況': json_object.get('Case_Status', 'null'),
            '停復業狀況描述': json_object.get('Case_Status_Desc', 'null'),
            '停業核准日期': json_object.get('Sus_App_Date', 'null'),
            '停業/延展期間(起)': json_object.get('Sus_Beg_Date', 'null'),
            '停業/延展期間(迄)': json_object.get('Sus_End_Date', 'null')
        }
    except Exception:
        return {'序號': dfix, '統一編號': dfid, '狀態': '商工查無此統一編號'}

def gcis_items_record(json_object, dfix, dfid):
    records = []
    company_info = {'序號': dfix, '統一編號': dfid, '狀態': '成功', '公司名稱': json_object.get('Company_Name', 'null/A')}
    for it in json_object.get('Cmp_Business', []):
        rec = company_info.copy()
        rec.update({
            '營業項目序號': it.get('Business_Seq_NO', 'null'),
            '營業項目': it.get('Business_Item', 'null'),
            '營業項目說明': it.get('Business_Item_Desc', 'null')
        })
        records.append(rec)
    return records

def gcis_director_record(json_list, dfix, dfid):
    records = []
    base = {'序號': dfix, '統一編號': dfid, '狀態': '成功'}
    for it in json_list:
        rec = base.copy()
        rec.update({
            '董監事職稱': it.get('Person_Position_Name', 'null'),
            '董監事姓名': it.get('Person_Name', 'null'),
            '董監事持股': it.get('Person_Shareholding', 'null')
        })
        records.append(rec)
    return records

# ---------- DB ----------
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

def get_db_connection():
    return pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
        charset='utf8mb4', autocommit=False, connect_timeout=15
    )

def file_name_indb(vars, s3load_time, df_num, connection):
    unit, department, source, item, mode, sequence_number = vars[0], vars[1], vars[3], vars[4], vars[5], int(vars[6])
    try:
        with connection.cursor() as cur:
            sql = """
                INSERT INTO crawlerdb.Crawler_Batch_History
                (unit, department, source, item, mode, sequence_number, s3_source_datetime, total_count, execution_method)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            cur.execute(sql, (unit, department, source, item, mode, sequence_number, s3load_time, df_num, 'gcis_batch'))
        connection.commit()
        print('sql_insert')
    except Exception as e:
        connection.rollback()
        print(f"DB 資料插入失敗：{e}")

def update_record_indb(vars, s3load_time, x_unm, o_unm, connection):
    unit, department, source, item, mode, sequence_number = vars[0], vars[1], vars[3], vars[4], vars[5], int(vars[6])
    try:
        with connection.cursor() as cur:
            select_sql = """
                SELECT id FROM crawlerdb.Crawler_Batch_History
                WHERE unit=%s AND department=%s AND source=%s AND item=%s AND mode=%s
                  AND sequence_number=%s AND s3_source_datetime=%s
                ORDER BY source_insert_datetime DESC
                LIMIT 1
            """
            cur.execute(select_sql, (unit, department, source, item, mode, sequence_number, s3load_time))
            row = cur.fetchone()
            if row:
                update_sql = """
                    UPDATE crawlerdb.Crawler_Batch_History
                    SET result_completion_datetime = NOW(),
                        success_count = %s,
                        failure_count = %s
                    WHERE id = %s
                """
                cur.execute(update_sql, (o_unm, x_unm, row[0]))
                connection.commit()
                print('sql_update')
            else:
                print('未找到符合條件的資料進行更新')
    except Exception as e:
        connection.rollback()
        print(f"DB 更新失敗：{e}")

def update_s3result_time_indb(vars, s3_result_time, connection):
    """更新 s3_result_datetime；依相同條件鎖定一筆紀錄"""
    unit, department, source, item, mode, sequence_number = vars[0], vars[1], vars[3], vars[4], vars[5], int(vars[6])
    try:
        with connection.cursor() as cur:
            select_sql = """
                SELECT id FROM crawlerdb.Crawler_Batch_History
                WHERE unit=%s AND department=%s AND source=%s AND item=%s AND mode=%s
                  AND sequence_number=%s AND s3_source_datetime=%s
                ORDER BY source_insert_datetime DESC
                LIMIT 1
            """
            cur.execute(select_sql, (unit, department, source, item, mode, sequence_number, s3_result_time))
            row = cur.fetchone()
            if row:
                cur.execute("UPDATE crawlerdb.Crawler_Batch_History SET s3_result_datetime=%s WHERE id=%s",
                            (s3_result_time, row[0]))
                connection.commit()
                print('s3_result_time 更新成功')
            else:
                print('update_s3result_time_indb 未找到符合條件的資料進行更新')
    except Exception as e:
        connection.rollback()
        print(f"s3_result_time 更新失敗：{e}")

# ---------- CSV 輸出 ----------
def write_results_to_csv(records, output_path):
    fieldnames = [
        '來源檔案ID', '統一編號', '狀態',
        '公司統一編號', '公司狀況描述', '公司名稱', '資本總額(元)', '實收資本額(元)',
        '代表人姓名', '公司登記地址', '登記機關名稱', '核准設立日期',
        '最後核准變更日期', '撤銷日期', '停復業狀況', '停復業狀況描述',
        '停業核准日期', '停業/延展期間(起)', '停業/延展期間(迄)'
    ]
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

# ---------- 主流程 ----------
def main():
    # 資料夾（本機）
    local_download_path = os.getenv("LOCAL_INPUT_DIR",  r"C:\HNB_Code\gcis_batch\input/")
    local_result_path   = os.getenv("LOCAL_OUTPUT_DIR", r"C:\HNB_Code\gcis_batch\output/")

    # DB
    conn = get_db_connection()

    # 列舉 S3 來源
    ok, output = aws_s3_ls(S3_INPUT_PATH)
    if not ok or not output.strip():
        print("無爬蟲檔案須處理...")
        return

    filename_df = last_source_pro(output)
    lines = output.strip().split('\n')
    today = datetime.today().strftime('%Y%m%d')

    print(f"process_count:{len(lines)}")
    print(filename_df['FileName'])

    for x in range(1, len(lines)+1):
        print(f"處理中: {x}")
        filtered = filename_df.loc[filename_df['Order'] == x, 'FileName']
        if filtered.empty:
            print(f"找不到符合條件的檔案，Order = {x}")
            continue

        first_file = filtered.values[0]
        s3load_time = filename_df.loc[filename_df['Order'] == x, 'DateTime'].values[0]
        s3load_time = pd.to_datetime(s3load_time).strftime('%Y-%m-%d %H:%M:%S')

        # 下載來源 CSV
        s3_ofile_path = f"{S3_INPUT_PATH}{first_file}".strip()
        ok, msg = aws_s3_download(s3_ofile_path, local_download_path)
        print(ok, msg)

        # 檔名解析與內容檢核
        name_ok, name_msg, vars = file_name_check(first_file)
        print(name_ok, name_msg, vars)
        if not name_ok:
            print(name_msg)
            continue

        local_path = os.path.join(local_download_path, first_file)
        data_ok, data_msg, df = file_data_check(local_path)
        if not data_ok:
            print(f"{first_file}-CSV內容錯誤：{data_msg}")
            continue

        df_num = df.shape[0]
        file_name_indb(vars, s3load_time, df_num, conn)

        # 依來源項目分支處理
        records = []
        o_unm, x_unm = 0, 0
        if vars[3] == 'gcis':
            if vars[4] == 'info':
                Num = df.shape[0]
                for row in trange(0, Num):
                    time.sleep(0.2)
                    dfix = df.iloc[row]['序號']
                    dfid = df.iloc[row]['統一編號']
                    try:
                        jtxt = get_c1(dfid)
                        jobj = json.loads(jtxt[1:-1])
                        rec = gcis_info_record(jobj, dfix, dfid)
                        records.append(rec)
                        o_unm += 1
                    except Exception:
                        records.append({'序號': dfix, '統一編號': dfid, '狀態': '失敗'})
                        x_unm += 1

            elif vars[4] == 'items':
                Num = df.shape[0]
                for row in trange(0, Num):
                    time.sleep(0.2)
                    dfix = df.iloc[row]['序號']
                    dfid = df.iloc[row]['統一編號']
                    try:
                        jtxt = get_c3(dfid)
                        jobj = json.loads(jtxt[1:-1])
                        items = gcis_items_record(jobj, dfix, dfid)
                        records.extend(items)
                        o_unm += len(items)
                    except Exception:
                        records.append({'序號': dfix, '統一編號': dfid, '狀態': '失敗'})
                        x_unm += 1

            elif vars[4] == 'director':
                Num = df.shape[0]
                for row in trange(0, Num):
                    time.sleep(0.2)
                    dfix = df.iloc[row]['序號']
                    dfid = df.iloc[row]['統一編號']
                    try:
                        jtxt = get_ci(dfid)
                        jobj = json.loads(jtxt)
                        directors = gcis_director_record(jobj, dfix, dfid)
                        records.extend(directors)
                        o_unm += len(directors)
                    except Exception:
                        records.append({'序號': dfix, '統一編號': dfid, '狀態': '失敗'})
                        x_unm += 1
            else:
                print(f"gcis no this source: {vars[4]}")
                continue
        else:
            print(f"no this validation: {vars[3]}")
            continue

        # 產出結果檔案
        result_name = f"{vars[0]}_{vars[1]}_{vars[2]}_{vars[3]}_{vars[4]}_{vars[5]}_{vars[6]}_result_{today}.csv"
        local_result = os.path.join(local_result_path, result_name)
        os.makedirs(local_result_path, exist_ok=True)
        pd.DataFrame(records).to_csv(local_result, index=False, encoding="utf-8")
        print(f"資料已成功寫入 {result_name}")

        # 更新 DB 成功/失敗筆數
        update_record_indb(vars, s3load_time, x_unm, o_unm, conn)

        # 上傳結果到 S3（output）
        ok, umsg = aws_s3_upload(local_result, S3_OUTPUT_PATH)
        print(ok, umsg)

        # 以 S3 列表時間當作 result_s3_time（若需要）
        ok, out_ls = aws_s3_ls(S3_OUTPUT_PATH)
        if ok:
            result_s3_time = "未找到檔案"
            for line in out_ls.splitlines():
                if result_name in line:
                    result_s3_time = ' '.join(line.split()[:2])
                    break
            print(f"結果檔案上傳時間: {result_s3_time}")
        else:
            print(f"錯誤: {out_ls}")
            result_s3_time = s3load_time

        # 再次更新（如果你需要紀錄 s3_result_datetime）
        try:
            conn.ping(reconnect=True)
            update_record_indb(vars, s3load_time, x_unm, o_unm, conn)
        except pymysql.MySQLError as e:
            print(f"資料庫操作失敗：{e}")

        # 若你的資料表要記錄 s3_result_datetime，沿用原本條件（相容既有流程）
        update_s3result_time_indb(vars, pd.to_datetime(s3load_time).strftime('%Y-%m-%d %H:%M:%S'), conn)

        # 清理：刪本機、刪雲端 input
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
            if os.path.exists(local_result):
                os.remove(local_result)
            ok, dmsg = aws_s3_delete(s3_ofile_path)
            print(ok, dmsg)
        except Exception as e:
            print(f"清理檔案時發生錯誤：{e}")

    # 結束
    conn.close()

def qmain():
    pass

if __name__ == '__main__':
    main()
