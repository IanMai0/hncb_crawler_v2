# -*- coding: utf-8 -*-
"""
075_Crawler（已修補弱點版）
- 全面啟用 HTTPS 與憑證驗證
- requests 設定 timeout + 重試
- S3 改用 boto3（避免命令注入）
- AWS/DB 憑證改讀環境變數（可對接 Secrets Manager）
- 保留原來主要流程與輸出
"""
import os
import re
import csv
import json
import time
import logging
import warnings
from datetime import datetime, timedelta

import pandas as pd
import requests
import pymysql
from pandas import DataFrame
from tqdm import tqdm, trange
from bs4 import BeautifulSoup

# === 安全：不要關閉任何 SSL 驗證與警告 ===
warnings.filterwarnings("ignore", category=UserWarning)

# ---------- 環境變數 / 常數 ----------
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
S3_URI = os.getenv("S3_URI", "s3://hncb/075-crawler/")
LOCAL_DOWNLOAD_PATH = os.getenv("LOCAL_DOWNLOAD_PATH", r"C:/HNB_Code/075_Crawler/list_folder/")
LOCAL_RESULT_PATH = os.getenv("LOCAL_RESULT_PATH", r"C:/HNB_Code/075_Crawler/result_folder/")

# requests 超時（連線, 讀取）
REQ_TIMEOUT = (10, 30)

# ---------- requests Session + Retry（所有 HTTP 呼叫共用） ----------
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

def make_session():
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST")
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    })
    return s

SESSION = make_session()

# ---------- DB ----------
def db_connect():
    try:
        conn = pymysql.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
            database=DB_NAME, charset="utf8mb4", autocommit=False
        )
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return None

def msdb_to_df(mydb, sql):
    with mydb.cursor() as cursor:
        cursor.execute(sql)
        df = DataFrame(cursor.fetchall())
    return df

def insert_record_indb(connection, unit, department, source, item, mode,
                       sequence_number, today_time, s3_result_datetime,
                       execution_method, memo, m_sum, m_o, m_x):
    try:
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO crawlerdb.Crawler_Batch_History (
                unit, department, source, item, mode, sequence_number,
                s3_source_datetime, s3_result_datetime, execution_method, memo,
                total_count, success_count, failure_count
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            vals = (unit, department, source, item, mode, sequence_number,
                    today_time, s3_result_datetime, execution_method, memo,
                    m_sum, m_o, m_x)
            cursor.execute(sql, vals)
        connection.commit()
        print("✅ 資料成功插入 MySQL")
    except Exception as e:
        connection.rollback()
        print(f"❌ DB 資料插入失敗：{e}")

# ---------- 公用：日期處理 ----------
def date_check_insql(date_str):
    date_str = (date_str or "").replace(' ', '').replace('　', '')
    if len(date_str) > 1:
        gregorian_year = int(date_str[:3]) + 1911
        date_str = f"'{gregorian_year:04d}-{date_str[3:5]}-{date_str[5:7]}'"
    if date_str == '':
        date_str = 'NULL'
    return date_str

# ---------- GCIS / TWSE / TPEx ----------
def get_c1(cid: str):
    # https + 參數化 querystring，且加 timeout
    url = "https://data.gcis.nat.gov.tw/od/data/api/5F64D864-61CB-4D0D-8AD9-492047CC1EA6"
    params = {
        "$format": "json",
        "$filter": f"Business_Accounting_NO eq {cid}",
        "$skip": 0,
        "$top": 50
    }
    r = SESSION.get(url, params=params, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.text

def get_c3(cid: str):
    url = "https://data.gcis.nat.gov.tw/od/data/api/236EE382-4942-41A9-BD03-CA0709025E7C"
    params = {"$format": "json", "$filter": f"Business_Accounting_NO eq {cid}", "$skip": 0, "$top": 50}
    r = SESSION.get(url, params=params, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.text

def get_ci(cid: str):
    # 修正為 HTTPS
    url = "https://data.gcis.nat.gov.tw/od/data/api/FCB90AB1-E382-45CE-8D4F-394861851E28"
    params = {"$format": "json", "$filter": f"Business_Item eq {cid}", "$skip": 0, "$top": 50}
    r = SESSION.get(url, params=params, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.text

def get_bc(cid: str):
    url = "https://data.gcis.nat.gov.tw/od/data/api/673F0FC0-B3A7-429F-9041-E9866836B66D"
    params = {"$format": "json", "$filter": f"No eq {cid}", "$skip": 0, "$top": 50}
    r = SESSION.get(url, params=params, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.text

def get_ds(cid: str):
    url = "https://data.gcis.nat.gov.tw/od/data/api/4E5F7653-1B91-4DDC-99D5-468530FAE396"
    params = {"$format": "json", "$filter": f"Business_Accounting_NO eq {cid}", "$skip": 0, "$top": 50}
    r = SESSION.get(url, params=params, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.text

def fetch_stock_dats(stock_code):
    url = "https://mopsov.twse.com.tw/mops/web/ajax_t05st03"
    payload = {
        "encodeURIComponent": "1",
        "step": "1",
        "firstin": "1",
        "off": "1",
        "queryName": "co_id",
        "inputType": "co_id",
        "TYPEK": "all",
        "isnew": "false",
        "co_id": stock_code,
    }
    try:
        resp = SESSION.post(url, data=payload, timeout=REQ_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        return str(soup.find('html'))
    except Exception as e:
        logging.info(f"fetch_stock_dats error: {e}")
        return ""

def open_equitySecurities(local_download_path, target_date):
    file_path = f"{local_download_path}equitysecurities_{target_date}.csv"
    with open(file_path, encoding="utf-8") as csvfile:
        rows = list(csv.reader(csvfile))
    df = pd.DataFrame(rows, columns=['股票代號', '統一編號', '董事長名稱'])
    return df

def equitySecurities_process(df):
    bdf = df.copy()
    Num = bdf.shape[0]
    O_num = X_num = 0
    for row in trange(0, Num):
        time.sleep(0.2)
        dfid = bdf.iloc[row]['統一編號']
        try:
            clist = get_c1(dfid)
            json_object = json.loads(clist[1:-1])
            responsible_name = json_object.get('Responsible_Name') or 'isnull'
            if re.match(r'^\s*$', str(responsible_name)):
                responsible_name = 'isnull'
            bdf.at[row, '董事長名稱'] = responsible_name
            O_num += 1
        except Exception as e:
            bdf.at[row, '董事長名稱'] = 'gcis_error'
            logging.info(f"equitysecurities {dfid} Error: {e}")
            X_num += 1
    log_char = f"equitysecurities-sum :{Num} O_num:{O_num} ,X_num: {X_num}"
    logging.info(log_char)
    return bdf, log_char, Num, O_num, X_num

def stockID_process(df):
    o_num = x_num = 0
    Num = df.shape[0]
    print("mops_Start")
    for _, row in df.iterrows():
        stid = row['股票代號']
        time.sleep(5)
        stock_data = fetch_stock_dats(stid)
        if not stock_data:
            df.loc[df['股票代號'] == stid, '董事長名稱'] = 'timeout'
            x_num += 1
            time.sleep(10)
            continue
        soup = BeautifulSoup(stock_data, 'html.parser')
        try:
            company_not_found = soup.find('center').find('h3')
            if company_not_found and "公司不存在" in company_not_found.text:
                df.loc[df['股票代號'] == stid, '董事長名稱'] = '公司不存在'
                x_num += 1
                continue
            msg = soup.find('center').find('font', color="BLUE")
            if msg and ("查詢過於頻繁" in msg.text or "Too many query requests" in msg.text):
                time.sleep(30)
                continue
            if "頁面無法執行" in soup.text or "THE PAGE CANNOT BE ACCESSED" in soup.text:
                df.loc[df['股票代號'] == stid, '董事長名稱'] = 'Timeout'
                x_num += 1
                continue
        except AttributeError:
            try:
                stid_f = soup.find('th', string='股票代號').find_next_sibling('td').string.strip()
                chairman = soup.find('th', string='董事長').find_next_sibling('td').string.strip()
                df.loc[df['股票代號'] == stid_f, '董事長名稱'] = chairman
                o_num += 1
            except Exception as e:
                logging.info(f"{stid} 無法查詢：{e}")
                df.loc[df['股票代號'] == stid, '董事長名稱'] = 'NoDate'
                x_num += 1
    log_char = f"mopsList-sum :{Num} O_num:{o_num} ,X_num :{x_num}"
    logging.info(log_char)
    return df, log_char, Num, o_num, x_num

import html as _html
def convert_html_entities(text):
    return _html.unescape(text) if isinstance(text, str) else text

TWSE_API_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TPEX_API_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"

def fetch_twse_data(mops_df):
    try:
        mops_df = mops_df.copy()
        mops_df.columns = mops_df.columns.str.strip()
        if '股票代號' not in mops_df.columns:
            raise KeyError("mops_df 缺少 '股票代號' 欄位")
        resp1 = SESSION.get(TWSE_API_URL, timeout=REQ_TIMEOUT); resp1.raise_for_status()
        resp2 = SESSION.get(TPEX_API_URL, timeout=REQ_TIMEOUT); resp2.raise_for_status()
        twse_data = resp1.json()
        tpex_data = resp2.json()
        map_twse = {e['公司代號']: e['董事長'] for e in twse_data}
        map_tpex = {e['SecuritiesCompanyCode']: e['Chairman'] for e in tpex_data}
        mapping = {**map_twse, **map_tpex}
        mops_df['董事長名稱'] = mops_df['股票代號'].astype(str).str.strip().map(mapping).fillna('mops_error')
        mops_df['董事長名稱'] = mops_df['董事長名稱'].apply(convert_html_entities)
        mops_o = (mops_df['董事長名稱'] != 'mops_error').sum()
        mops_x = (mops_df['董事長名稱'] == 'mops_error').sum()
        mops_sum = len(mops_df)
        mops_logchar = f"MOPS-sum :{mops_sum} O_num:{mops_o} ,X_num: {mops_x}"
        print(mops_df, mops_logchar, mops_sum, mops_o, mops_x)
        return mops_df, mops_logchar, mops_sum, mops_o, mops_x
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return mops_df.assign(董事長名稱="mops_error"), "Fetch failed", len(mops_df), 0, len(mops_df)

def fetch_tpex_chairman(stk_code):
    url = "https://www.tpex.org.tw/web/regular_emerging/corporateInfo/regular/regular_stock_detail.php"
    params = {"l": "zh-tw", "stk_code": stk_code}
    resp = SESSION.get(url, params=params, timeout=REQ_TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    tag = soup.select_one(
        "div:nth-of-type(4) div:nth-of-type(2) div:nth-of-type(5) table:nth-of-type(1) tr:nth-of-type(4) td:nth-of-type(1)"
    )
    return tag.text.strip() if tag else "mops_error"

def StakeHolder_process(local_download_path, target_date):
    file_path = f"{local_download_path}stakeHolder_{target_date}.csv"
    with open(file_path, encoding="utf-8") as csvfile:
        rows = list(csv.reader(csvfile))
    df = pd.DataFrame(rows, columns=['代號', '統一編號', '董事長名稱'])
    bdf = df.copy()
    Num = bdf.shape[0]
    O_num = X_num = 0
    for row in trange(0, Num):
        time.sleep(0.2)
        dfid = bdf.iloc[row]['統一編號']
        try:
            clist = get_c1(dfid)
            json_object = json.loads(clist[1:-1])
            responsible_name = json_object.get('Responsible_Name') or 'isnull'
            bdf.at[row, '董事長名稱'] = responsible_name
            O_num += 1
        except Exception as e:
            bdf.at[row, '董事長名稱'] = 'gcis_error'
            logging.info(f"StakeHolder_process-{dfid} Error: {e}")
            X_num += 1
    log_char = f"StakeHolder-sum :{Num} O_num:{O_num} ,X_num: {X_num}"
    logging.info(log_char)
    return bdf, log_char, Num, O_num, X_num

def mops_process(local_download_path, target_date):
    file_path = f"{local_download_path}mops_{target_date}.csv"
    with open(file_path, encoding="utf-8") as csvfile:
        rows = list(csv.reader(csvfile))
    df = pd.DataFrame(rows, columns=['代號', '股票代號', '董事長名稱'])
    return df

# ---------- S3：改用 boto3（杜絕命令注入） ----------
import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse

def parse_s3_uri(s3_uri: str):
    u = urlparse(s3_uri)
    if u.scheme != "s3":
        raise ValueError("S3_URI must start with s3://")
    bucket = u.netloc
    prefix = u.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix

_s3_client = boto3.client("s3", region_name=AWS_REGION)

def aws_s3_ls(s3_path):
    try:
        bucket, prefix = parse_s3_uri(s3_path)
        paginator = _s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        lines = []
        for page in pages:
            for obj in page.get("Contents", []):
                ts = obj["LastModified"].strftime("%Y-%m-%d %H:%M:%S")
                size = obj["Size"]
                key = obj["Key"]
                lines.append(f"{ts} {size} {key}")
        return True, "\n".join(lines)
    except ClientError as e:
        return False, f"S3 列出失敗：{e}"

def aws_s3_download(s3_path, local_path):
    try:
        bucket, prefix = parse_s3_uri(s3_path)
        # s3_path 這裡會給完整 key，故轉為 key
        key = prefix  # 若 s3_path 是 s3://bucket/path/file.csv
        # 若傳入的是 s3://bucket/dir/ + 檔名，呼叫方已拼好，這裡直接取最後段
        if key.endswith("/"):
            raise ValueError("請提供完整 S3 物件路徑（包含檔名）")
        filename = os.path.basename(key)
        os.makedirs(local_path, exist_ok=True)
        dest = os.path.join(local_path, filename)
        _s3_client.download_file(bucket, key, dest)
        return True, f"下載成功：{dest}"
    except Exception as e:
        return False, f"下載失敗：{e}"

def aws_s3_upload(local_path, s3_path):
    try:
        bucket, prefix = parse_s3_uri(s3_path)
        if prefix.endswith("/"):
            key = prefix + os.path.basename(local_path)
        else:
            key = prefix
        _s3_client.upload_file(local_path, bucket, key)
        return True, f"上傳成功：s3://{bucket}/{key}"
    except Exception as e:
        return False, f"上傳失敗：{e}"

def check_files(log_data, target_date):
    data = []
    for line in (log_data or "").strip().split('\n'):
        parts = line.split(maxsplit=2)
        if len(parts) == 3:
            data.append(parts)
    df = pd.DataFrame(data, columns=['Timestamp', 'Size', 'Filename'])
    required = [
        f'stakeholder_{target_date}.csv',
        f'equitysecurities_{target_date}.csv',
        f'config_{target_date}.txt',
        f'mops_{target_date}.csv',
    ]
    status = {}
    for fname in required:
        status[fname] = 'exist' if not df.empty and df['Filename'].str.contains(fname).any() else 'Missing'
    return status

# ---------- 主流程 ----------
def main():
    # 路徑
    s3_path = S3_URI
    local_download_path = LOCAL_DOWNLOAD_PATH
    local_result_path = LOCAL_RESULT_PATH

    # DB 連線
    connection = db_connect()
    if not connection:
        raise RuntimeError("無法連線資料庫，請確認環境變數。")

    StakeHolder_logchar = 'Stakeholder not executed'
    equity_logchar = 'Equity securities not executed'
    mops_logchar = 'MOPS not executed'

    execute_modules = ['mops', 'equity', 'stakeholder']   # 依需求調整
    today = datetime.today().strftime('%Y%m%d')
    today_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    target_date = today

    # S3 列表
    success, output = aws_s3_ls(s3_path)
    if not success:
        logging.warning(output)
    file_status = check_files(output if success else "", target_date)

    # Stakeholder
    if 'stakeholder' in execute_modules:
        StakeHolder_logchar = ''
        if file_status.get(f'stakeholder_{target_date}.csv') == 'exist':
            download_file = f"{s3_path}stakeholder_{target_date}.csv"
            ok, message = aws_s3_download(download_file, local_download_path)
            if ok:
                logging.info(f"{download_file}: Successfully downloaded")
                Stake_df, StakeHolder_logchar, Stake_sum, Stake_o, Stake_x = StakeHolder_process(local_download_path, target_date)
                Stake_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                insert_record_indb(connection, '075', '000', 'gcis', 'info', 's', 1, today_time,
                                   Stake_time, '075_Crawler', 'fbbs_Sta', Stake_sum, Stake_o, Stake_x)
                os.makedirs(local_result_path, exist_ok=True)
                out_path = f"{local_result_path}stakeholder_result_{target_date}.csv"
                Stake_df.to_csv(out_path, index=False, encoding="utf-8")
                uptype, upmsg = aws_s3_upload(out_path, s3_path)
                print(uptype, upmsg)
            else:
                logging.info(f"{download_file}: Download failed - {message}")
                StakeHolder_logchar = 'Stakeholder not downloaded'
        else:
            logging.info("No Stakeholder file")
            StakeHolder_logchar = 'Stakeholder not provided'

    # Equity
    if 'equity' in execute_modules:
        equity_logchar = ''
        if file_status.get(f'equitysecurities_{target_date}.csv') == 'exist':
            download_file = f"{s3_path}equitysecurities_{target_date}.csv"
            ok, message = aws_s3_download(download_file, local_download_path)
            if ok:
                logging.info(f"{download_file}: Successfully downloaded")
                equity_df = open_equitySecurities(local_download_path, target_date)
                equity_df, equity_logchar, equity_sum, equity_o, equity_x = equitySecurities_process(equity_df)
                equity_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                insert_record_indb(connection, '075', '000', 'gcis', 'info', 's', 2, today_time,
                                   equity_time, '075_Crawler', 'fbbs_equ', equity_sum, equity_o, equity_x)
                os.makedirs(local_result_path, exist_ok=True)
                out_path = f"{local_result_path}equitysecurities_result_{target_date}.csv"
                equity_df.to_csv(out_path, index=False, encoding="utf-8")
                uptype, upmsg = aws_s3_upload(out_path, s3_path)
                print(uptype, upmsg)
            else:
                logging.info(f"{download_file}: Download failed - {message}")
                equity_logchar = 'Equity securities not downloaded'
        else:
            logging.info("Equity securities file not provided")
            equity_logchar = 'Equity securities not provided'

    # MOPS
    if 'mops' in execute_modules:
        mops_logchar = ''
        mode = 'B'  # A: 抓網頁、B: 走 API
        if file_status.get(f'mops_{target_date}.csv') == 'exist':
            download_file = f"{s3_path}mops_{target_date}.csv"
            ok, message = aws_s3_download(download_file, local_download_path)
            if ok:
                logging.info(f"{download_file}: Successfully downloaded")
                mops_df = mops_process(local_download_path, target_date)
                if mode == 'A':
                    mops_df, mops_logchar, mops_sum, mops_o, mops_x = stockID_process(mops_df)
                else:
                    mops_df, mops_logchar, mops_sum, mops_o, mops_x = fetch_twse_data(mops_df)
                os.makedirs(local_result_path, exist_ok=True)
                out_path = f"{local_result_path}mops_result_{target_date}.csv"
                mops_df.to_csv(out_path, index=False, encoding="utf-8")
                uptype, upmsg = aws_s3_upload(out_path, s3_path)
                print(uptype, upmsg)
            else:
                logging.info(f"{download_file}: Download failed - {message}")
                mops_logchar = 'Mops not downloaded'
        else:
            logging.info("Mops file not provided")
            mops_logchar = 'Mops not provided'

    # Config 檔（若存在就拉下來）
    if file_status.get(f'config_{target_date}.txt') == 'exist':
        download_file = f"{s3_path}config_{target_date}.txt"
        ok, message = aws_s3_download(download_file, local_download_path)
        if ok:
            logging.info(f"{download_file}: Successfully downloaded")
        else:
            logging.info(f"{download_file}: Download failed - {message}")

    # 寫回執行摘要
    os.makedirs(local_result_path, exist_ok=True)
    filename = f'config_{target_date}.txt'
    filepath = os.path.join(local_result_path, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(equity_logchar + '\n' + StakeHolder_logchar + '\n' + mops_logchar)
    uptype, upmsg = aws_s3_upload(filepath, s3_path)
    print(uptype, upmsg)

if __name__ == '__main__':
    start_time = datetime.now()
    print("START Time : ", start_time.strftime('%Y-%m-%d %H:%M:%S'))

    log_directory = "Logging"
    os.makedirs(log_directory, exist_ok=True)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    # Windows 主控台若有亂碼可改用檔案觀看

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(f"{log_directory}/075_Crawler_{start_time.strftime('%Y%m%d')}.log", encoding='utf-8'),
            console_handler
        ]
    )

    main()

    time.sleep(1)
    end_time = datetime.now()
    elapsed = end_time - start_time
    minutes, seconds = divmod(int(elapsed.total_seconds()), 60)
    logging.info(f"Time taken: {minutes} minutes {seconds} seconds")
    print(end_time)
    print(f"Time taken: {minutes} minutes {seconds} seconds")
