import pymysql
import logging
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple
from pandas import DataFrame
import csv


# =========================
# logger
# =========================

def get_logger(name: str = "pcc_pipeline") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    return logger


# =========================
# 資料庫連線
# =========================

def get_mysql_settings_from_env() -> Dict[str, Any]:
    host = os.getenv("MYSQL_HOST")
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    db = os.getenv("MYSQL_DB")
    port = int(os.getenv("MYSQL_PORT", "3306"))

    if not host or not user or not password or not db:
        raise ValueError("MySQL 環境變數不足：MYSQL_HOST/USER/PASSWORD/DB 必填。")

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

# =========================
# helpers 小工具
# =========================
def msdb_to_df(mydb_conn, sql):
    cursor = mydb_conn.cursor()
    cursor.execute(sql)
    df = DataFrame(cursor.fetchall())
    return df


def _current_db_name(conn) -> str:
    """回傳目前連線使用的 DB 名稱。"""
    with conn.cursor() as cur:
        cur.execute("SELECT DATABASE() AS db")
        row = cur.fetchone()
        return str(row.get("db") or "").strip()


def table_exists(conn, table_name: str, schema_name: Optional[str] = None) -> bool:
    """檢查 table 是否存在（避免因 legacy DB 缺表而整個流程炸掉）。"""
    schema = (schema_name or _current_db_name(conn) or "crawlerdb").strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 AS ok
            FROM information_schema.tables
            WHERE table_schema=%s AND table_name=%s
            LIMIT 1
            """,
            (schema, table_name),
        )
        return cur.fetchone() is not None

# 民國轉西元
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

# 試著把文字變成整數，如果不像數字，就乾脆給空值（None）
def _to_int_or_none(v):
    v = (v or "").strip()
    return int(v) if v.isdigit() else None


# =========================
# daily cleanup (TRUNCATE)
# =========================

def truncate_daily_tables(conn, logger: Optional[logging.Logger] = None) -> None:
    """日批前後都可呼叫。

    - crawlerdb.tmp_rawData：新表
    - crawlerdb.Tmp_TaxInfo：舊表

    你要求：日批用完就清空，不留在 tmp 表。
    """
    logger = logger or get_logger()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE crawlerdb.tmp_rawData")
        cur.execute("TRUNCATE TABLE crawlerdb.Tmp_TaxInfo")
    conn.commit()
    logger.info("🧹 已清空日批暫存表：tmp_rawData / Tmp_TaxInfo")


# =========================
# CSV -> 暫存表（參數化、批次）
# =========================
# def gcis_Info_csv_to_db(csv_path='BGMOPEN1.csv'):
def pcc_excellent_to_db(csv_path):
    '''
    這邊 import csv 是暫時的作法, 當整個程式流程確認沒有問題以後, 就要直接從 xls > 暫存處理 > df > db,
    且全程就不用再額外落檔 csv, 保留原先 raw data 的 xls file 即可
    '''

    # read df

    # read csv
    with open(csv_path, newline='', encoding="utf-8") as csvfile:
        rows = list(csv.reader(csvfile))

    # 先清空暫存表
    mydbcursor.execute("TRUNCATE TABLE crawlerdb.pcc_excellent_tmp")  # 清空整張資料表的內容，但保留資料表的結構**
    mydb.commit()

    if not rows:
        print("預計寫入批次內容為空，已清空 pcc_excellent_tmp。")
        return

    # 準備批次資料（跳第一列表頭）
    batch = []
    for idx, r in enumerate(rows):
        if idx == 0:
            continue
        # 防全形空白 & 去除一般空白
        r = [(r[i] or '').replace(' ', '').replace('\u3000', '') for i in range(len(r))]

        # 以下這塊處理廠商相關欄位
        corporation_number = _to_int_or_none(r[0]) or None   # 廠商統編 / 廠商代碼 > 嘗試轉數字
        corporation_name = r[1] or None     # 廠商名稱
        corporation_address = r[2] or None  # 廠商地址
        # 以下這塊處理時間相關欄位
        effective_date = _tw_roc_to_iso(r[10]) or None # 獎勵起始日期 > 民國轉西元
        expire_date = _tw_roc_to_iso(r[11]) or None    # 獎勵終止日期 > 民國轉西元
        # 以下這塊處理標案相關欄位
        judgment_no = r[9] or None    # 評優良廠商依據之規定
        # 以下這塊處理機關相關欄位
        announce_agency_no = r[12] or None   # 通知主管機關文號
        announce_agency_name = r[4] or None  # 機關名稱/刊登機關名稱
        announce_agency_code = r[3] or None  # 機關代碼/刊登機關代碼
        announce_agency_address = r[5] or None  # 機關地址
        contact_person = r[6] or None  # 聯絡人/機關聯絡人
        contact_phone = r[7] or None  # 聯絡電話/機關聯絡人電話/聯絡人電話
        announce_agency_mail = r[8] or None  # 機關聯絡人電子郵件信箱
        # 以下這塊處理其它欄位
        remark = r[13] or None  # 備註

        batch.append((
            corporation_number, corporation_name, corporation_address, announce_agency_code, announce_agency_name,
            announce_agency_address, contact_person, contact_phone, announce_agency_mail, judgment_no, effective_date,
            expire_date,  announce_agency_no, remark
        ))

    insert_sql = """
        INSERT INTO crawlerdb.pcc_excellent_tmp
        (Corporation_number, Corporation_name, Corporation_address, Announce_agency_code, Announce_agency_name,
            Announce_agency_address, Contact_person, Contact_phone, Announce_agency_mail, Judgment_no, Effective_date,
            Expire_date, Announce_agency_no, Remark)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    mydbcursor.executemany(insert_sql, batch)
    mydb.commit()
    print(f"寫入暫存表 {len(batch)} 筆")


# =========================
# 比對異動 & 寫入 PccExcellent（以暫存為主）
# =========================
def new_taxrc():
    cursor = mydb.cursor()

    # 1) 計數：pcc_excellent_tmp 對 PccExcellent 最新一筆的差異（含新統編）
    # 當日異動
    count_sql = """
    SELECT COUNT(*)
    FROM crawlerdb.pcc_excellent_tmp a  
    LEFT JOIN crawlerdb.PccExcellent t
      ON t.Party_ID = a.Party_ID
    LEFT JOIN crawlerdb.PccExcellent t2
      ON t2.Party_ID = t.Party_ID AND t2.Update_Time > t.Update_Time
    WHERE
      t2.Party_ID IS NULL  -- t 就是該 Party_ID 的最新一筆；若 t 為 NULL 代表新統編
      AND (
        t.Party_ID IS NULL OR
        NOT (
          a.Corporation_number       <=> t.Corporation_number AND
          a.Corporation_name         <=> t.Corporation_name AND
          a.Corporation_address      <=> t.Corporation_address AND
          a.Announce_agency_code     <=> t.Announce_agency_code AND
          a.Announce_agency_name     <=> t.Announce_agency_name AND
          a.Announce_agency_address  <=> t.Announce_agency_address AND
          a.Contact_person           <=> t.Contact_person AND
          a.Contact_phone            <=> t.Contact_phone AND
          a.Announce_agency_mail     <=> t.Announce_agency_mail AND
          a.Judgment_no              <=> t.Judgment_no AND
          a.Effective_date           <=> t.Effective_date AND
          a.Expire_date              <=> t.Expire_date AND
          a.Announce_agency_no       <=> t.Announce_agency_no AND
          a.Remark                   <=> t.Remark AND
        )
      );
    """
    cursor.execute(count_sql)
    cnt = cursor.fetchone()[0]
    print(f"PCC 資料本日異動數(含新統編與異動): {cnt}")

    # 2) 寫入差異到 TaxInfo（新統編 + 異動）
    insert_sql = """
    INSERT INTO crawlerdb.PccExcellent (
        Corporation_number, Corporation_name, Corporation_address, Announce_agency_code, Announce_agency_name,
        Announce_agency_address, Contact_person, Contact_phone, Announce_agency_mail, Judgment_no, Effective_date,
        Expire_date, Announce_agency_no, Remark
    )
    SELECT
        a.Corporation_number, a.Corporation_name, a.Corporation_address, a.Announce_agency_code, a.Announce_agency_name,
        a.Announce_agency_address, a.Contact_person, a.Contact_phone, a.Announce_agency_mail, a.Judgment_no, a.Effective_date,
        a.Expire_date, a.Announce_agency_no, a.Remark   
    FROM crawlerdb.pcc_excellent_tmp a
    LEFT JOIN crawlerdb.PccExcellent t
      ON t.Corporation_number = a.Corporation_number
    LEFT JOIN crawlerdb.PccExcellent t2
      ON t2.Corporation_number = t.Corporation_number AND t2.Update_Time > t.Update_Time
    WHERE
      t2.Corporation_number IS NULL
      AND (
        t.Corporation_number IS NULL OR
        NOT (
          a.Corporation_number       <=> t.Corporation_number AND
          a.Corporation_name         <=> t.Corporation_name AND
          a.Corporation_address      <=> t.Corporation_address AND
          a.Announce_agency_code     <=> t.Announce_agency_code AND
          a.Announce_agency_name     <=> t.Announce_agency_name AND
          a.Announce_agency_address  <=> t.Announce_agency_address AND
          a.Contact_person           <=> t.Contact_person AND
          a.Contact_phone            <=> t.Contact_phone AND
          a.Announce_agency_mail     <=> t.Announce_agency_mail AND
          a.Judgment_no              <=> t.Judgment_no AND
          a.Effective_date           <=> t.Effective_date AND
          a.Expire_date              <=> t.Expire_date AND
          a.Announce_agency_no       <=> t.Announce_agency_no AND
          a.Remark                   <=> t.Remark AND
        )
      );
    """
    cursor.execute(insert_sql)
    mydb.commit()

    row = cursor.fetchone()
    print(f"爬蟲狀態表本日ID新增數(預估): {row[0]} ")


# =========================
# main
# =========================

