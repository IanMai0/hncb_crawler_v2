# 稅籍資料日批管線（GCIS TaxInfo Daily Pipeline）

本專案是一套 **稅籍資料日批處理管線**，用於每日（或回補）處理政府 GCIS 稅籍 CSV，並將「新增 / 異動」資料**追加寫入主表（歷史保留）**。

核心設計目標：

* **Raw-first**：原始資料一律先完整入庫，作為可稽核依據
* **Legacy-compatible**：沿用既有 `Tmp_TaxInfo` / `TaxInfo` 表結構，不打掉重練
* **Append-only main table**：主表只新增，不覆蓋、不 upsert
* **強制核對**：raw(DATA) vs tmp 筆數不一致，批次直接失敗
* **可日批、可回補、可手動指定 CSV**

---

## 架構 流程 DB
<img width="474" height="325" alt="image" src="https://github.com/user-attachments/assets/1ba872ad-3863-4136-8281-ff36a4806a6e" />
<img width="606" height="326" alt="image" src="https://github.com/user-attachments/assets/439ce793-d3cd-45e5-8fed-1402f059ce7e" />
<img width="579" height="613" alt="image" src="https://github.com/user-attachments/assets/2e3b8329-c16d-4247-85cf-959acfb356c3" />
<img width="744" height="700" alt="image" src="https://github.com/user-attachments/assets/69e4bfeb-b628-4769-879a-1853535c94ff" />

---

## 專案結構

```
.
├── /logs                            # 程式運作 logs (紀錄用)
   ├── gcis_pipeline_20260119.log    # 日批 logs（記錄用）
   ├── gcis_pipeline_20260120.log    # 日批 logs（記錄用）
   .
   .
   .
├── /work                            # 裝載_自動下載日批檔案 (紀錄用)
   ├── BGMOPEN1_20260119_151750.zip  # 直接下載日批壓縮檔
   ├── BGMOPEN1_20260119_151750.csv  # 解壓縮後日批 csv 檔案
   ├── BGMOPEN1_20260120_151750.zip  # 
   ├── BGMOPEN1_20260120_151750.csv  # 
   .
   .
   .
├── run_daily_job_v3.py              # 日批入口（CLI / 排程用）
├── crawler_etl_v3.py                # 下載、raw 入庫、ETL
├── db_loader_v4.py                  # DB I/O 與 tmp→main merge（依 vGPT.sql）
├── batch_tax_daily_v260115.bat      # Windows 排程批次檔
├── vGPT.sql                         # raw vs tmp vs main 差異比對 SQL 設計稿
├── .env                             # 環境變數設定（不納入版控）
└── README.md
```

---
## 來源網站查詢流程說明
**前往目標網址：https://findbiz.nat.gov.tw/fts/query/QueryBar/queryInit.do?datatype=B**
![商工登記查詢_前往下載全國營業稅籍資料集](https://github.com/user-attachments/assets/83df1853-2014-410d-8e63-235456012813)

**點擊跳轉至網址, 並點擊下載 CSV 檔案：https://data.gov.tw/dataset/9400**
<img width="1773" height="980" alt="image" src="https://github.com/user-attachments/assets/24f56f36-56b9-4018-8718-62d0b2cc9b09" />

**下載至本機端目錄畫面**
![稅籍下載 zip and csv file](https://github.com/user-attachments/assets/f25c85c5-bf4c-44e2-b616-bd2e4718fba3)

---


## 整體流程說明（實際執行順序）

```
START | run_id=RUN_YYYYMMDD_HHMMSS
→ （日批）下載 ZIP → 解壓 CSV
   或
→ （回補）直接使用指定 CSV

→ tmp_rawData（新表）
   - HEADER / META / DATA 全量原封不動寫入

→ META 日期解析
→ 檔案日期驗證（嚴格 / 指定 / 跳過）

→ ETL
   - 僅從 tmp_rawData(DATA) 讀
   - 民國轉西元、型別清洗
   - 寫入舊表 Tmp_TaxInfo

→ 筆數核對（硬規則）
   - tmp_rawData(DATA) == Tmp_TaxInfo

→ 差異合併
   - Tmp_TaxInfo vs TaxInfo（最新版本）
   - 只寫入「新增 / 欄位異動」
   - TaxInfo 為 append-only 歷史表

→（可選）TaxRecord 補齊（可關閉）

→ 清空暫存表
   - tmp_rawData
   - Tmp_TaxInfo

→ END
```

---

## 各檔案職責說明

### `run_daily_job_v3.py`（日批入口 / Orchestrator）

**職責：**

* CLI 解析（是否指定 CSV）
* 產生 `run_id`
* 控制整體流程順序
* 呼叫 ETL / DB loader
* 控制 cleanup 與錯誤中止行為

**支援模式：**

* **標準日批（自動下載）**

  ```bash
  python run_daily_job_v3.py
  ```

* **指定 CSV（跳過下載/解壓）**

  ```bash
  python run_daily_job_v3.py --csv "C:\path\BGMOPEN1_YYYYMMDD_xxx.csv"
  ```

* **指定檔案日期（回補）**

  ```powershell
  $env:EXPECT_FILE_DATE="2026-01-22"
  python run_daily_job_v3.py --csv "C:\path\BGMOPEN1_20260122.csv"
  ```

* **跳過日期驗證（緊急止血）**

  ```powershell
  $env:STRICT_FILE_DATE="0"
  python run_daily_job_v3.py --csv "C:\path\BGMOPEN1.csv"
  ```

---

### `crawler_etl_v4.py`（ETL 與 raw 處理）

**職責：**

* 下載 ZIP（含 SSL fallback）
* 解壓 CSV
* 將 CSV 每一行寫入 `tmp_rawData`

  * row_type = HEADER / META / DATA
* 解析 META 日期
* 檔案日期驗證
* 從 `tmp_rawData(DATA)` 做 ETL
* 呼叫 DB loader 寫入 `Tmp_TaxInfo`

**設計原則：**

* ETL **不直接讀 CSV**
* ETL 的唯一資料來源是 `tmp_rawData`
* raw table 為第一級事實來源（source of truth）

---

### `db_loader_v4.py`（資料庫操作層）

**職責：**

* MySQL 連線（從環境變數）
* tmp_rawData：

  * insert
  * count(DATA)
  * truncate
* Tmp_TaxInfo（舊表）：

  * insert
  * count
  * truncate
* TaxInfo（主表）：

  * 差異比對
  * append-only 寫入

**差異合併邏輯（依 `vGPT.sql`）：**

* 對 `TaxInfo` 取每個 `Party_ID` 最新一筆（window function）
* 對 `Tmp_TaxInfo` 計算 row hash
* 僅在以下情況寫入 main：

  * main 無該 Party_ID（新增）
  * hash 不同（欄位異動）

---

### `.env`（環境與行為設定）

**必要 DB 設定：**

```env
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=xxx
MYSQL_PASSWORD=xxx
MYSQL_DB=crawlerdb
```

**流程控制開關：**

```env
# 日期驗證
STRICT_FILE_DATE=1
EXPECT_FILE_DATE=2026-01-22

# 停用 TaxRecord（若資料庫無此表）
DISABLE_TAXRECORD=1

# SSL 下載（必要時）
ALLOW_INSECURE_SSL=1
```

> `.env` 需搭配：

```bash
pip install python-dotenv
```

---

### `batch_tax_daily_v260115.bat`（Windows 排程）

**用途：**

* Windows Task Scheduler 呼叫用
* 建立執行環境
* 呼叫 venv Python
* 導出 log

**典型內容：**

```bat
@echo off
cd /d C:\path\to\project

call .venv\Scripts\activate

python run_daily_job_v3.py >> logs\tax_daily_%DATE%.log 2>&1
```

---

## 資料表說明（重點）

### `tmp_rawData`（新表）

* 原始 CSV 完整落地
* 可追溯、可重跑、可稽核
* 日批結束後清空(待確定)

### `Tmp_TaxInfo`（日表）

* ETL 後乾淨資料
* 不含 run_id
* 日批結束後清空

### `TaxInfo`（主表）

* 歷史保留
* append-only
* 不做 update / upsert
---
## DDL
### crawlerdb.tmp_rawData
```sql
-- crawlerdb.tmp_rawdata definition

CREATE TABLE `tmp_rawdata` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `run_id` varchar(32) NOT NULL,
  `source_url` varchar(500) NOT NULL,
  `local_zip_path` varchar(500) NOT NULL,
  `local_csv_path` varchar(500) NOT NULL,
  `downloaded_at` datetime(6) NOT NULL,
  `file_date` date DEFAULT NULL,
  `row_num` int NOT NULL,
  `row_type` enum('HEADER','META','DATA') NOT NULL,
  `c01` text,
  `c02` text,
  `c03` text,
  `c04` text,
  `c05` text,
  `c06` text,
  `c07` text,
  `c08` text,
  `c09` text,
  `c10` text,
  `c11` text,
  `c12` text,
  `c13` text,
  `c14` text,
  `c15` text,
  `c16` text,
  `loaded_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  KEY `idx_run_row` (`run_id`,`row_num`),
  KEY `idx_run_type` (`run_id`,`row_type`),
  KEY `idx_run_filedate` (`run_id`,`file_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

### crawlerdb.tmp_taxInfo
```sql
-- crawlerdb.tmp_taxinfo definition
CREATE TABLE `tmp_taxinfo` (
  `Party_ID` varchar(20) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `Party_Addr` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `Parent_Party_ID` int DEFAULT NULL,
  `Party_Name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `PaidIn_Capital` bigint DEFAULT NULL,
  `Setup_Date` date DEFAULT NULL,
  `Party_Type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Use_Invoice` char(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code` int DEFAULT NULL,
  `Ind_Name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code1` int DEFAULT NULL,
  `Ind_Name1` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code2` int DEFAULT NULL,
  `Ind_Name2` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code3` int DEFAULT NULL,
  `Ind_Name3` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Update_Time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

### crawlerdb.taxInfo
```sql
-- crawlerdb.taxinfo definition
CREATE TABLE `taxinfo` (
  `Party_ID` varchar(20) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `Party_Addr` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `Parent_Party_ID` int DEFAULT NULL,
  `Party_Name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `PaidIn_Capital` bigint DEFAULT NULL,
  `Setup_Date` date DEFAULT NULL,
  `Party_Type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Use_Invoice` char(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code` int DEFAULT NULL,
  `Ind_Name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code1` int DEFAULT NULL,
  `Ind_Name1` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code2` int DEFAULT NULL,
  `Ind_Name2` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code3` int DEFAULT NULL,
  `Ind_Name3` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Update_Time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

---
## 資料欄位說明
| 欄位英文 | 欄位中文 | 資料型態 | 範例 |
| --- | --- | --- | --- |
| party_id | 統一編號 | varchar | 72373274 |
| party_addr | 營業地址 | varchar | 南投縣中寮鄉廣福村中寮段０３０７－００１３地號 |
| parent_party_id | 總機構統一編號 | int | 23285582 |
| party_name | 營業人名稱 | varchar | 萊爾富國際股份有限公司第四六一一營業處 |
| paidin_capital | 資本額 | bigint | 5000000 |
| setup_date | 設立日期 | date | 2020-01-06 |
| party_type | 組織別名稱 | varchar | 其它 |
| use_invoice | 使用統一發票 | char(1) | Y/N |
| ind_code | 行業代號 | int | 471,112 |
| ind_name | 名稱 | varchar | 直營連鎖式便利商店 |
| ind_code1 | 行業代號1 | int | 同上 |
| ind_name1 | 名稱1 | varchar | 同上 |
| ind_code2 | 行業代號2 | int | 同上 |
| ind_name2 | 名稱2 | varchar | 同上 |
| ind_code3 | 行業代號3 | int | 同上 |
| ind_name3 | 名稱3 | varchar | 同上 |
| Update_Time | 更新時間 | datetime | 2026-01-23 14:35:08 |

---

## 驗證與稽核

* **硬核對**

  * `tmp_rawData(DATA) == Tmp_TaxInfo`
* **差異驗證**

  * tmp vs main 預期差異數 ≈ 實際寫入筆數
* **抽樣驗證**

  * 同一 Party_ID 新舊版本欄位比對

---

## 注意事項（工程現實）

* MySQL 需 **8.0+**（使用 window function）
* 若 `TaxInfo(Party_ID, Update_Time)` 無索引，效能會受限
* SSL 憑證問題屬政府站台問題，已提供 fallback，但 log 會警告

---

## 運行畫面
<img width="1799" height="633" alt="image" src="https://github.com/user-attachments/assets/13f34772-eeeb-49db-8a81-b02bb84d0eca" />


---
# crawler_etl 系列版本差異說明（v1 → v3）

本文說明稅籍日批管線中的版本演進差異**，目的在於：

- 說清楚每一版「實際在幹嘛」
- 解釋為何需要升級到 v3
- 哪些設計是「刻意為之」，而非多此一舉

---
## 版本特性對比總覽

| 面向 | crawler_etl.py (v1) | crawler_etl_v2.py (v2) | crawler_etl_v3.py (v3) |
| --- | --- | --- | --- |
| **模組定位** | 腳本型 ETL | 半 pipeline ETL | 正式 ETL 模組 |
| **資料輸入來源** | 直接讀 CSV | 直接讀 CSV | **僅從 tmp_rawData 讀取** |
| **Raw Data 落地** | 無 | 無 | `tmp_rawData` |
| **Raw-first 設計** | 無 | 無 | 具備核心原則 |
| **CSV 處理方式** | 一次性讀取 | 一次性讀取 | **逐行寫入 raw table** |
| **資料轉換邏輯** | Inline 處理 | 抽成 function | 完整 ETL pipeline |
| **民國轉西元** | Inline | Inline / function | 專用轉換函式 |
| **欄位正規化** | 零散處理 | 部分集中 | 系統化集中處理 |
| **ETL 輸出目標** | Tmp_TaxInfo | Tmp_TaxInfo | **Tmp_TaxInfo（舊表）** |
| **與主表耦合** | 高（隱性） | 中 | 無完全解耦 |
| **資料筆數驗證** | 無 | 無 | 有 raw(DATA) ↔ tmp |
| **錯誤中止機制** | 幾乎無 | 部分 | **強制 fail-fast** |
| **可重跑能力** | 無 | 無 | 有 |
| **可回補 CSV** | 無 | 部分 | 有 |
| **資料可稽核性** | 無 | 無 | 有 |
| **生產可用性** | 高風險 | 中風險 | 建議使用 |
---
# run_daily_job 系列版本差異說明（v1 → v3）

## GCIS 稅籍資料處理腳本：版本演進分析報告
整體演進核心在於從**單一任務處理**轉向**多模式支援**，並最終實現**資料全流程溯源**。

## 版本特性對比表

| 特性 | v1 (基礎版) | v2 (CLI 優化版) | v3 (完整溯源版) |
| --- | --- | --- | --- |
| **主要定位** | 單一排程入口 | 支援多模式與差異寫入 | 強化 Raw Data 存證與 ETL 核對 |
| **資料寫入邏輯** | `upsert_latest` (覆蓋/更新) | `insert_diff` (僅寫入異動) | 差異寫入 + `TaxRecord` 補齊 |
| **模式切換** | 僅支援自動下載 | `daily` / `from_csv` 子命令 | 參數化支援與除錯開關 |
| **Raw Data 處理** | 直接解析 | 直接解析 | **先存入 `tmp_rawData` 表再 ETL** |
| **清理機制** | 無明確清空 | 執行前後強制清空 | 支援 `--no-cleanup` 模式 |

---
## 重點關鍵 Function 簡介（`crawler_etl_v3.py` + `run_daily_job_v3.py`）
以下挑的是**真正控制資料生命線**（下載/存證/ETL/驗證/日批清理/merge）的關鍵 Function

## 1) `crawler_etl_v3.py`（下載/存證(raw)/ETL 到 legacy tmp）

### A. `_download_file(url, dst_path, *, logger, timeout, max_retry) -> None`

**定位：下載層的「抗爛網路」核心**（含 SSL fallback）

* **做什麼**

  * 使用 `requests` 下載 ZIP。
  * 先嘗試 `verify=certifi.where()`（正常驗證）
  * 若遇到鬼問題：`Missing Subject Key Identifier` → 會 retry
  * 最後一輪會 fallback 到 `verify=False`（不驗證 SSL）讓流程能跑下去（並打 warning）

* **logs 看到的行為**

  * `verify=certifi` 失敗數次
  * 最後 `verify=INSECURE(verify=False)` 成功（InsecureRequestWarning）

* **風險/注意**

  * `verify=False` 是「可用但不優雅」：能跑，但安全性下降（容易被中間人攻擊）。
  * 註解：真要根治：要處理對方站台憑證鏈或改走政府/備援鏡像源（但這是另一支線任務）。

---

### B. `download_and_extract(work_dir: str) -> tuple[str, str, datetime]`

**定位：把遠端 ZIP 變成本地「可追溯」檔案（含時間戳命名）**

* **做什麼**

  * 產生時間戳 `BGMOPEN1_YYYYmmdd_HHMMSS.zip/.csv`
  * 呼叫 `_download_file()` 下載 ZIP
  * 解壓縮，找出 zip 內的 `BGMOPEN1.csv`，搬到帶時間戳的新檔名
* **回傳**

  * `zip_path`, `csv_path`, `downloaded_at`

---

### C. `csv_to_tmp_rawdata(conn, run_id, *, source_url, local_zip_path, local_csv_path, downloaded_at) -> date`

**定位：raw 存證層的核心（要求「raw 一律先入 DB」就在這裡落地）**

* **做什麼**

  * 逐行讀 CSV，把每行都寫入 **`crawlerdb.tmp_rawData`**
  * 分類 `row_type`：

    * 第 1 行：`HEADER`
    * 第 2 行：`META`（並解析 `file_date`）
    * 其後：`DATA`
  * 每行固定補齊到 16 欄（`c01..c16`），避免欄數不齊炸裂
  * 寫入 DB 使用 `insert_tmp_rawdata()`（在 loader 檔）

* **回傳**

  * `file_date`（從 META 第 2 行解析出來的日期）

* **為什麼它重要**

  * 考量後續資料工程要做「可追溯」「出事可回放」「ETL 可驗證」，沒有 raw 存證則只能憑運氣。

---

### D. `validate_file_date_or_raise(source_file_date: date) -> None`

**定位：日批完整性守門員（防止你把昨天檔案當今天跑）**

* **預設行為（嚴格）**

  * 若有 `EXPECT_FILE_DATE=YYYY-MM-DD`：用它比
  * 否則：拿 `source_file_date` 比對今天日期 `today`
  * 不符就 `raise RuntimeError`

* **可控開關**

  * `STRICT_FILE_DATE=0` → 直接略過（但會 warning）
  * `--skip-date-check`（run_daily_job v3 參數）本質上也是把嚴格檢查關掉

---

### E. `raw_to_legacy_tmp_etl(conn, run_id, source_file_date) -> int`

**定位：把 raw(DATA) 轉成舊表 `Tmp_TaxInfo` 的 ETL 核心**

* **做什麼**

  1. 從 `tmp_rawData` 抓 `row_type='DATA'`
  2. 逐列做欄位正規化/轉型（空白、全形空白、民國日期、純數字行業碼、資本額 int）
  3. 把清洗後資料寫到 **舊表** `crawlerdb.Tmp_TaxInfo`（用 `insert_legacy_tmp_taxinfo`）

* **回傳**

  * 寫入 `Tmp_TaxInfo` 的筆數（clean_cnt）

* **「核對」**

  * `tmp_rawData(DATA)` 的筆數要等於 `Tmp_TaxInfo` 筆數
  * 這件事是在 `run_daily_job_v3.py` 裡做最後核對 log 的

---

## 2) `run_daily_job_v3.py`（日批 orchestrator / CLI 入口）

### A. `_parse_args() -> argparse.Namespace`

**定位：CLI 行為開關總控（你要的「可針對已下載 CSV 操作」就在這裡）**

支援參數：

* `--work-dir`：工作目錄（下載/解壓/臨時檔）
* `--csv`：指定本地 CSV → **跳過下載/解壓**
* `--skip-date-check`：略過日期檢查（等同關 strict）
* `--no-cleanup`：不清空 tmp 表（除錯用，正式日批不要開）

> logs 裡 `python run_daily_job_v3.py daily ...` 報錯是正常的：目前 v3 **沒有 subcommand**（daily/from_csv），只有用 `--csv` 做模式切換。

---

### B. `_make_run_id(now: datetime) -> str`

**定位：產生 run_id（追溯鏈的 key）**

* 格式：`RUN_YYYYmmdd_HHMMSS`
* 這個會一路寫進 `tmp_rawData.run_id`，也會出現在 log（便於追查）

---

### C. `main() -> None`

**定位：整條 pipeline 的 orchestrator（控制流程順序、DB清理策略、核對、merge）**

它做的事情，照執行順序（對應你想保留的流程）：

1. **產生 run_id + log START**
2. 若 `--skip-date-check` → 設定 `STRICT_FILE_DATE=0`
3. **連線 MySQL**
4. **日批前清空**

   * `truncate_tmp_rawdata(conn)`
   * `truncate_legacy_tmp_taxinfo(conn)`
5. **資料來源分歧**

   * 無 `--csv`：走 `download_and_extract(work_dir)`
   * 有 `--csv`：直接使用該 CSV（跳過下載/解壓）
6. **raw 入庫**

   * `csv_to_tmp_rawdata(...)` → 回 `file_date`
7. **日期檢查**

   * `validate_file_date_or_raise(file_date)`
8. **ETL 入 legacy tmp**

   * `raw_to_legacy_tmp_etl(...)` → `clean_cnt`
9. **核對 raw vs tmp**

   * `count_rawdata_data_rows(run_id)` vs `count_legacy_tmp_taxinfo()`
10. **tmp -> main 差異寫入**

* `merge_diff_tmp_to_main_taxinfo(conn)`

11. **TaxRecord（可停用）**

* 若 `DISABLE_TAXRECORD=1`：跳過（你 logs 就是這樣）

12. **日批後清空（預設會做）**

* 除非 `--no-cleanup`

13. log END

# 核心點

* `crawler_etl_v3.py` 負責：**下載/解壓 → raw 存證 → ETL 到 tmp**
* `run_daily_job_v3.py` 負責：**日批流程編排 + CLI 模式切換 + 核對 + merge + 清理策略**

---
未來淺在優化方向：

* 建立 `TaxInfo_latest` 快取表
* 差異率異常告警
* pipeline 監控與報表化

---
