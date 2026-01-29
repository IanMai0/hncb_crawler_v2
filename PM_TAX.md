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
   ├── gcis_pipeline_20260121.log    # 日批 logs（記錄用）
   ├── gcis_pipeline_20260122.log    # 日批 logs（記錄用）
   ├── gcis_pipeline_20260123.log    # 日批 logs（記錄用）
├── /work                            # 裝載_自動下載日批檔案
   ├── BGMOPEN1_20260119_151750.zip  # 直接下載日批壓縮檔
   ├── BGMOPEN1_20260119_151750.csv  # 解壓縮後日批 csv 檔案
   ├── BGMOPEN1_20260120_151750.zip  # 
   ├── BGMOPEN1_20260120_151750.csv  # 
   ├── BGMOPEN1_20260121_151750.zip  # 
   ├── BGMOPEN1_20260121_151750.csv  # 
   ├── BGMOPEN1_20260122_151750.zip  # 
   ├── BGMOPEN1_20260122_151750.csv  # 
   ├── BGMOPEN1_20260123_151750.zip  # 
   ├── BGMOPEN1_20260123_151750.csv  # 
├── run_daily_job_v3.py              # 日批入口（CLI / 排程用）
├── crawler_etl_v3.py                # 下載、raw 入庫、ETL
├── db_loader_v4.py                  # DB I/O 與 tmp→main merge（依 vGPT.sql）
├── batch_tax_daily_v260115.bat      # Windows 排程批次檔
├── vGPT.sql                         # raw vs tmp vs main 差異比對 SQL 設計稿
├── .env                             # 環境變數設定（不納入版控）
└── README.md
```

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

### `crawler_etl_v3.py`（ETL 與 raw 處理）

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

### `Tmp_TaxInfo`（舊暫存表）

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
CREATE TABLE IF NOT EXISTS `crawlerdb`.`tmp_rawData` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

  `run_id` VARCHAR(32) NOT NULL,
  `source_url` VARCHAR(500) NOT NULL,
  `local_zip_path` VARCHAR(500) NOT NULL,
  `local_csv_path` VARCHAR(500) NOT NULL,

  `downloaded_at` DATETIME(6) NOT NULL,
  `file_date` DATE NULL,

  `row_num` INT NOT NULL,
  `row_type` ENUM('HEADER','META','DATA') NOT NULL,

  `c01` TEXT NULL,
  `c02` TEXT NULL,
  `c03` TEXT NULL,
  `c04` TEXT NULL,
  `c05` TEXT NULL,
  `c06` TEXT NULL,
  `c07` TEXT NULL,
  `c08` TEXT NULL,
  `c09` TEXT NULL,
  `c10` TEXT NULL,
  `c11` TEXT NULL,
  `c12` TEXT NULL,
  `c13` TEXT NULL,
  `c14` TEXT NULL,
  `c15` TEXT NULL,
  `c16` TEXT NULL,

  `loaded_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`id`),
  KEY `idx_run_row` (`run_id`, `row_num`),
  KEY `idx_run_type` (`run_id`, `row_type`),
  KEY `idx_run_filedate` (`run_id`, `file_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### crawlerdb.tmp_taxInfo
```sql
CREATE TABLE IF NOT EXISTS `crawlerdb`.`tmp_taxInfo` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

  `run_id` VARCHAR(32) NOT NULL,
  `source_file_date` DATE NOT NULL,
  `row_num` INT NOT NULL,

  `party_addr` VARCHAR(300) NULL,
  `party_id` VARCHAR(20) NOT NULL,
  `parent_party_id` VARCHAR(20) NULL,
  `party_name` VARCHAR(200) NULL,
  `paidin_capital` BIGINT NULL,
  `setup_date` DATE NULL,
  `party_type` VARCHAR(50) NULL,
  `use_invoice` CHAR(1) NULL,

  `ind_code` VARCHAR(20) NULL,
  `ind_name` VARCHAR(100) NULL,
  `ind_code1` VARCHAR(20) NULL,
  `ind_name1` VARCHAR(100) NULL,
  `ind_code2` VARCHAR(20) NULL,
  `ind_name2` VARCHAR(100) NULL,
  `ind_code3` VARCHAR(20) NULL,
  `ind_name3` VARCHAR(100) NULL,

  `row_hash` CHAR(64) NOT NULL,
  `etl_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_run_row` (`run_id`, `row_num`),
  KEY `idx_run_party` (`run_id`, `party_id`),
  KEY `idx_party_hash` (`party_id`, `row_hash`),
  KEY `idx_file_date` (`source_file_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### crawlerdb.taxInfo
```sql
CREATE TABLE IF NOT EXISTS `crawlerdb`.`taxInfo` (
  `party_id` VARCHAR(20) NOT NULL,

  `party_addr` VARCHAR(300) NULL,
  `parent_party_id` VARCHAR(20) NULL,
  `party_name` VARCHAR(200) NULL,
  `paidin_capital` BIGINT NULL,
  `setup_date` DATE NULL,
  `party_type` VARCHAR(50) NULL,
  `use_invoice` CHAR(1) NULL,

  `ind_code` VARCHAR(20) NULL,
  `ind_name` VARCHAR(100) NULL,
  `ind_code1` VARCHAR(20) NULL,
  `ind_name1` VARCHAR(100) NULL,
  `ind_code2` VARCHAR(20) NULL,
  `ind_name2` VARCHAR(100) NULL,
  `ind_code3` VARCHAR(20) NULL,
  `ind_name3` VARCHAR(100) NULL,

  `row_hash` CHAR(64) NOT NULL,

  `source_file_date` DATE NOT NULL,
  `last_run_id` VARCHAR(32) NOT NULL,

  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`party_id`),
  KEY `idx_source_date` (`source_file_date`),
  KEY `idx_row_hash` (`row_hash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

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
## 版本開發差異
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
## 一、crawler_etl.py（v1）

### 版本定位
**腳本型 ETL（Script-style ETL）**

這是最初版本，重點在於「能跑就好」，不是完整 pipeline。

### 實際行為
- 下載 ZIP
- 解壓 CSV
- 直接用 `csv.reader()` 讀取 CSV
- inline 清洗資料（空白、民國轉西元、數字）
- 直接寫入舊版 `Tmp_TaxInfo`
- 接著用複雜 SQL 比對 `Tmp_TaxInfo` 與 `TaxInfo`

### 核心問題
1. **沒有 raw data 概念**
   - 原始資料未落地
   - ETL 出錯後無法回溯、無法稽核

2. **ETL、DB、流程耦合**
   - 所有邏輯混在一個檔案
   - 無法單獨測試或重跑某一階段

3. **SQL 過度複雜**
   - 使用 self-join + `t2 IS NULL` 取得最新版本
   - 效能與正確性高度依賴 Update_Time

4. **不可重跑**
   - CSV 只存在記憶體中
   - 失敗後只能重新下載

### 一句話總結
> v1 能跑，但不能信、不能查、不能重來。

---

## 二、crawler_etl_v2.py（v2）

### 版本定位
**開始 pipeline 化，但基礎尚未補齊**

這一版是「已經知道問題在哪，但還在打好地基」。

### 相較 v1 的改善
1. **開始拆分 ETL function**
   - 清洗邏輯不再全部 inline
2. **嘗試整理流程順序**
   - 下載 / ETL / DB 寫入概念上分開
3. **開始重新思考 SQL**
   - 討論 latest version、hash 比對、效能問題

### 仍然存在的缺陷
1. **仍然沒有 raw data**
   - ETL 仍直接從 CSV → Tmp_TaxInfo
2. **無法驗證 ETL 是否漏資料**
   - 缺少 raw vs tmp 的筆數核對
3. **CSV 仍是一次性輸入**
   - 失敗仍需重跑下載
4. **流程保護不足**
   - 有些錯誤只記錄 log，未 hard fail

---

## 三、crawler_etl_v3.py（v3）

### 版本定位
**可稽核、可回補、可懷疑的正式 ETL 模組**
這一版是完整 pipeline 思維落地後的成果。

## v3 核心設計原則

### 1️ Raw-first（第一級事實來源）
- CSV 每一行（HEADER / META / DATA）
- **全部先寫入 `tmp_rawData`**
- raw table 是唯一可信來源（source of truth）

### 2 強制不變量（Hard Invariant）
- `tmp_rawData(DATA)` **必須等於** `Tmp_TaxInfo`
- 不相等 → 批次直接失敗

這是 v3 與前兩版最大的「質變」。

### 3 職責完全分離
- `crawler_etl_v3.py`
  - 不知道 `TaxInfo` 的存在
  - 不負責 tmp → main merge
- ETL 的唯一輸出：**舊版 `Tmp_TaxInfo`**

### 4️ 支援指定 CSV（回補 / 除錯）
```bash
python run_daily_job_v3.py --csv path/to/file.csv
```

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

## 詳細版本差異介紹

### 1. v1：基礎自動化流程

這是最初的腳本，旨在完成最基本的排程任務。

* **核心邏輯**：流程為線性執行「下載 -> 入庫 -> 驗證 -> Merge」。
* **驗證方式**：簡單對比臨時表中的 `raw_data_cnt` 與 `clean_cnt` 筆數。
* **侷限性**：缺乏靈活性，若下載失敗，難以針對既有檔案進行手動補檔。

### 2. v2：多模式切換與增量邏輯

此版本引入了命令列介面（CLI），大幅提升了維運的靈活性。

* **新增模式**：
* `daily`：自動化下載並處理。
* `from_csv`：允許指定本地 CSV 路徑，跳過下載步驟。


* **資料存檔邏輯**：改為 `insert_diff_tmp_to_main_taxinfo`，僅將與主表不同的新資料寫入，旨在保留所有歷史異動紀錄，而不僅僅是更新最新狀態。
* **自動管理**：增加 `truncate_tmp_tables` 函數，確保每次執行前暫存空間皆為乾淨狀態。

### 3. v3：資料品質保障與完整溯源

目前最嚴謹的版本，強化了資料在進入主表前的「存證」與「核對」過程。

* **Raw Data 實體化**：
* 新增 `csv_to_tmp_rawdata` 步驟，先將原始 CSV 全文寫入資料庫 `tmp_rawData` 表，保留原始軌跡。
* 隨後才執行 `rawdata_to_legacy_tmp_taxinfo` 進行 ETL 轉換。


* **強化驗證機制**：明確執行 `count_rawdata_data_rows` 與 `count_legacy_tmp_taxinfo` 的交叉核對。
* **關聯表維護**：額外呼叫 `ensure_taxrecord_for_new_party_ids`，確保新出現的統一編號在關聯表中被正確建立。
* **開發與除錯優化**：
* 支援 `.env` 環境變數讀取。
* 提供 `--skip-date-check` 跳過嚴格日期檢查。
* 提供 `--no-cleanup` 選項，方便開發者在執行後檢查暫存表內容。

## 小結

從 v1 到 v3，腳本的演進反映了對資料準確性的追求：從「能跑就好」進化到「具備追溯能力的自動化流水線」。對於生產環境，建議統一使用 v3 以確保資料完整性。
---

## 總結

這是一套 **可日批、可回補、可稽核、可懷疑** 的資料管線，
設計重點不是「跑得快」，而是「跑錯一定會被抓到」。

未來淺在優化方向：

* 建立 `TaxInfo_latest` 快取表
* 差異率異常告警
* pipeline 監控與報表化

---
