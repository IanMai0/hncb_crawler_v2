# HNCB Crawler v2 核心網爬架構總覽
本文件總攬記錄了基於 `lab_crawler` 目錄的新型網爬架構，涵蓋五大業務模組、核心技術專題、資料庫 DDL 以及自動化工具箱。
---
## 核心總表

| 來源模組 | 模式 | 狀況 | 調度層 | 轉換層 | 資料層 | MEMO |
| :--- | :---: | :---: | :--- | :--- | :--- | :--- |
| [**稅籍**](#1-稅籍模組-tax) | 日批 | 異常處理中 | `tax_daily_v3_純稅籍.py` | `tax_daily_v3_純稅籍.py` | `tax_daily_v3_純稅籍.py` | 三位一體 |
|                            | 日批 | 異常處理中 | `run_daily_job_v3.py` | `crawler_etl_v3.py` | `db_loader_v4.py` | - |
| [**國貿局**](#2-國貿局模組-trade-bureau) | 手動 | 上線 | `lab_250930v3_模組化版本.py` | `lab_250930v3_模組化版本.py` | `None` | - |
| [**工廠**](#3-工廠模組-factory) | 月批 | 上線 | `lab_flow_control.py` | `lab_factory_etl_v5.py` | `None` | - |
| [**商工**](#gcis-商工_友善列印) | 手動 | 未上線 | `gcis_snapshot_main.py` | `None` | `None` | 商工畫面快照功能 |
| [**商工**](#gcis-商工) | 手動 | 未上線 | `tax_daily_v3_純GCIS.py` | `None` | `None` | - |
| [**商工公司**](#5-gcis-商工公司模組-gcis-company) | 手動 | 上線 | `lab_批次處裡_單元測試版本_251007.py` | `lab_批次處裡_單元測試版本_251007.py` | `None` | - |
| [**商工商業**](#4-gcis-商工商業模組-gcis-business) | 手動 | 上線 | `lab_批次處裡_單元測試版本_251007.py` | `lab_批次處裡_單元測試版本_251007.py` | `None` | - |

---
## 🏗️ 核心業務模組
本架構針對不同來源設計了高度模組化的爬蟲引擎：
## 1. 稅籍模組 (Tax)
### 主要目錄
`hncb_crawler\自動化網爬架構\lab_crawler\優化後空間\稅籍`
### 主要程式分析
| 程式名稱 | 角色 | 重點描述 |
| :--- | :--- | :--- |
| **`run_daily_job_v3.py`** | **調度層** | 負責日批流程控制，包含 CLI 參數解析、日誌管理、以及協調下載、ETL 與入庫的順序。 |
| **`crawler_etl_v3.py`** | **轉換層** | 處理 ZIP 下載、解壓縮、CSV 讀取，以及將原始資料轉換為標準格式 (ETL)。 |
| **`db_loader_v4.py`** | **資料層** | 專責資料庫操作，包含 Raw Data 寫入、Tmp 表清除、以及核心的 **差異比對 (Merge)** 邏輯。 |
### 三層架構拆解
#### 1. 調度層 (Orchestration)
*   **Source**: `run_daily_job_v3.py`
*   **核心功能**:
    *   **CLI 介面**: 支援 `--csv` (跳過下載)、`--no-cleanup` (保留暫存表) 等參數。
    *   **流程控制**: `Truncate` (清暫存) -> `Download` (下載) -> `Load Raw` (入庫) -> `ETL` (轉換) -> `Merge` (寫入主表)。
    *   **交易管理**: 確保每一階段成功才進入下一階段，失敗則回滾或報錯。
#### 2. 轉換層 (Transformation)
*   **Source**: `crawler_etl_v3.py`
*   **核心功能**:
    *   `download_and_extract()`: 處理 `BGMOPEN1.zip` 下載與解壓。
    *   `csv_to_tmp_rawdata()`: 讀取 CSV 並解析 META 行日期，原封不動寫入 `tmp_rawData`。
    *   `rawdata_to_legacy_tmp_taxinfo()`: 清洗資料（全形轉半形、日期正規化），轉入 `Tmp_TaxInfo`。
#### 3. 資料層 (Data Layer)
*   **Source**: `db_loader_v4.py`
*   **核心功能**:
    *   `insert_tmp_rawdata()`: 批量寫入原始資料。
    *   `merge_diff_tmp_to_main_taxinfo()`: **關鍵邏輯**。使用 SQL Window Function 取出最新版，並透過 MD5 Hash 比對 `Tmp` 與 `Main` 表，僅寫入異動或新增資料 (Append-only)。
### MEMO
*   此模組為專案的 **最新完整三層架構**。
### 未來優化建議
1.  **MD5 效能**: 目前 MD5 計算在 SQL 端執行，若 DB 負載過重，可移至 Python 端 `crawler_etl_v3` 計算 Hash 後再入庫。
2.  **數據異常狀況處理：**
---
## 2. 國貿局模組 (Trade Bureau)
### 主要目錄
`hncb_crawler\自動化網爬架構\lab_crawler\優化後空間\國貿局`
### 主要程式
| 程式名稱 | 角色 | 重點描述 |
| :--- | :--- | :--- |
| **`lab_250930v3_模組化版本.py`** | **混合** | 同時包含調度迴圈、爬蟲邏輯 (AJAX 繞過)、與 CSV 匯出邏輯。 |
| *(缺資料層)* | - | 目前使用內嵌的 `BucketCsvExporter` 寫檔案，未對接 DB。 |
### 三層架構拆解 (Source: `lab_250930v3_模組化版本.py`)
#### 1. 調度層 (Orchestration)
*   **Source**: `main()` 函式與 `toolbox` 類別
*   **核心功能**:
    *   `load_target_ids()`: 讀取目標統編清單。
    *   `main()`: 執行主迴圈，控制批次大小 (Batch Size) 與進度 Log。
    *   `hist_done` / `sess_done`: 維護已完成名單，支援斷點續跑。
#### 2. 轉換層 (Transformation)
*   **Source**: `tradeAdmin` 類別
*   **核心功能**:
    *   `initialize()`: 取得 `verifySHidden` Token，繞過簡易驗證。
    *   `get_basicData()` / `get_gradeData()`: 解析 JSON 回傳值，處理空值 (`_safe_str`)。
    *   `normalize_band()`: 將實績級距代碼 (A, B, C...) 轉換為可讀的數值範圍 (>=10, >=9,<10)。
#### 3. 資料層 (Data Layer)
*   **Source**: `BucketCsvExporter` 類別
*   **核心功能**:
    *   `add_basic()` / `add_grade()`: 將資料暫存於 List Buffer。
    *   `flush()` / `export_if_due()`: 定時或定量將 Buffer 寫入 CSV 檔案。
    *   **缺口**: 無 SQL 寫入邏輯。
### MEMO
*   `HttpClient` 封裝得宜，具備 Session 重建 (`rebuild`) 與 Retry 機制，網路層穩定。
*   採用 AJAX 接口直接獲取 JSON，避開了傳統 HTML 解析的複雜度。
*   除了 AJAX 開發路線以外, 另有一條路線走 OCR + GenAI 解圖形驗證碼 > 執行網爬。
### 未來優化建議
1.  **新增 DB Loader**: 參考稅籍模組，建立 `db_loader_trade.py`，將 `BucketCsvExporter` 替換為資料庫寫入器。
2.  **參數化配置**: 目前輸入/輸出路徑寫死在 `main`，應改用 `argparse` 傳入。
---
## 3. 工廠模組 (Factory)
### 主要目錄
`hncb_crawler\自動化網爬架構\lab_crawler\舊版code\factory`
### 主要程式分析
| 程式名稱 | 角色 | 重點描述 |
| :--- | :--- | :--- |
| **`lab_flow_control.py`** | **調度層** | 控制下載與執行流程，但結構較原始（巢狀 Try-Except）。 |
| **`lab_factory_etl_v5.py`** | **轉換層** | 包含完整的資料清洗與正規化邏輯，採 OOP 設計。 |
| *(缺資料層)* | - | `Output` 類別中的 `output_data_to_DB` 方法為空實作。 |
### 三層架構拆解
#### 1. 調度層 (Orchestration)
*   **Source**: `lab_flow_control.py`
*   **核心功能**:
    *   `download_and_extract_zip()`: 下載檔案（缺乏 Retry 機制）。
    *   **流程控制**: 使用多層巢狀 `try...except` 進行錯誤捕捉，可讀性與維護性較差。
    *   **路徑硬編碼**: 包含 `C:/Users/wits/...` 絕對路徑。
#### 2. 轉換層 (Transformation)
*   **Source**: `lab_factory_etl_v5.py`
*   **核心功能**:
    *   `DataPreprocessor`: 處理日期轉換 (ROC -> AD)、全形轉半形。
    *   `DataCleaner`: 過濾 PUA 特殊字元（政府資料常見問題）、HTML Unescape。
    *   `DataAnomalyReporter`: 根據規則標記異常資料 (E01~E11)。
#### 3. 資料層 (Data Layer)
*   **Source**: `lab_factory_etl_v5.py` -> `Output` 類別
*   **核心功能**:
    *   `output_data_to_csv()`: 寫入 CSV。
    *   `output_data_to_DB()`: **未實作 (pass)**。
### MEMO
*   ETL 邏輯（轉換層）為當前較為完整的部分，OOP 結構清晰，特別針對 PUA 字元進行細膩處理。
*   調度層與資料層則是主要弱點，暫時缺少呼叫的調度層與資料寫入層。
### 未來優化建議
1.  **重寫開發調度層**: 廢棄 `lab_flow_control.py`，改用類似 `run_daily_job` 的標準架構。
2.  **實作 DB 入庫 資料層**: 補完 `output_data_to_DB`，對接 `crawlerdb`。
3.  **移除絕對路徑**: 將路徑改為相對路徑或配置檔讀取。
---
## 4. GCIS 商工商業模組 (GCIS Business)
### 主要目錄
`hncb_crawler\自動化網爬架構\lab_crawler\優化後空間\GCIS`
### 主要程式分析
| 程式名稱 | 角色 | 重點描述 |
| :--- | :--- | :--- |
| **`lab_批次處裡_單元測試版本_251007.py`** | **混合** | 單一大檔 (Monolithic)。包含 AWS IP 切換、API 爬蟲、ETL 與檔案寫入。 |
| *(缺資料層)* | - | 依賴 `_upsert_row` 直接對 CSV 進行讀寫操作。 |
### 三層架構拆解 (Source: `lab_..._251007.py`)
#### 1. 調度層 (Orchestration)
*   **Source**: `run_crawler_business_info` / `run_crawler_business_items` / `SwitchIP`
*   **核心功能**:
    *   `SwitchIP`: 負責監控 AWS EC2 IP 狀態並執行 EIP 切換。
    *   `run_crawler_*`: 讀取目標 CSV，迭代統編，控制錯誤次數 (`max_error`)。
#### 2. 轉換層 (Transformation)
*   **Source**: `ETl` 類別 -> `crawler_business_*`
*   **核心功能**:
    *   `crawler_business_info()`: 呼叫 API 取得商業基本資料 (應用一/三)。
    *   `parse_business_items()`: **特色邏輯**。解析中文營業項目代碼（如「一0、」「十一、」），拆解為代碼與名稱清單。
    *   `strQ2B()`: 全形轉半形工具。
#### 3. 資料層 (Data Layer)
*   **Source**: `ETl._upsert_row`
*   **核心功能**:
    *   **CSV Upsert**: 每次寫入前讀取整個 CSV -> 檢查統編是否存在 -> Update/Append -> 寫回 CSV。
    *   **效能瓶頸**: 此方式在資料量大時 I/O 成本極高，且無並發安全性。
### MEMO
*   營業項目的解析邏輯 (`parse_business_items`) 非常實用，解決了中文混雜數字的拆解難題。
*   `_upsert_row` 是最大的效能致命傷。
### 未來優化建議
1.  **廢除 CSV Upsert**: 改用 `tmp_table` 批量寫入 + SQL Merge 模式。
2.  **拆分模組**: 將 `SwitchIP` 獨立為 `lib.aws`，`GcisClient` 獨立為 `lib.network`。
---
## 5. GCIS 商工公司模組 (GCIS Company)
### 主要目錄
`hncb_crawler\自動化網爬架構\lab_crawler\優化後空間\GCIS`
### 主要程式分析
同上，與商業模組共用同一支 `lab_批次處裡_單元測試版本_251007.py`。
### 三層架構拆解
#### 1. 調度層 (Orchestration)
*   **Source**: `run_crawler_company_info`
*   **核心功能**:
    *   與商業模組類似，但讀取的是公司專用的目標清單。
    *   `export_pending_ids()`: 比對 output 與 input，輸出未完成名單。
#### 2. 轉換層 (Transformation)
*   **Source**: `ETl.crawler_company_info`
*   **核心功能**:
    *   `fetch_info_c()`: 呼叫公司 API。
    *   **狀態判斷**: 判斷回傳是否為空，或欄位缺失過多 (`sum(無資料) >= 7`)，標記為「疑似商號」或「統編錯誤」。
    *   **欄位映射**: 將 API 的英文欄位 (e.g., `Company_Setup_Date`) 映射為中文欄位 (`核准設立日期`)。
#### 3. 資料層 (Data Layer)
*   **Source**: `ETl._upsert_row` (共用)
*   **核心功能**:
    *   同商業模組，依賴低效的 CSV 讀寫更新。
### MEMO
*   公司與商業模組混在一起，導致代碼龐大可能較為難以維護。
*   對於「無資料」或「異常」的判斷邏輯（如 `state` 標記）暫時寫死在爬蟲流程中。
### 未來優化建議
1.  **邏輯分離**: 將公司 (`Company`) 與商業 (`Business`) 的 ETL 類別拆開，因為兩者的欄位定義完全不同。
2.  **新型態架構開發**：調度層 > 轉換層 > 資料層。
3.  **相關耦合性議題處理**：
4.  **異常處理標準化**: 將「疑似商號」、「無資料」等狀態碼標準化，並記錄到 DB 的狀態欄位，而非混在資料欄位中。
---
## 🏗️ 爬蟲框架與系統架構
採用「前後端解耦」與「模組化 Client」設計，確保核心擷取邏輯與業務流控制分離。
- **AJAX & API 擷取**：針對動態加載頁面，直接模擬 XHR/AJAX 請求，繞過繁瑣 DOM 解析。
    - 參考實作：[gcis.py:L479](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/gcis.py#L479) (`GcisClient._get`)
- **前後端分離/解耦**：模組化 Client (如 `GcisClient`) 僅負責數據擷取，透過 Python 原生字典進行傳輸，方便整合至不同的資料介面 (FastAPI/Flask/Django)。
- **自動化檔案載入**：具備全自動 ZIP 下載、解壓至 CSV 並轉存資料庫的無人值守功能。
    - 參考實作：[run_daily_job_v3.py:L116](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/優化後空間/稅籍/run_daily_job_v3.py#L116)
---
## 🛡️ 核心技術與專題
### 🕵️ 反爬蟲功能 (Anti-Anti-Crawling)
整合於 [toolBox.py](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/toolBox.py) 與各模組中。
- **動態換 IP**: 介接 AWS SDK，具備 `switchIP` 類別，可自動切換 EC2 EIP。
- **每日配額**: `DailyQuota` 持久化計數，限制每日 IP 切換次數，避免觸發表層封鎖。
- **智慧重試**: 針對 `RateLimitError` 與 `BlockedError` 實施指數級退避 (Exponential Backoff)。
### 🔄 批次處理與 ETL
- **高吞吐入庫**: 利用 `executemany` 與 `MySQL LOAD DATA` 概念進行大規模數據交換。
- **資料分流**: 驗證失敗的資料自動標註錯誤代碼（如 E01, E07）並匯入 `_error_log`。
- **格式歸一化**: [DataPreprocessor](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/factory_etl.py#L47) 負責處理複雜的代碼拆分。
### 📊 Logs 與監控
- **集中日誌**: 每個作業具備獨立的 `.log` 文件與 Console 輸出。
- **異常通知**: 整合 Telegram Bot API，當執行嚴重中斷或發現大量異常時主動推播。
---
## 🛡️ 反爬蟲策略 (Advanced Anti-Scraping)
為了應對高度嚴格的封鎖機制，本專案實作了多維度的繞過策略。
### 1. GenAI 智能驗證碼辨識
- **數字驗證碼**：介接 OpenAI GPT-4o 視覺介面，實現 99% 準確率的文字驗證碼自動解碼。
- **複雜驗證 (ReCAPTCHA)**：預留 GenAI 識別邏輯，可針對 Google ReCAPTCHA 的「公車、消防栓、紅綠燈」進行影像分類與模擬點擊。
### 2. 動態 IP 矩陣 (AWS EIP Rotation)
- **核心工具**：[SwitchIP](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/優化後空間/GCIS/lab_批次處裡_單元測試版本_251007.py#L78) 負責直接與 AWS IMDSv2 通訊，觸發 EIP 的解綁、釋放、重新申請與綁定。
- **每日配額管理**：`DailyQuota` 透過 JSON 持久化管理，防止因切換 IP 過於頻繁導致 AWS 帳戶或子網受損。
---
## 🔄 資料工程流水線 (Standard Pipeline)
本專案奉行「資料為本、核對為王」的工程核心。
### 三層資料架構流程
1. **Raw (原始入庫)**：
    - **Raw-first 規約**：原始 CSV 的 HEADER/META/DATA 一律「先行完整入庫」，作為未來發生爭議或稽核時的唯一真跡。
    - 參考表：`tmp_rawData`
2. **Tmp (中間暫存/Staging)**：
    - 經 ETL 清洗後的臨時表，用於進行業務核對。
3. **Main (正式累積/History)**：
    - **Append-only**：主表採取只增不減策略，保留歷史變遷紀錄，不進行 destructive 的 upsert。
### 核心核對機制
- **強制核對 (Strict Check)**：若 `raw(DATA) count` 與 `tmp count` 筆數不一致，系統將直接攔截並中止任務。
- **MD5 差異雜湊**：[merge_diff_tmp_to_main_taxinfo](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/優化後空間/稅籍/db_loader_v4.py#L254) 採用 16 欄位 MD5 雜湊對比，極速識別增量與異動。
---
## 🧹 ETL 與資料清洗邏輯
### 1. 複雜字元與難字處理
- **PUA (Private Use Area) 辨識**：自動識別 `0xE000-0xF8FF` 區域字元，將難字/符號轉碼或標準化，避免資料庫崩潰。
- **正規化**：基於 `unicodedata.normalize('NFC')` 進行字串統一。
- 參考實作：[DataCleaner.replace_diff](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/factory_etl.py#L155)。
### 2. 格式歸一化
- **民國轉西元**：實現 `ad_to_roc` 轉換函數，支援 `1140625` -> `2025-06-25` 的自動推算。
- **全半形處理**：全面將全形英數、符號轉為標準半形。
---
## 🖥️ 應用場景與 Use Cases
1. **CLI 指令模式**：支援透過終端機傳參，實現自定義 CSV 指定或跳過下載執行。
2. **API 服務整合**：
    - **FastAPI / Flask**：用於商工即時查詢 API。
    - **Django**：可整合至後台管理系統，進行資料批次管理。
    - 參考封裝：[api_main.py](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/舊版code/tax/072_781_crawler_all_info_a_1_result/072_781_crawler_all_info_a_1_result/商工API/api_main.py)
---
## 🗃️ 詳盡資料庫 DDL (MySQL)
路徑：[fatory_sql.sql](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/fatory_sql.sql)
| 表名 | 描述 |
| :--- | :--- |
| `FactoryInfo` | 正式工廠資料表（含 統一編號、工廠名稱、負責人等 15+ 欄位） |
| `FactoryInfo_stage` | 原始 CSV 匯入暫存區 |
| `FactoryInfo_stage2` | 經格式驗證後的合格資料存儲區 |
| `FactoryInfo_error_log` | 紀錄包含異常原因與錯誤發生時間的不合格數據 |
| `quality_report_data` | 提供資料品質分析與命中率統計的寬表 |
---
## 🗃️ 資料庫定義 (DDL - MySQL)
- **詳盡 DDL 連結**：[tax_DDL.sql](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/優化後空間/稅籍/SQL/tax_DDL.sql)
### 範例：tmp_rawData (極細緻定義)
| 欄位名 | 欄位中文 | 資料型態 | 範例 | 說明 |
| :--- | :--- | :--- | :--- | :--- |
| `run_id` | 批次執行標識 | VARCHAR(32) | RUN_2026/02/02 | 唯一執行碼 |
| `row_type` | 行類型標簽 | ENUM | DATA | HEADER/META/DATA |
| `c01` - `c16` | 通用原始數據 | TEXT | 20828393 | 對應 CSV 原始內容 |
| `loaded_at` | 入庫時間 | DATETIME | 2082/02/02 16:00 | 自動紀錄 |
> [!TIP]
> **索引優化**: 關鍵表皆具備 `idx_run_party` 與 `idx_row_hash` 等索引，顯著提升百萬級數據的差異比對效能。
---
## 🛠️ ToolBox (工具箱)
全專案通用的底層支撐工具。
- **檔案路徑**: [toolBox.py](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/toolBox.py)
- **內含工具**:
    - `switchIP`: AWS IP 切換核心。
    - `DailyQuota`: 配額管理器。
    - `NetworkError`, `BlockedError`: 自定義異常定義。
---
> ---
## 研發與沙盒區 (R&D Sandbox)
記錄開發過程中為驗證高效能邏輯所開發的獨立功能腳本。
---
## 國貿局：
- **主要程式**: [lab_251001_獨立測試功能開發.py](hncb_crawler/自動化網爬架構/lab_crawler/優化後空間/國貿局/lab_251001_獨立測試功能開發.py)
- **精密分片測試**: 整合 `argparse` 實現高度細化的批次控制，可根據 **Shard (分片)** 或 **Mod/Rem (取模/餘數)** 進行精準的資料段派送測試。
- **狀態持久化**: 透過輕量級 JSON 檔案 (`state.json`) 定義 `last_index` 標記，實現極致精確的斷點續跑開發。
> [!IMPORTANT]
> 維護時務必優先確保 `lab_crawler` 目錄內的程式碼正確性，此目錄為目前最新的生產級網爬邏輯核心。
*Generated by Ian - 2026-02-02*
>
--- 
## GCIS 商工_友善列印
- **主要程式**：[gcis_snapshot_main.py](hncb_crawler/自動化網爬架構/lab_crawler/舊版code/gcis/gcis_snapshot_main.py)
- ### 程式重點介紹：GCIS 友善列印

這支程式的目標很單純但做法很「務實」：
**用 Selenium 打開經濟部商工登記查詢頁，查統編，進入公司頁，點「友善列印」，然後用 Chrome DevTools Protocol（CDP）的 `Page.printToPDF` 直接把頁面輸出成 PDF**。  

### company_id 白名單驗證（8 碼數字）
- `_assert_company_id()` 用 regex `^\d{8}$` 強制輸入為 8 碼數字
- 不符合直接 `raise ValueError`
- 這是「防呆 + 防注入/防奇怪輸入」的第一道門

### 直接 CDP 匯出 PDF（穩定、可重現）
- 核心改造：`driver.execute_cdp_cmd("Page.printToPDF", {...})`
- 回傳是 base64，程式 `base64.b64decode()` 後寫檔
- 好處：
  - 不需要下載對話框
  - 不需要 OS 層級滑鼠鍵盤控制（pyautogui）
  - headless 也能跑

### 重試機制（針對網站不穩、驗證碼、偶發錯誤）
- `CONFIG["retry_limit"] = 5`，外層 `while attempt < retry_limit`
- 偵測到 reCAPTCHA 就 **quit + continue 重試**
- 對「偶發性網路/載入/被擋」比一次死更合理

### timeout + WebDriverWait 顯性等待
- `driver.set_page_load_timeout(CONFIG["page_load_timeout"])`
- 多處 `WebDriverWait(...).until(EC.presence_of_element_located(...))`
- 重點：不靠 `time.sleep()` 猜時間（sleep 是賭博，Wait 是工程）

### 嚴謹資源釋放
- `finally` 裡做 `driver.quit()`，避免 Chrome 殘留滿地跑
- 在 reCAPTCHA 分支也會先 quit 再重試

### 主流程：`Gcis_Snapshot(company_id, test_type=False)`
1. **驗證 company_id**
2. **啟動 driver**
3. 開查詢頁 `queryInit.do`
4. 等輸入框 `qryCond`
5. 輸入統編 + Enter
6. **reCAPTCHA 偵測**（class `g-recaptcha`）：有就重試
7. 等搜尋結果第一筆連結
   - 等不到 → 回傳 `Not_companyID`
8. 點第一筆 → 進公司詳情
9. 等並點「友善列印」按鈕 `friendlyPrint`
10. 切換到新視窗（友善列印會另開視窗）
11. `Page.printToPDF` → 寫入 `{outdir}/gcis_friendlyprint_result_{cid}_{timestamp}.pdf`
12. 回傳 `success`

---

## GCIS 商工
### 這份程式在幹嘛（重點版）

這是一個「把 **經濟部商工登記（GCIS）開放資料 API** 拉下來，做基本清洗與格式轉換後，**寫進 MySQL**」的入庫腳本。  
核心邏輯：**以 crawlerdb.TaxRecord 當待處理清單 + 狀態旗標表**，逐步補齊公司基本資料、可營項目、項目說明、公司/分公司/商業類型、董監持股。

> 實際上更像是 **ETL/爬取入庫**；真正的「品質檢查」只靠旗標與缺值判斷，尚缺少 schema 驗證、欄位型別/範圍檢查、重複/一致性檢查。

### 1) 環境與依賴

- 關閉 SSL 驗證：`ssl._create_default_https_context = ssl._create_unverified_context`
  - 方便但危險：等於允許被中間人攻擊；若在企業/金融環境，較為危險。
- `warnings.filterwarnings("ignore", category=UserWarning)`：壓掉警告，debug 會更痛。
- 用到的主要套件
  - `requests`：打 API（用 Session + timeout + retry）
  - `pymysql`：寫 MySQL
  - `json` / `datetime` / `time`：解析與排程
  - `tqdm.trange`：進度條

### 2) DB 連線與狀態旗標（TaxRecord 作為總控）

### MySQL 連線
- `autocommit=False`：手動 commit/rollback（合理）
- 但各函式大量「逐筆 commit」，效能和鎖競爭會很抖。

### TaxRecord 旗標欄位（關鍵）
這份腳本主要靠 `crawlerdb.TaxRecord` 上的欄位旗標來避免重跑：

- `COM_1_TYPE`：C1 公司基本資料是否入庫（Y/N/NULL）
- `COM_3_TYPE`：C3 可營項目是否入庫（Y/N/NULL）
- `COM_D_TYPE`：DS 董監/持股是否入庫（Y/N/NULL）
- `COM_TYPE / BNH_TYPE / BIZ_TYPE`：公司/分公司/商業 是否存在（Y/N/NULL）
- 另有 `*_Update_Time` 記錄更新時間

> 旗標是這套系統的「唯一流程控制」。一旦旗標錯標，資料就可能永久跳過或重複灌入。

### 3) HTTP 層：timeout + 重試（簡易版）

### Session 設定
- 固定 `User-Agent`
- `HTTP_TIMEOUT=15`、`MAX_RETRY=3`、重試間隔 `0.8s`

### `http_get_text(url, params)`
- 成功回 `r.text`
- 失敗印 log，重試
- 最終失敗回空字串 `""`

> 這裡沒有針對 429/5xx 做指數退避，也沒有針對 JSON 直接 `r.json()`，屬於「夠用但不硬」。

### 4) 小工具（資料清洗/轉換）

### `_tw_roc_to_iso(s)`
- 把民國日期 `yyyMMdd` 轉 `yyyy-MM-dd`
- 規則：前三碼是民國年，+1911；其餘照切 mm/dd
- 空值/格式不對回 `None`

### `_to_int_or_none(v)`
- 純數字字串 -> int，否則 None  
（在本段程式中幾乎沒被用到）

### 5) GCIS API 封裝（五個端點）

每個函式都只是組 URL + OData filter：

- `get_c1(cid)`：公司登記基本資料（應用一）
- `get_c3(cid)`：公司登記基本資料（應用三：可營項目）
- `get_ci(item_code)`：營業項目代碼的描述資料
- `get_bc(cid)`：查公司/分公司/商業 exist/type
- `get_ds(cid)`：董監/持股資料

> 它們都回「文字」，後面再 `json.loads()`。好處是統一錯誤處理，壞處是解析錯誤晚爆。

### 6) 五段入庫流程（真正主菜）

### A) `c1_to_db()`：公司基本資料入庫 → `CompanyStatus`
**來源**：`TaxRecord` 中 `COM_1_TYPE IS NULL` 的 Party_ID  
**動作**：
1. `get_c1(party_id)` 抓資料
2. 解析 JSON（list 取第一筆）
3. 抽欄位、日期轉換（民國→西元）
4. `INSERT INTO crawlerdb.CompanyStatus (...)`
5. 更新 `TaxRecord.COM_1_TYPE='Y'` 或失敗標 `N`

**寫入的資料重點**
- 公司名、狀態、資本額、實收資本額、負責人、地址、登記機關
- 設立/核准變更/撤銷/停業等日期
- Case_Status/Desc

**需要警覺的點**
- `INSERT` 沒有 upsert：重跑可能重複（除非 DB 有 unique constraint 擋）
- `PaidIn_Capital` 預設 0：會把「未知」跟「真的 0」混在一起

### B) `c3_to_db()`：可營項目入庫 → `CompanyItems`
**來源**：TaxRecord 內 `COM_1_TYPE='Y'` 且 CompanyItems 尚無資料  
**動作**：
1. `get_c3(party_id)`
2. 解析後走 `Cmp_Business` 陣列
3. 把 `Business_Item` 一條條塞進 `CompanyItems(Party_ID, Business_Item)`
4. 依是否有插入資料更新 `COM_3_TYPE=Y/N`

**你需要警覺的點**
- 沒有去重：同一家公司同一項目若 API 回重複，可能重插（看 DB constraint）
- `T_num` 變數沒用（看起來是以前想統計 total 但忘了）

### C) `item_to_db()`：補齊營業項目說明 → `ItemsDescription`
**來源**：CompanyItems 中出現、但 ItemsDescription 不存在的 `Business_Item`  
**動作**：
1. `get_ci(code)` 拉 item 描述
2. 寫入分類階層、描述文字、內容、以及 `Dgbas`（用 `\t` 拆 code/desc）

**你需要警覺的點**
- `time.sleep(1)`：一個 code 等 1 秒，資料量大會超慢
- 若 `dgbas_raw` 不是 `code\tdesc` 格式就只放 code
- 沒有明確處理「API 回空值」造成的資料不完整（只是不插）

### D) `bizcom_type_to_db()`：判斷 公司/分公司/商業 類型旗標 → 回寫 TaxRecord
**來源**：TaxRecord 中任一 type 欄位為 NULL  
**動作**：
1. `get_bc(pid)` → JSON array
2. 篩 `exist == "Y"` 的 TYPE
3. 三種旗標：
   - `COM_TYPE`：是否包含「公司」
   - `BNH_TYPE`：是否包含「分公司」
   - `BIZ_TYPE`：是否包含「商業」
4. update 回 TaxRecord

**你需要警覺的點**
- 假設 TYPE 文字固定是「公司/分公司/商業」：若 API 改字詞就全掛
- 沒有把 API 失敗和「真的不存在」分開（失敗也會標 N 嗎？目前是：失敗→arr=[]→全 N）


### E) `ds_to_db()`：董監/持股入庫 → `CompanySharesheld`
**來源**：TaxRecord 中 `COM_D_TYPE IS NULL AND COM_TYPE='Y'`  
**動作**：
1. `get_ds(pid)` → list
2. 每筆 insert 到 `CompanySharesheld`
3. 若 inserted > 0 → `COM_D_TYPE='Y'` 否則 'N'

**你需要警覺的點**
- 每筆 insert 都可能 rollback（但 rollback 會回滾整個 transaction 中未 commit 的插入）
- 沒有先清掉舊資料：如果公司董監變動，你這會越疊越多，除非你把它當歷史表

### 7) main() 執行順序（流程編排）

`main()` 順序是：

1. `bizcom_type_to_db()`
2. `c1_to_db()`
3. `c3_to_db()` + `item_to_db()`
4. `ds_to_db()`

並在 `__main__` 印開始/結束時間。DB close 被註解掉。

### 8) 淺在風險點（務實版）

- **SSL 驗證被關掉**：這不是「小瑕疵」，是資安雷。
- **資料一致性弱**：用旗標控制流程，但沒有「資料版本/更新策略」；API 資料變了，表內不會反映（只會因為旗標已 Y 而跳過）。
- **重複資料/重跑策略不明**：全用 INSERT，沒有 upsert / unique constraint 的話會堆垃圾。
- **失敗 vs 無資料混在一起**：HTTP 失敗回空字串，最後多數路徑直接標 N，會把「網路抖一下」變成「永久沒資料」。
- **效能**：逐筆 commit + 大量 sleep；在資料量大時會慢到像地質年代。

---

## 9) 用一句話總結

這份 code 是「用 GCIS 開放資料 API 補齊 TaxRecord 的公司資訊，並以一堆 Y/N 旗標做流程控管」的 ETL 腳本；能跑，但在資安、可回補更新、失敗判斷、與資料去重/版本化上都有明顯缺口。
---
