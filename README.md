# HNCB Crawler v2 核心網爬架構總覽 (lab_crawler)
本文件詳盡記錄了基於 `lab_crawler` 目錄的新型網爬架構，涵蓋五大業務模組、核心技術專題、資料庫 DDL 以及自動化工具箱。
---
## 核心總表

| 來源模組 | 調度層 (邏輯) | 轉換層 (風險/問題) | 資料層 (狀態) | Memo |
| :--- | :--- | :--- | :--- | :--- |
| **稅籍** | `run_daily_job_v3.py` | `crawler_etl_v3.py` | `db_loader_v4.py` | None |
| **國貿局** | `lab_250930v3_模組化版本.py` | `lab_250930v3_模組化版本.py` | None | None |
| **工廠** | `lab_flow_control.py` | `lab_factory_etl_v5.py` | None | None |
| **商工公司** | `lab_批次處裡_單元測試版本_251007.py` | `lab_批次處裡_單元測試版本_251007.py` | None | None |
| **商工商業** | `lab_批次處裡_單元測試版本_251007.py` | `lab_批次處裡_單元測試版本_251007.py` | None | None |

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
## 🧪 研發與沙盒區 (R&D Sandbox)
記錄開發過程中為驗證高效能邏輯所開發的獨立功能腳本。
### 獨立功能與分區測試
國貿局：
- **主要程式**: [lab_251001_獨立測試功能開發.py](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/優化後空間/國貿局/lab_251001_獨立測試功能開發.py)
- **精密分片測試**: 整合 `argparse` 實現高度細化的批次控制，可根據 **Shard (分片)** 或 **Mod/Rem (取模/餘數)** 進行精準的資料段派送測試。
- **狀態持久化**: 透過輕量級 JSON 檔案 (`state.json`) 定義 `last_index` 標記，實現極致精確的斷點續跑開發。
> [!IMPORTANT]
> 維護時務必優先確保 `lab_crawler` 目錄內的程式碼正確性，此目錄為目前最新的生產級網爬邏輯核心。
*Generated by Ian - 2026-02-02*
> 


