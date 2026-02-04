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
### 1. 稅籍模組 (Tax) —— 三位一體架構
針對財政部稅籍日檔的高頻更新需求，由三隻核心程式構成高強度的自動化流水線：
- **調度層 (App)**: [run_daily_job_v3.py](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/優化後空間/稅籍/run_daily_job_v3.py)
    - 作為整個模組的 **Entry Point**。
    - 負責參數解析（日批/回補/手動指定 CSV）、流程控制、暫存表清理、以及執行 run_id 的生成與監控。
- **轉換層 (ETL)**: [crawler_etl_v3.py](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/優化後空間/稅籍/crawler_etl_v3.py)
    - 負責 **資料獲取與初步清洗**。
    - 內容包含：自動偵測 URL 下載、ZIP 解壓、CSV 核心日期（META）解析與驗證、以及將「民國日期」轉為「標準西元格式」的轉換邏輯。
- **資料層 (DB)**: [db_loader_v4.py](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/優化後空間/稅籍/db_loader_v4.py)
    - 負責 **高度複雜的入庫邏輯**。
    - 執行 `Raw (Audit) > Tmp (Staging) > Main (History)` 的三層流轉。
    - 核心包含：`executemany` 批次入庫、MD5 差異雜湊比對、以及「Raw vs Tmp」筆數不一致即攔截的強制核對機制、「Tmp vs Main」比對。
# 國貿局模組 (TradeAdmin) 代碼分析報告
針對國貿局查詢系統之嚴格限制，採用專門開發的彈性擷取方案：
- **核心程式**: [lab_250930v3_模組化版本.py](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/優化後空間/國貿局/lab_250930v3_模組化版本.py)
- **HttpClient 彈性機制**: 具備自動重建 Session 功能，當偵測到連續連線失敗時會重啟連線池，繞過伺服器端的連線數封鎖。
- **精準 Buffer 匯出**: 透過 [BucketCsvExporter](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/優化後空間/國貿局/lab_250930v3_模組化版本.py#L191) 實現定時 Flush (預設 10 分鐘) 到固定名稱 CSV (`basic_info.csv`)，優化大量 I/O 並防止斷電遺失資料。
- **斷點保護 (Signal Handling)**: 內建 `SIGINT`/`SIGTERM` 捕捉器，確保在使用者中斷時強制將記憶體 Buffers 資料落檔。
- **100% 稽核對帳**: 對於查無資料或 API 失敗的 ID，自動輸出「空值標記」行，確保輸出結果與輸入名單完全對等。
#### 🚀 路線 A：AJAX 高速繞過 (主推)
- **核心程式**: [lab_250930v3_模組化版本.py](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/優化後空間/國貿局/lab_250930v3_模組化版本.py)
- **技術細節**: 通過逆向 XHR 協議，發現系統存在固定驗證碼漏洞 (`verifyCode: "5408"`)。
- **執行流程**:
    1. 獲取當前 Session 的 `verifySHidden` token。
    2. 直接攜帶固定驗證碼 `5408` 提交 POST 請求。
- **優勢**: 零成本、極低延遲、無辨識錯誤風險。
#### 🧠 路線 B：GenAI + OCR 智能辨識 (備援)
- **技術細節**: 下載 `popCaptcha.action` 驗證碼圖片，並利用 **OpenAI GPT-4o Vision** 進行視覺分析。
- **應用場景**: 當 AJAX 固定值繞過法失效或遇到複雜人機驗證時切換。
- **HttpClient 彈性機制**: 具備自動重建 Session 功能，當偵測到連續連線失敗時會重啟連線池。
- **精準 Buffer 匯出**: 透過 [BucketCsvExporter](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/優化後空間/國貿局/lab_250930v3_模組化版本.py#L191) 實現定時 Flush。

# 工廠模組 (Factory Module) 代碼分析報告
## 1. 概覽
本報告針對位於 `舊版code/factory` 目錄下的兩個核心檔案進行深度分析：
1.  **[lab_factory_etl_v5.py](cci:7://file:///C:/Users/wits/PycharmProjects/hncb_crawler/%E8%87%AA%E5%8B%95%E5%8C%96%E7%B6%B2%E7%88%AC%E6%9E%B6%E6%A7%8B/lab_crawler/%E8%88%8A%E7%89%88code/factory/lab_factory_etl_v5.py:0:0-0:0)**: 核心資料處理 ETL (Extract, Transform, Load) 邏輯。
2.  **[lab_flow_control.py](cci:7://file:///C:/Users/wits/PycharmProjects/hncb_crawler/%E8%87%AA%E5%8B%95%E5%8C%96%E7%B6%B2%E7%88%AC%E6%9E%B6%E6%A7%8B/lab_crawler/%E8%88%8A%E7%89%88code/factory/lab_flow_control.py:0:0-0:0)**: 流程控制與自動化下載腳本。
## 2. 代碼分析細節
### 2.1 [lab_factory_etl_v5.py](cci:7://file:///C:/Users/wits/PycharmProjects/hncb_crawler/%E8%87%AA%E5%8B%95%E5%8C%96%E7%B6%B2%E7%88%AC%E6%9E%B6%E6%A7%8B/lab_crawler/%E8%88%8A%E7%89%88code/factory/lab_factory_etl_v5.py:0:0-0:0) (ETL 核心)
此模組採用 **物件導向 (OOP)** 設計，將資料處理流程拆解為五個主要類別，職責劃分清晰。
| 類別 (Class) | 職責 (Responsibility) | 關鍵邏輯與特點 | 潛在問題與改進建議 |
| :--- | :--- | :--- | :--- |
| **[DataPreprocessor](cci:2://file:///C:/Users/wits/PycharmProjects/hncb_crawler/%E8%87%AA%E5%8B%95%E5%8C%96%E7%B6%B2%E7%88%AC%E6%9E%B6%E6%A7%8B/lab_crawler/%E8%88%8A%E7%89%88code/factory/lab_factory_etl_v5.py:46:0-134:17)** | **預處理**<br>- 讀取 CSV<br>- 欄位正規化<br>- 資料拆分<br>- 日期轉換 | - **全形轉半形**: 使用 `chr(ord(c) - 0xFEE0)` 轉換邏輯。<br>- **年月轉換**: [convert_roc_to_ad](cci:1://file:///C:/Users/wits/PycharmProjects/hncb_crawler/%E8%87%AA%E5%8B%95%E5%8C%96%E7%B6%B2%E7%88%AC%E6%9E%B6%E6%A7%8B/lab_crawler/%E8%88%8A%E7%89%88code/factory/lab_factory_etl_v5.py:56:4-68:27) 處理民國轉西元 (1140625 -> 2025-06-25)。<br>- **欄位拆分**: 將「產業類別/主要產品」拆解為代碼與名稱。 | - **日期驗證**: 目前僅檢查長度 `\d{7,8}`，建議加入更嚴謹的日期合法性檢查 (如月份不可 > 12)。<br>- **欄位硬編碼**: 依賴 `COLUMN_CODE_MAP` 全域變數。 |
| **[DataCleaner](cci:2://file:///C:/Users/wits/PycharmProjects/hncb_crawler/%E8%87%AA%E5%8B%95%E5%8C%96%E7%B6%B2%E7%88%AC%E6%9E%B6%E6%A7%8B/lab_crawler/%E8%88%8A%E7%89%88code/factory/lab_factory_etl_v5.py:137:0-173:22)** | **清洗**<br>- HTML 解碼<br>- 空白處理<br>- 標點處理<br>- 難字處理 | - **PUA 處理**: 對 Unicode PUA 區段 (`0xE000-0xF8FF`) 進行過濾，這是處理政府資料的關鍵邏輯。<br>- **Unicode Normalize**: 使用 `NFC` 正規化。 | - **效能**: `apply` 若資料量大可能較慢，可考慮向量化操作。<br>- **特殊欄位**: 對「工廠名稱」等欄位保留標點，邏輯正確。 |
| **[DataAnomalyReporter](cci:2://file:///C:/Users/wits/PycharmProjects/hncb_crawler/%E8%87%AA%E5%8B%95%E5%8C%96%E7%B6%B2%E7%88%AC%E6%9E%B6%E6%A7%8B/lab_crawler/%E8%88%8A%E7%89%88code/factory/lab_factory_etl_v5.py:176:0-263:22)** | **異常檢測**<br>- 規則標記<br>- 異常統計 | - **規則引擎**: 定義了詳細的 regex 規則 (如統一編號重複檢查、長度檢查)。<br>- **異常代碼**: 使用 `E01`-`E11` 標準化錯誤代碼。 | - **規則維護性**: 規則寫死在 [__init__](cci:1://file:///C:/Users/wits/PycharmProjects/hncb_crawler/%E8%87%AA%E5%8B%95%E5%8C%96%E7%B6%B2%E7%88%AC%E6%9E%B6%E6%A7%8B/lab_crawler/gcis.py:240:4-245:27) 中，建議抽離至設定檔 (Config/YAML)。<br>- **記憶體**: `value_counts()` 在大數據量下的記憶體消耗需注意。 |
| **[StatisticalSummaryEngine](cci:2://file:///C:/Users/wits/PycharmProjects/hncb_crawler/%E8%87%AA%E5%8B%95%E5%8C%96%E7%B6%B2%E7%88%AC%E6%9E%B6%E6%A7%8B/lab_crawler/%E8%88%8A%E7%89%88code/factory/lab_factory_etl_v5.py:266:0-324:54)** | **統計**<br>- 文字分佈<br>- Top 100 分析 | - **特定報表**: 包含「前100大工廠持有者」的客製化報表邏輯。 | - **耦合性**: 包含特定業務邏輯 (Top 100)，與通用統計功能混雜。 |
| **[Output](cci:2://file:///C:/Users/wits/PycharmProjects/hncb_crawler/%E8%87%AA%E5%8B%95%E5%8C%96%E7%B6%B2%E7%88%AC%E6%9E%B6%E6%A7%8B/lab_crawler/%E8%88%8A%E7%89%88code/factory/lab_factory_etl_v5.py:326:0-356:12)** | **輸出**<br>- CSV 匯出<br>- DB 介面(空) | - **欄位排序**: 強制按照指定順序輸出。<br>- **DB 缺口**: [output_data_to_DB](cci:1://file:///C:/Users/wits/PycharmProjects/hncb_crawler/%E8%87%AA%E5%8B%95%E5%8C%96%E7%B6%B2%E7%88%AC%E6%9E%B6%E6%A7%8B/lab_crawler/%E8%88%8A%E7%89%88code/factory/lab_factory_etl_v5.py:355:4-356:12) 目前為 `pass`，尚未實作。 | - **DB 整合**: **這是與 Tax/TradeAdmin 模組最大的差距，缺乏自動入庫機制。** |
### 2.2 [lab_flow_control.py](cci:7://file:///C:/Users/wits/PycharmProjects/hncb_crawler/%E8%87%AA%E5%8B%95%E5%8C%96%E7%B6%B2%E7%88%AC%E6%9E%B6%E6%A7%8B/lab_crawler/%E8%88%8A%E7%89%88code/factory/lab_flow_control.py:0:0-0:0) (流程控制)
此腳本負責協調下載與 ETL 流程，但結構較為鬆散，偏向單次執行的 Script。
| 功能區塊 | 現況描述 | 問題與風險 |
| :--- | :--- | :--- |
| **環境設定** | 使用 `sys.path.append` 加入絕對路徑 (`C:/Users/wits/...`) | **高風險**: 路徑寫死 (Hardcoded)，無法在不同開發者或環境間移植。需改為相對路徑或模組化引用。 |
| **下載模組** | `download_and_extract_zip`: 下載 > 檢查 ZIP > 解壓 > 重命名(時間戳) | **URL 寫死**: 工廠資料 URL 直接寫在 `main` 區塊，一旦網址變更需修 code。<br>**缺乏 Retry**: 雖然有檢查 status code，但缺乏像 `HttpClient` 的 robust retry 機制。 |
| **流程編排** | 巢狀 `try-except` 結構 (Pyramid of Doom) | **可讀性差**: 每一層都為了 log 而包一層 try-except，造成代碼過度縮排。建議改用 Decorator 或統一的 Error Handler。 |
| **自動化判斷** | `AutomaticAbnormalJudgment`: 包含 API/Crawler/URL 檢查介面 | **未完成**: 內含 "待雲端與 DB Setup Complete" 註解，目前僅為空殼或 Placeholder。 |
## 3. 與現有架構 (Tax/TradeAdmin) 的差異
| 比較項目 | 工廠模組 (舊版) | 稅籍/國貿局模組 (新版) | 建議修正方向 |
| :--- | :--- | :--- | :--- |
| **資料庫 (DB)** | **無** (僅輸出 CSV) | **完整** (Raw -> Tmp -> Main 三層架構) | 實作 `output_data_to_DB`，對接 `crawlerdb`。 |
| **配置 (Config)** | 寫死在代碼中 (URL, Path) | 環境變數 (.env) / 參數化 | 抽離 URL、路徑至配置檔或 CLI 參數。 |
| **網路層** | 簡易 `requests` | 封裝的 `HttpClient` (Retry, Session) | 整合共用的網路基礎建設。 |
| **錯誤處理** | 巢狀 Try-Except | 裝飾器 / 全域異常處理 | 重構流程控制，扁平化代碼。 |
| **模組引用** | 絕對路徑 `sys.path` | 相對引入 / Package | 移除 `sys.path.append` 硬路徑。 |
## 4. 重構建議 (Refactoring Roadmap)
1.  **路徑與環境變數修正**:
    *   移除 `C:/Users/wits/...` 絕對路徑。
    *   將 `ZIP_URL` 與輸出路徑改為可配置參數。
2.  **DB 入庫實作**:
    *   參考稅籍模組，實作 `tmp_rawdata` -> `tmp_factory` -> `main_factory` 的入庫流程。
    *   在 `Output` 類別中完成 `output_data_to_DB`。
3.  **流程控制優化**:
    *   使用 `argparse` 支援 CLI 參數 (如指定日期、指定檔案)。
    *   扁平化 `try-except` 結構。
4.  **共用組件整合**:
    *   確認是否可共用 `factory_etl.py` 中的 `DataCleaner` 邏輯 (目前邏輯似乎高度相似但各自維護)。
## 5. 結論
`lab_factory_etl_v5.py` 的 ETL 邏輯本身相對完整且結構清晰（除了 DB 部分），但 `lab_flow_control.py` 作為進入點則過於依賴本地環境且流程控制原始。首要任務是**移除絕對路徑依賴**並**打通 DB 入庫**環節。
### 4. 商工公司/商業模組 (GCIS)
深入擷取經濟部商工登記資料，區分公司與商業兩套系統。
- **原始碼**: [gcis.py](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/gcis.py)
- **主要功能**:
    - `fetch_info_c()` / `fetch_directors_c()`: 公司基本資料與董監事明細。
    - `fetch_agency_b()` / `fetch_info_b()`: 商業登記應用一/三資料。
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


