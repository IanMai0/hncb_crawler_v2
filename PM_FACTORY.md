# 工廠模組 (Factory Module) 代碼報告
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
