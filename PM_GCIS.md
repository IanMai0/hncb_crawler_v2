# 商工資料日批管線（GCIS Commerce & Industry Daily Pipeline）

本文件定義 **商工（商業/公司）資料日批處理管線** 的架構規範。目標是將現有的單一爬蟲腳本重構為標準化、可觀測、支援 Raw-first 的 ETL 流程，並與稅籍模組（Tax）採用相同的架構標準。

核心設計目標：
*   **Raw-first**：每次爬取的回應（JSON）一律先存入 Raw Table，作為 Raw Data 存證。
*   **Modular Architecture**：拆分調度層、ETL 層、資料庫層。
*   **Dual-Stream**: 同時支援「商業登記 (Business)」與「公司登記 (Company)」兩條資料流。
*   **Incremental Merge**：主表採用 Append-only 策略，僅寫入異動與新增資料。

---
## 核心資料源總表
| 模組名稱 | MEMO | 資源連結 (URL) |
| :--- | :--- | :--- |
| **商工公司模組-應用一** | 登記基本資料 | https://data.gcis.nat.gov.tw/od/data/api/5F64D864-61CB-4D0D-8AD9-492047CC1EA6?$format={format}&$filter=Business_Accounting_NO eq {Business_Accounting_NO}&$skip={skip}&$top={top}) |
| **商工公司模組-應用二** | 董監持股資料 | https://data.gcis.nat.gov.tw/od/data/api/5F64D864-61CB-4D0D-8AD9-492047CC1EA6?$format={format}&$filter=Business_Accounting_NO eq {Business_Accounting_NO}&$skip={skip}&$top={top}) |
| **商工公司模組-應用三** | 營業項目資料 | https://data.gcis.nat.gov.tw/od/data/api/5F64D864-61CB-4D0D-8AD9-492047CC1EA6?$format={format}&$filter=Business_Accounting_NO eq {Business_Accounting_NO}&$skip={skip}&$top={top}) |
| **商工公司模組-應用一** | 登記基本資料 | https://data.gcis.nat.gov.tw/od/data/api/5F64D864-61CB-4D0D-8AD9-492047CC1EA6?$format={format}&$filter=Business_Accounting_NO eq {Business_Accounting_NO}&$skip={skip}&$top={top}) |
| **商工公司模組-應用二** | 董監持股資料 | https://data.gcis.nat.gov.tw/od/data/api/5F64D864-61CB-4D0D-8AD9-492047CC1EA6?$format={format}&$filter=Business_Accounting_NO eq {Business_Accounting_NO}&$skip={skip}&$top={top}) |
| **商工公司模組-應用三** | 營業項目資料 | https://data.gcis.nat.gov.tw/od/data/api/5F64D864-61CB-4D0D-8AD9-492047CC1EA6?$format={format}&$filter=Business_Accounting_NO eq {Business_Accounting_NO}&$skip={skip}&$top={top}) |

---

---

## 專案結構 (Target)
```
.
├── /logs                            # 程式運作 logs
├── /inputs                          # 目標統編清單 (Target List)
├── run_gcis_daily.py                # [New] 日批入口（Orchestrator）
├── gcis_etl.py                      # [New] 爬蟲 Client、Raw 入庫、ETL 轉換
├── db_loader_gcis.py                # [New] DB I/O 與 Merge 邏輯
├── lib/
│   ├── network.py                   # [Shared] 共用 HttpClient / IP Rotation (SwitchIP)
│   └── aws_utils.py                 # [Shared] AWS EIP 控制
├── .env                             # 環境變數
└── README.md
```

---

## 整體流程說明（Workflow）
```
START | run_id=GCIS_YYYYMMDD_HHMMSS
→ 讀取目標清單 (Input CSV)
→ 初始化 Batch Job

[Loop per Batch / ID]
    → 爬取 (Crawl)
       - 商業 API (Business) / 公司 API (Company)
       - IP Rotation (若遇封鎖)
    
    → tmp_gcis_raw (存證)
       - 寫入完整 JSON Response、HTTP Status、Crawl Time
       
    → ETL (轉換)
       - 資料清洗 (全形轉半形、民國轉西元)
       - 欄位映射 (Mapping)
       - 寫入 Tmp_GcisBusiness / Tmp_GcisCompany (暫存)

[End Loop]

→ 差異合併 (Merge)
   - Tmp vs Main (Hash 比對)
   - 寫入 GcisBusiness / GcisCompany (主表)

→ 清空暫存表 (Cleanup)
   - tmp_gcis_raw
   - Tmp_Gcis*

→ END
```

---

## 各檔案職責說明

### `run_gcis_daily.py`（日批入口 / Orchestrator）
**職責：**
*   **CLI 解析**：支援 `--target-csv` (指定目標清單), `--mode` (business/company/all)。
*   **流程控制**：管理 [run_id](cci:1://file:///C:/Users/wits/PycharmProjects/hncb_crawler/%E8%87%AA%E5%8B%95%E5%8C%96%E7%B6%B2%E7%88%AC%E6%9E%B6%E6%A7%8B/lab_crawler/%E5%84%AA%E5%8C%96%E5%BE%8C%E7%A9%BA%E9%96%93/%E7%A8%85%E7%B1%8D/run_daily_job_v3.py:53:0-54:49)，協調爬蟲迴圈與 DB Merge 時機。
*   **進度管理**：記錄已完成/未完成統編，支援斷點續跑。

### `gcis_etl.py`（ETL 與 Raw 處理）
**職責：**
*   **GcisClient**：封裝 API 請求，整合 [SwitchIP](cci:2://file:///C:/Users/wits/PycharmProjects/hncb_crawler/%E8%87%AA%E5%8B%95%E5%8C%96%E7%B6%B2%E7%88%AC%E6%9E%B6%E6%A7%8B/lab_crawler/%E5%84%AA%E5%8C%96%E5%BE%8C%E7%A9%BA%E9%96%93/GCIS/lab_%E6%89%B9%E6%AC%A1%E8%99%95%E8%A3%A1_%E5%96%AE%E5%85%83%E6%B8%AC%E8%A9%A6%E7%89%88%E6%9C%AC_251007.py:77:0-294:23) 機制（處理 429/Block）。
*   **Raw Parser**：將 API JSON 封裝為標準 Raw Row 格式。
*   **Transformers**：
    *   `transform_company()`: 處理公司 API 欄位。
    *   `transform_business()`: 處理商業 API 欄位 (含營業項目代碼拆解邏輯)。

### `db_loader_gcis.py`（資料庫操作層）
**職責：**
*   **Raw Table I/O**：批量寫入 `tmp_gcis_raw`。
*   **Legacy Tmp I/O**：寫入 `Tmp_GcisBusiness`, `Tmp_GcisCompany`。
*   **Merge Logic**：執行 SQL `MERGE` 或 `INSERT IGNORE ... SELECT ...` (Hash Diff)，將異動寫入主表。

---

## 資料表設計 (Schema)

### 1. `tmp_gcis_Business_raw` / `tmp_gcis_company_raw` (新表 - Raw 存證)
用於儲存每次 API 請求的原始回應，便於除錯與重新解析。

```sql
CREATE TABLE `tmp_gcis_raw` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `run_id` varchar(32) NOT NULL,
  `party_id` varchar(20) NOT NULL COMMENT '統一編號',
  `data_type` enum('COMPANY', 'BUSINESS') NOT NULL,
  `api_url` varchar(500) NOT NULL,
  `raw_json` longtext COMMENT 'API 原始回應 JSON',
  `http_status` int NOT NULL,
  `crawled_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  KEY `idx_run_party` (`run_id`, `party_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 2. `Tmp_GcisCompany` (暫存公司資料)
結構相同，Tmp 為暫存，Main 為歷史主表。

```sql
CREATE TABLE `GcisCompany` (
  `Party_ID` varchar(20) NOT NULL COMMENT '統一編號',
  `Party_Name` varchar(200),
  `Capital` bigint COMMENT '資本總額',
  `PaidIn_Capital` bigint COMMENT '實收資本額',
  `Rep_Name` varchar(100) COMMENT '負責人',
  `Address` varchar(255),
  `Setup_Date` date,
  `Status` varchar(50) COMMENT '公司狀態',
  `Update_Time` datetime DEFAULT CURRENT_TIMESTAMP,
  -- 其他 GCIS 特有欄位
  KEY `idx_party` (`Party_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 3. Tmp_GcisBusiness / GcisBusiness (商業資料)
結構相同，Tmp 為暫存，Main 為歷史主表。

```sql
CREATE TABLE `GcisBusiness` (
  `Party_ID` varchar(20) NOT NULL COMMENT '統一編號',
  `Business_Name` varchar(200),
  `Capital` bigint,
  `Rep_Name` varchar(100),
  `Address` varchar(255),
  `Organization_Type` varchar(50) COMMENT '組織型態',
  `Business_Items` text COMMENT '營業項目代碼字串',
  `Status` varchar(50),
  `Update_Time` datetime DEFAULT CURRENT_TIMESTAMP,
  KEY `idx_party` (`Party_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```
---
## 版本演進說明

### v1 (Current - Monolithic)
*   **程式**: `lab_批次處裡_單元測試版本_251007.py`
*   **特點**: 
    *   單一腳本完成 Crawl, ETL, IO。
    *   使用 CSV 進行 Upsert (File-based DB)，效能隨資料量下降。
    *   AWS IP 切換與業務邏輯高度耦合。

### v2 (Target - Modular & DB-centric)
*   **程式**: `run_gcis_daily.py` + `gcis_etl.py` + `db_loader_gcis.py`
*   **改進**:
    *   **DB Base**: 廢除 CSV Upsert，全面改用 MySQL。
    *   **Raw-first**: 增加 Raw Table 存證，確保資料可追溯。
    *   **Decoupled**: 爬蟲 (Client) 與儲存 (Loader) 分離，IP Rotation 模組獨立。

---

## 注意事項
1.  **IP Rotation**: GCIS API 對 IP 封鎖頻率極高，[SwitchIP](cci:2://file:///C:/Users/wits/PycharmProjects/hncb_crawler/%E8%87%AA%E5%8B%95%E5%8C%96%E7%B6%B2%E7%88%AC%E6%9E%B6%E6%A7%8B/lab_crawler/%E5%84%AA%E5%8C%96%E5%BE%8C%E7%A9%BA%E9%96%93/GCIS/lab_%E6%89%B9%E6%AC%A1%E8%99%95%E8%A3%A1_%E5%96%AE%E5%85%83%E6%B8%AC%E8%A9%A6%E7%89%88%E6%9C%AC_251007.py:77:0-294:23) 模組需確保穩定性，並與 Retry 機制深度整合。
2.  **資料一致性**: 商業與公司 API 可能回傳不同結構的資料，ETL 層需做好防禦性程式設計 (Defensive Programming)。
3.  **營業項目**: 商業登記的營業項目常包含中文描述（如「一、...」），需保留 v1 的解析邏輯 (`parse_business_items`)。


