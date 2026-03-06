# 國貿局模組 (TradeAdmin) 開發整理
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
