# GCIS
## GCIS_商工公司
## GCIS_商工商業
---
# 工廠
---
# 稅籍
## 稅籍資料日批管線（GCIS TaxInfo Daily Pipeline）

本專案是一套 **稅籍資料日批處理管線**，用於每日（或回補）處理政府 GCIS 稅籍 CSV，並將「新增 / 異動」資料**追加寫入主表（歷史保留）**。

核心設計目標：

* **Raw-first**：原始資料一律先完整入庫，作為可稽核依據
* **Legacy-compatible**：沿用既有 `Tmp_TaxInfo` / `TaxInfo` 表結構，不打掉重練
* **Append-only main table**：主表只新增，不覆蓋、不 upsert
* **強制核對**：raw(DATA) vs tmp 筆數不一致，批次直接失敗
* **可日批、可回補、可手動指定 CSV**
---
# 國貿局
---
# 優良拒往
---
# 動保
---
