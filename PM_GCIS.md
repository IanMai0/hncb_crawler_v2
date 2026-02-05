### 4. 商工公司/商業模組 (GCIS)
深入擷取經濟部商工登記資料，區分公司與商業兩套系統。
- **原始碼**: [gcis.py](file:///C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/gcis.py)
- **主要功能**:
    - `fetch_info_c()` / `fetch_directors_c()`: 公司基本資料與董監事明細。
    - `fetch_agency_b()` / `fetch_info_b()`: 商業登記應用一/三資料。
