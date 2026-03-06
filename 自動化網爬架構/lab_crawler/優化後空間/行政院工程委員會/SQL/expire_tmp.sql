CREATE TABLE pcc_expire_tmp (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    -- 以下這塊處理廠商相關欄位
    corporation_number VARCHAR(20),        -- 廠商統編/廠商代碼 0
    corporation_name VARCHAR(255),         -- 廠商名稱 2
    corporation_address VARCHAR(255),      -- 廠商地址 3
    corporation_country VARCHAR(55),       -- 國別/廠商國別 4
    corporation_principal VARCHAR(10),     -- 廠商負責人  10
    corporation_principal_id VARCHAR(10),  -- 廠商負責人身份證字號/廠商負責人身分證字號 11
    -- 以下這塊處理時間相關欄位
    -- status_reason TEXT,             -- 狀態原因(暫保留)
    origional_announce_date DATETIME,  -- 首次刊登公報之公告日/原始公告時間/原始公告日期 14
    announce_date DATETIME,            -- 公告時間/公告日期 15
    effective_date DATETIME,           -- 生效日/生效時間/拒絕往來生效日 16
    expire_date DATETIME,              -- 截止時間/拒絕往來截止日 18
    -- announce_date DATETIME,         -- 刊登政府採購公報之公告日(暫保留)
    -- 以下這塊處理標案相關欄位
    case_name VARCHAR(255),                   -- 標案名稱 12
    case_no VARCHAR(55),                      -- 標案案號 1
    case_appeal_result VARCHAR(5000),         -- 異議或申訴結果 19
    -- 以下這塊處理法律相關欄位
    judgment_doc_no VARCHAR(50),              -- 判決書編號/判決書字號 20
    judgment_gpa101_caluse VARCHAR(20),       -- 符合政府採購法第101條第1項款次/符合政府採購法第101條所列之情形 13
    judgment_effective_auration VARCHAR(10),  -- 符合政府採購法第103條所定期間/政府採購法第103條第一項所規定之期間 17
    judgment_no VARCHAR(255),                 -- 適用法條 21
    judgment_range_date DATE,                 -- 法條所定期間 22
    judgment_info VARCHAR(255),               -- 罪名及其他必要揭露資訊 23
    -- 以下這塊處理機關相關欄位
    announce_agency_name VARCHAR(25),     -- 機關名稱/刊登機關名稱 6
    announce_agency_no, VARCHAR(55),      -- 機關代碼/刊登機關代碼 5
    announce_agency_address VARCHAR(55),  -- 機關地址 7
    contact_person VARCHAR(10),           -- 聯絡人/機關聯絡人 8
    contact_no VARCHAR(20),               -- 聯絡電話 9
    -- announce_agency_mail VARCHAR(55),  -- 機關聯絡人電子郵件信箱(暫保留)
    -- 以下這塊處理其它欄位
    remark VARCHAR(50),                             -- 備註 24
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP,  -- raw data 寫入時間
    -- 等待確認 Insert time, 讓行政院工程委員會與稅籍專案邏輯相同

    raw_payload JSON,

    INDEX idx_party_id (party_id),
    INDEX idx_status_type (status_type),
    INDEX idx_publish_date (publish_date)
);
