-- 臨時表
CREATE TABLE `pcc_expire_tmp` (
    -- 以下這塊處理廠商相關欄位
    `Corporation_number` VARCHAR(20) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,        -- 廠商統編/廠商代碼 0
    `Corporation_name` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,         -- 廠商名稱 2
    `Corporation_address` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,      -- 廠商地址 3
    `Corporation_country` VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,       -- 國別/廠商國別 4
    `Corporation_principal` VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,     -- 廠商負責人  10
    `Corporation_principal_id` VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,  -- 廠商負責人身份證字號/廠商負責人身分證字號 11
    -- 以下這塊處理時間相關欄位
    `Original_announce_date` DATETIME DEFAULT NULL,  -- 首次刊登公報之公告日/原始公告時間/原始公告日期 14
    `Announce_date` DATETIME DEFAULT NULL,            -- 公告時間/公告日期 15
    `Effective_date` DATETIME DEFAULT NULL,           -- 生效日/生效時間/拒絕往來生效日 16
    `Expire_date` DATETIME DEFAULT NULL,              -- 截止時間/拒絕往來截止日 18
    -- 以下這塊處理標案相關欄位
    `Case_name` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,                   -- 標案名稱 12
    `Case_no` VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,                      -- 標案案號 1
    `Case_appeal_result` VARCHAR(5000),         -- 異議或申訴結果 19
    -- 以下這塊處理法律相關欄位
    `Judgment_doc_no` VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,              -- 判決書編號/判決書字號 20
    `Judgment_gpa101_clause` VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,       -- 符合政府採購法第101條第1項款次/符合政府採購法第101條所列之情形 13
    `Judgment_effective_duration` VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,  -- 符合政府採購法第103條所定期間/政府採購法第103條第一項所規定之期間 17
    `Judgment_no` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,                 -- 適用法條 21
    `Judgment_range_date` VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,                 -- 法條所定期間 22
    `Judgment_info` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,               -- 罪名及其他必要揭露資訊 23
    -- 以下這塊處理機關相關欄位
    `Announce_agency_name` VARCHAR(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,     -- 機關名稱/刊登機關名稱 6
    `Announce_agency_no` VARCHAR(55) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,      -- 機關代碼/刊登機關代碼 5
    `Announce_agency_address` VARCHAR(55) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,  -- 機關地址 7
    `Contact_person` VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,           -- 聯絡人/機關聯絡人 8
    `Contact_no` VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,               -- 聯絡電話 9
    -- 以下這塊處理其它欄位
    `Remark` VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,                   -- 備註 24
    `Update_Time` DATETIME DEFAULT CURRENT_TIMESTAMP                                                      -- raw data 寫入時間
    -- 等待確認 Insert time, 讓行政院工程委員會與稅籍專案邏輯相同
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- 正式表
CREATE TABLE PccExpire (
    -- 以下這塊處理廠商相關欄位
    `Corporation_number` VARCHAR(20) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,        -- 廠商統編/廠商代碼 0
    `Corporation_name` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,         -- 廠商名稱 2
    `Corporation_address` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,      -- 廠商地址 3
    `Corporation_country` VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,       -- 國別/廠商國別 4
    `Corporation_principal` VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,     -- 廠商負責人  10
    `Corporation_principal_id` VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,  -- 廠商負責人身份證字號/廠商負責人身分證字號 11
    -- 以下這塊處理時間相關欄位
    `Original_announce_date` DATETIME DEFAULT NULL,  -- 首次刊登公報之公告日/原始公告時間/原始公告日期 14
    `Announce_date` DATETIME DEFAULT NULL,            -- 公告時間/公告日期 15
    `Effective_date` DATETIME DEFAULT NULL,           -- 生效日/生效時間/拒絕往來生效日 16
    `Expire_date` DATETIME DEFAULT NULL,              -- 截止時間/拒絕往來截止日 18
    -- 以下這塊處理標案相關欄位
    `Case_name` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,                   -- 標案名稱 12
    `Case_no` VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,                      -- 標案案號 1
    `Case_appeal_result` VARCHAR(5000),         -- 異議或申訴結果 19
    -- 以下這塊處理法律相關欄位
    `Judgment_doc_no` VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,              -- 判決書編號/判決書字號 20
    `Judgment_gpa101_clause` VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,       -- 符合政府採購法第101條第1項款次/符合政府採購法第101條所列之情形 13
    `Judgment_effective_duration` VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,  -- 符合政府採購法第103條所定期間/政府採購法第103條第一項所規定之期間 17
    `Judgment_no` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,                 -- 適用法條 21
    `Judgment_range_date` VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,                 -- 法條所定期間 22
    `Judgment_info` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,               -- 罪名及其他必要揭露資訊 23
    -- 以下這塊處理機關相關欄位
    `Announce_agency_name` VARCHAR(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,     -- 機關名稱/刊登機關名稱 6
    `Announce_agency_no` VARCHAR(55) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,      -- 機關代碼/刊登機關代碼 5
    `Announce_agency_address` VARCHAR(55) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,  -- 機關地址 7
    `Contact_person` VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,           -- 聯絡人/機關聯絡人 8
    `Contact_no` VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,               -- 聯絡電話 9
    -- 以下這塊處理其它欄位
    `Remark` VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,                   -- 備註 24
    `Update_Time` DATETIME DEFAULT CURRENT_TIMESTAMP                                                     -- raw data 寫入時間
    -- 等待確認 Insert time, 讓行政院工程委員會與稅籍專案邏輯相同
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
