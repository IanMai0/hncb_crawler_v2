-- 臨時表
CREATE TABLE `pcc_excellent_tmp` (
    -- 以下這塊處理廠商相關欄位
    `Corporation_number` VARCHAR(20) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,             -- 廠商統編/廠商代碼 0
    `Corporation_name` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,        -- 廠商名稱 1
    `Corporation_address` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,     -- 廠商地址 2
    -- 以下這塊處理時間相關欄位
    `Effective_date` DATETIME DEFAULT NULL,                                                               -- 獎勵起始日期 10
    `Expire_date` DATETIME DEFAULT NULL,                                                                  -- 獎勵終止日期 11    
    -- 以下這塊處理標案相關欄位
    `Judgment_no` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,             -- 評優良廠商依據之規定 9
    -- 以下這塊處理機關相關欄位
    `Announce_agency_no` VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,       -- 通知主管機關文號 12
    `Announce_agency_name` VARCHAR(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,     -- 機關名稱/刊登機關名稱 4
    `Announce_agency_code` VARCHAR(55) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,    -- 機關代碼/刊登機關代碼 3
    `Announce_agency_address` VARCHAR(55) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,  -- 機關地址 5
    `Contact_person` VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,           -- 聯絡人/機關聯絡人 6
    `Contact_phone` VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,            -- 聯絡電話/機關聯絡人電話/聯絡人電話 7
    `Announce_agency_mail` VARCHAR(55) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,     -- 機關聯絡人電子郵件信箱 8
    -- 以下這塊處理其它欄位
    `Remark` VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,                   -- 備註 13
    `Update_Time` DATETIME DEFAULT CURRENT_TIMESTAMP                                                     -- raw data 寫入時間
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- 正式表
CREATE TABLE `PccExcellent` (
    -- 以下這塊處理廠商相關欄位
    `Corporation_number` VARCHAR(20) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,             -- 廠商統編/廠商代碼 0
    `Corporation_name` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,        -- 廠商名稱 1
    `Corporation_address` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,     -- 廠商地址 2
    -- 以下這塊處理時間相關欄位
    `Effective_date` DATETIME DEFAULT NULL,                                                               -- 獎勵起始日期 10
    `Expire_date` DATETIME DEFAULT NULL,                                                                  -- 獎勵終止日期 11    
    -- 以下這塊處理標案相關欄位
    `Judgment_no` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,             -- 評優良廠商依據之規定 9
    -- 以下這塊處理機關相關欄位
    `Announce_agency_no` VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,       -- 通知主管機關文號 12
    `Announce_agency_name` VARCHAR(25) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,     -- 機關名稱/刊登機關名稱 4
    `Announce_agency_code` VARCHAR(55) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,    -- 機關代碼/刊登機關代碼 3
    `Announce_agency_address` VARCHAR(55) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,  -- 機關地址 5
    `Contact_person` VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,           -- 聯絡人/機關聯絡人 6
    `Contact_phone` VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,            -- 聯絡電話/機關聯絡人電話/聯絡人電話 7
    `Announce_agency_mail` VARCHAR(55) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,     -- 機關聯絡人電子郵件信箱 8
    -- 以下這塊處理其它欄位
    `Remark` VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,                   -- 備註 13
    `Update_Time` DATETIME DEFAULT CURRENT_TIMESTAMP                                                      -- raw data 寫入時間
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;