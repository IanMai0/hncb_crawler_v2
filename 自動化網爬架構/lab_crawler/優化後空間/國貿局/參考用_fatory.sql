CREATE DATABASE `CrawlerTestDB`;

USE `CrawlerTestDB`;

-- 建立表格 工廠主要表格
CREATE TABLE `CrawlerTestDB`.`FactoryInfo` (
    `Party_ID` VARCHAR(8) PRIMARY KEY COMMENT '統一編號',
    `Party_Name` VARCHAR(255) COMMENT '工廠名稱',
    `Party_Addr` VARCHAR(255) COMMENT '工廠地址',
    `Party_Reg_Code` VARCHAR(50) COMMENT '工廠登記編號',
    `Party_Licence_Code` VARCHAR(20) COMMENT '工廠設立許可案號',
    `Party_Adr_Dist` VARCHAR(100) COMMENT '工廠市鎮鄉村里',
    `Person_Name` VARCHAR(20) COMMENT '工廠負責人姓名',
    `Party_Type` VARCHAR(20) COMMENT '工廠組織型態',
    `Party_Status` VARCHAR(20) COMMENT '工廠登記狀態',
    `Industry_Category` VARCHAR(255) COMMENT '產業類別',
    `Main_Product` VARCHAR(255) COMMENT '主要產品',
    `Industry_Code` VARCHAR(255) COMMENT '產業類別代號',
    `Pro_Code` VARCHAR(255) COMMENT '主要產品代號',
    `PS` VARCHAR(255) COMMENT '異常註記',
	`batch_time` DATETIME COMMENT '匯入批次時間'
);

-- 建立初步匯入表格
CREATE TABLE IF NOT EXISTS `CrawlerTestDB`.`FactoryInfo_stage` (
    `工廠名稱` VARCHAR(500),
    `工廠登記編號` VARCHAR(500),
    `工廠設立許可案號` VARCHAR(500),
    `工廠地址` VARCHAR(500),
    `工廠市鎮鄉村里` VARCHAR(500),
    `工廠負責人姓名` VARCHAR(500),
    `統一編號` VARCHAR(500),
    `工廠組織型態` VARCHAR(500),
    `工廠登記狀態` VARCHAR(500),
    `產業類別` VARCHAR(500),
    `主要產品` VARCHAR(500),
    `產業類別代號` VARCHAR(500),
    `主要產品代號` VARCHAR(500),
    `__異常註記__` VARCHAR(500),
 	`batch_time` DATETIME COMMENT '匯入批次時間'
);

-- 建立表格_進正式表前哨站
CREATE TABLE `CrawlerTestDB`.`FactoryInfo_stage2` (
    `Party_ID` VARCHAR(8),
    `Party_Name` VARCHAR(255),
    `Party_Addr` VARCHAR(255),
    `Party_Reg_Code` VARCHAR(20),
    `Party_Licence_Code` VARCHAR(20),
    `Party_Adr_Dist` VARCHAR(255),
    `Person_Name` VARCHAR(20),
    `Party_Type` VARCHAR(20),
    `Party_Status` VARCHAR(20),
    `Industry_Category` VARCHAR(255),
    `Main_Product` VARCHAR(255),
    `Industry_Code` VARCHAR(255),
    `Pro_Code` VARCHAR(255),
    `PS` VARCHAR(255),
    `batch_time` DATETIME COMMENT '匯入批次時間'
);


-- 這邊請從 DBeaver 直接將裝載 Raw Data 的 CsvFile import 進入表內
-- 隨後針對這批資料的 batch_time 補上 匯入批次時間 (範例：2025年5月)
UPDATE `CrawlerTestDB`.`FactoryInfo_stage`
SET batch_time = '2025-05-01 00:00:00'  -- 這裡寫 匯入的批次時間
WHERE batch_time IS NULL;

-- 合格資料 → 正式表前哨站
INSERT INTO `CrawlerTestDB`.`FactoryInfo_stage2` (
    Party_ID, Party_Name, Party_Addr, Party_Reg_Code, Party_Licence_Code,
    Party_Adr_Dist, Person_Name, Party_Type, Party_Status,
    Industry_Category, Main_Product, Industry_Code, Pro_Code, PS, batch_time
)
SELECT 
    `統一編號`,        -- 對應 Party_ID
    `工廠名稱`,        -- 對應 Party_Name
    `工廠地址`,        -- 對應 Party_Addr
    `工廠登記編號`,    -- 對應 Party_Reg_Code
    `工廠設立許可案號`,-- 對應 Party_Licence_Code
    `工廠市鎮鄉村里`,  -- 對應 Party_Adr_Dist
    `工廠負責人姓名`,  -- 對應 Person_Name
    `工廠組織型態`,    -- 對應 Party_Type
    `工廠登記狀態`,    -- 對應 Party_Status
    `產業類別`,        -- 對應 Industry_Category
    `主要產品`,        -- 對應 Main_Product
    `產業類別代號`,    -- 對應 Industry_Code
    `主要產品代號`,    -- 對應 Pro_Code
    `__異常註記__`,    -- 對應 PS
    batch_time         -- ✅ 直接把 stage 的批次時間帶進去
FROM `CrawlerTestDB`.`FactoryInfo_stage`
WHERE batch_time = '2025-06-01 00:00:00'   -- ✅ 限定只處理 2506 這批
  AND CHAR_LENGTH(`統一編號`) = 8
  AND `統一編號` REGEXP '^[0-9]+$'
  AND CHAR_LENGTH(`工廠登記編號`) <= 20
  AND CHAR_LENGTH(`工廠設立許可案號`) <= 20
  AND CHAR_LENGTH(`工廠負責人姓名`) <= 20
  AND CHAR_LENGTH(`工廠組織型態`) <= 20
  AND CHAR_LENGTH(`工廠登記狀態`) <= 20
  AND CHAR_LENGTH(`主要產品代號`) <= 255
  AND CHAR_LENGTH(`工廠名稱`) <= 255
  AND CHAR_LENGTH(`工廠地址`) <= 255
  AND CHAR_LENGTH(`工廠市鎮鄉村里`) <= 255
  AND CHAR_LENGTH(`產業類別`) <= 255
  AND CHAR_LENGTH(`主要產品`) <= 255
  AND CHAR_LENGTH(`產業類別代號`) <= 255;


-- 壞資料處裡, 找出所有超過限制的資料, 一次檢查
-- 初次建表, 一次檢查
CREATE TABLE IF NOT EXISTS FactoryInfo_error_log (
    `工廠名稱` VARCHAR(500),
    `工廠登記編號` VARCHAR(500),
    `工廠設立許可案號` VARCHAR(500),
    `工廠地址` VARCHAR(500),
    `工廠市鎮鄉村里` VARCHAR(500),
    `工廠負責人姓名` VARCHAR(500),
    `統一編號` VARCHAR(500),
    `工廠組織型態` VARCHAR(500),
    `工廠登記狀態` VARCHAR(500),
    `產業類別` VARCHAR(500),
    `主要產品` VARCHAR(500),
    `產業類別代號` VARCHAR(500),
    `主要產品代號` VARCHAR(500),
    `__異常註記__` VARCHAR(500),
    `error_batch_time` DATETIME COMMENT '錯誤批次時間'
);

-- 之後每次匯入錯誤資料 → 用 INSERT INTO ... SELECT 加上時間：
INSERT INTO `CrawlerTestDB`.`FactoryInfo_error_log` (
    `工廠名稱`, `工廠登記編號`, `工廠設立許可案號`,
    `工廠地址`, `工廠市鎮鄉村里`, `工廠負責人姓名`,
    `統一編號`, `工廠組織型態`, `工廠登記狀態`,
    `產業類別`, `主要產品`, `產業類別代號`,
    `主要產品代號`, `__異常註記__`, `error_batch_time`
)
SELECT 
    `工廠名稱`, `工廠登記編號`, `工廠設立許可案號`,
    `工廠地址`, `工廠市鎮鄉村里`, `工廠負責人姓名`,
    `統一編號`, `工廠組織型態`, `工廠登記狀態`,
    `產業類別`, `主要產品`, `產業類別代號`,
    `主要產品代號`, `__異常註記__`,
    batch_time   -- ✅ 用 stage 的 batch_time 填 error_log 的 error_batch_time
FROM `CrawlerTestDB`.`FactoryInfo_stage`
WHERE batch_time = '2025-06-01 00:00:00'   -- ✅ 限定只處理 特定時間軸
  AND (
        CHAR_LENGTH(`統一編號`) <> 8
     OR `統一編號` NOT REGEXP '^[0-9]+$'
     OR CHAR_LENGTH(`工廠登記編號`) > 20
     OR CHAR_LENGTH(`工廠設立許可案號`) > 20
     OR CHAR_LENGTH(`工廠負責人姓名`) > 20
     OR CHAR_LENGTH(`工廠組織型態`) > 20
     OR CHAR_LENGTH(`工廠登記狀態`) > 20
     OR CHAR_LENGTH(`主要產品代號`) > 255
     OR CHAR_LENGTH(`工廠名稱`) > 255
     OR CHAR_LENGTH(`工廠地址`) > 255
     OR CHAR_LENGTH(`工廠市鎮鄉村里`) > 255
     OR CHAR_LENGTH(`產業類別`) > 255
     OR CHAR_LENGTH(`主要產品`) > 255
     OR CHAR_LENGTH(`產業類別代號`) > 255
  );

-- 計算數量_成功與失敗
SELECT 
    '2025-05' AS batch_month,
    (SELECT COUNT(*) 
     FROM `CrawlerTestDB`.`FactoryInfo_stage2`
     WHERE batch_time = '2025-05-01 00:00:00') AS success_count,
    (SELECT COUNT(*) 
     FROM `CrawlerTestDB`.`FactoryInfo_error_log`
     WHERE error_batch_time = '2025-05-01 00:00:00') AS error_count;

-- 計算機率_成功與失敗
SELECT 
    '2025-05' AS batch_month,
    success.success_count,
    err.error_count,
    ROUND(success.success_count / (success.success_count + err.error_count) * 100, 2) AS success_rate,
    ROUND(err.error_count / (success.success_count + err.error_count) * 100, 2) AS error_rate
FROM 
    (SELECT COUNT(*) AS success_count 
     FROM `CrawlerTestDB`.`FactoryInfo_stage2`
     WHERE batch_time = '2025-05-01 00:00:00') success,
    (SELECT COUNT(*) AS error_count 
     FROM `CrawlerTestDB`.`FactoryInfo_error_log`
     WHERE error_batch_time = '2025-05-01 00:00:00') err;

-- 看 各批次有多少筆（例如 2025-04、2025-05、2025-06）：
SELECT batch_time, COUNT(*) AS row_count
FROM `CrawlerTestDB`.`FactoryInfo_stage2`
GROUP BY batch_time
ORDER BY batch_time;

-- 看 各批次有多少筆（例如 2025-04、2025-05、2025-06）：
SELECT party_name, COUNT(*) AS row_count
FROM `CrawlerTestDB`.`FactoryInfo_stage2`
GROUP BY party_name
ORDER BY party_name;

-- 看統編最多 前幾名
SELECT 
    Party_ID,
    COUNT(*) AS freq
FROM `CrawlerTestDB`.`FactoryInfo_stage2`
-- where batch_time = '2025-06-01 00:00:00'
GROUP BY Party_ID
ORDER BY freq DESC
LIMIT 5;


-- ===========================================================================================
-- 成功 / 失敗
-- 針對 169,911 筆稅籍 Data 統編去做比對, 抓出這其中哪些統編有工廠(只抓6月)  
-- 1. 169,911 統編先寫進來
-- 2. 比對抓工廠六月數據
-- 3. 成功：附上Data(一對多 一個統編對多個工廠資料)、失敗：無工廠資料

-- 先跑成功資料（有工廠的）
SELECT 
    i.Party_ID,
    f.*
FROM CrawlerTestDB.target250808 i   -- 169,911 統編清單
JOIN CrawlerTestDB.FactoryInfo_stage2 f
    ON i.Party_ID = f.Party_ID
WHERE f.batch_time = '2025-06-01 00:00:00';

-- 檢查成功資料筆數
SELECT COUNT(*) AS success_count
FROM CrawlerTestDB.target250808 i
JOIN CrawlerTestDB.FactoryInfo_stage2 f
    ON i.Party_ID = f.Party_ID
WHERE f.batch_time = '2025-06-01 00:00:00';


-- 再跑失敗資料(無工廠的)
SELECT 
    i.Party_ID
FROM CrawlerTestDB.target250808 i
LEFT JOIN CrawlerTestDB.FactoryInfo_stage2 f
    ON i.Party_ID = f.Party_ID 
   AND f.batch_time = '2025-06-01 00:00:00'
WHERE f.Party_ID IS NULL;

-- 檢查失敗資料筆數
SELECT COUNT(*) AS fail_count
FROM CrawlerTestDB.target250808 i
LEFT JOIN CrawlerTestDB.FactoryInfo_stage2 f
    ON i.Party_ID = f.Party_ID 
   AND f.batch_time = '2025-06-01 00:00:00'
WHERE f.Party_ID IS NULL;

-- 合併成一個「成功 / 失敗」標記結果 (所有欄位輸出)
SELECT 
    i.Party_ID,   -- 保留清單的 Party_ID
    CASE WHEN f.Party_ID IS NOT NULL THEN '成功' ELSE '無工廠資料' END AS match_status,
    f.Party_Name,
    f.Party_Addr,
    f.Party_Reg_Code,
    f.Party_Licence_Code,
    f.Party_Adr_Dist,
    f.Person_Name,
    f.Party_Type,
    f.Party_Status,
    f.Industry_Category,
    f.Main_Product,
    f.Industry_Code,
    f.Pro_Code,
    f.PS,
    f.batch_time
FROM CrawlerTestDB.target250808 i
LEFT JOIN CrawlerTestDB.FactoryInfo_stage2 f
    ON i.Party_ID = f.Party_ID
   AND f.batch_time = '2025-06-01 00:00:00';

-- 彙總報表
-- 統計成功 / 失敗數量
SELECT 
    CASE WHEN f.Party_ID IS NOT NULL THEN '成功' ELSE '無工廠資料' END AS match_status,
    COUNT(*) AS total_count
FROM CrawlerTestDB.target250808 i
LEFT JOIN CrawlerTestDB.FactoryInfo_stage2 f
    ON i.Party_ID = f.Party_ID
   AND f.batch_time = '2025-06-01 00:00:00'
GROUP BY match_status;

-- 詳細明細
-- 合併明細（避免 Party_ID 重複）
SELECT 
    i.Party_ID,   -- 稅籍清單的 Party_ID
    CASE WHEN f.Party_ID IS NOT NULL THEN '成功' ELSE '無工廠資料' END AS match_status,
    f.Party_Name,
    f.Party_Addr,
    f.Party_Reg_Code,
    f.Party_Licence_Code,
    f.Party_Adr_Dist,
    f.Person_Name,
    f.Party_Type,
    f.Party_Status,
    f.Industry_Category,
    f.Main_Product,
    f.Industry_Code,
    f.Pro_Code,
    f.PS,
    f.batch_time
FROM CrawlerTestDB.target250808 i
LEFT JOIN CrawlerTestDB.FactoryInfo_stage2 f
    ON i.Party_ID = f.Party_ID
   AND f.batch_time = '2025-06-01 00:00:00'
ORDER BY match_status DESC, i.Party_ID;

-- ================================================
-- 根據目標統編 處理 error_log
-- 比對 目標統編 Data 是否存在 error_log
SELECT 
    i.Party_ID,
    f.*
FROM CrawlerTestDB.target250808 i   -- 169,911 統編清單
JOIN CrawlerTestDB.factoryinfo_error_log f
    ON i.Party_ID = f.統一編號
WHERE f.error_batch_time = '2025-06-01 00:00:00';

-- 合併成一個「成功 / 失敗」標記結果 (所有欄位輸出), 處裡 error_log Data
SELECT 
    i.Party_ID,   -- 保留清單的 Party_ID
    CASE WHEN f.統一編號 IS NOT NULL THEN '成功' ELSE '無工廠資料' END AS match_status,
    f.工廠名稱 as Party_Name,
    f.工廠地址 AS Party_Addr,
    f.工廠登記編號 AS Party_Reg_Code,
    f.工廠設立許可案號 AS Party_Licence_Code,
    f.工廠市鎮鄉村里 AS Party_Adr_Dist,
    f.工廠負責人姓名 as Person_Name,
    f.工廠組織型態 as Party_Type,
    f.工廠登記狀態 as Party_Status,
    f.產業類別 as Industry_Category,
    f.主要產品 as Main_Product,
    f.產業類別代號 as Industry_Code,
    f.主要產品代號 as Pro_Code,
    f.__異常註記__  as PS,
    f.error_batch_time as batch_time
FROM CrawlerTestDB.target250808 i
LEFT JOIN CrawlerTestDB.factoryinfo_error_log f
    ON i.Party_ID = f.統一編號
   AND f.error_batch_time = '2025-06-01 00:00:00';


-- 只撈出符合查詢目標
SELECT 
    i.Party_ID,   -- 保留清單的 Party_ID
    '成功' AS match_status,
    f.工廠名稱       AS Party_Name,
    f.工廠地址       AS Party_Addr,
    f.工廠登記編號   AS Party_Reg_Code,
    f.工廠設立許可案號 AS Party_Licence_Code,
    f.工廠市鎮鄉村里 AS Party_Adr_Dist,
    f.工廠負責人姓名 AS Person_Name,
    f.工廠組織型態   AS Party_Type,
    f.工廠登記狀態   AS Party_Status,
    f.產業類別       AS Industry_Category,
    f.主要產品       AS Main_Product,
    f.產業類別代號   AS Industry_Code,
    f.主要產品代號   AS Pro_Code,
    f.__異常註記__   AS PS,
    f.error_batch_time AS batch_time
FROM CrawlerTestDB.target250808 i
JOIN CrawlerTestDB.factoryinfo_error_log f   -- 改成 JOIN 直接只留成功
    ON i.Party_ID = f.統一編號
   AND f.error_batch_time = '2025-06-01 00:00:00';

-- ==========================================================
-- 資料品質報告

-- 總攬概況分析
-- 第一步：計算每個 Party_ID 出現的次數
WITH id_counts AS (
    SELECT
        `Party_ID`,
        COUNT(*) AS cnt
    FROM
        `quality_report_data`
    GROUP BY
        `Party_ID`
),  -- 179,774
-- 第二步：根據出現次數，將每個 Party_ID 歸類為重複或不重複
summary AS (
    SELECT
        COUNT(CASE WHEN T2.cnt > 1 THEN 1 END) AS duplicated_ids,
        COUNT(CASE WHEN T2.cnt = 1 THEN 1 END) AS unique_ids
    FROM
        `quality_report_data` AS T1
    LEFT JOIN
        id_counts AS T2 ON T1.Party_ID = T2.Party_ID
    GROUP BY
        T1.Party_ID
    ORDER BY
        T1.Party_ID
)
-- 第三步：計算總數、成功匹配與無工廠的筆數，並將所有指標匯總
SELECT
    COUNT(*) AS total_records,
    SUM(CASE WHEN `match_status` = '成功' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN `match_status` = '無工廠' THEN 1 ELSE 0 END) AS no_factory_count,
    SUM(CASE WHEN `match_status` NOT IN ('成功', '無工廠') THEN 1 ELSE 0 END) AS other_status_count,
    (SELECT SUM(duplicated_ids) FROM summary) AS duplicated_party_ids,
    (SELECT SUM(unique_ids) FROM summary) AS unique_party_ids
FROM
    `quality_report_data`;


-- 各欄位缺失值統計
SELECT
    COUNT(CASE WHEN `Party_ID` IS NULL OR `Party_ID` = '' THEN 1 END) AS missing_Party_ID,
    COUNT(CASE WHEN `match_status` IS NULL OR `match_status` = '' THEN 1 END) AS missing_match_status,
    COUNT(CASE WHEN `Party_Name` IS NULL OR `Party_Name` = '' THEN 1 END) AS missing_Party_Name,
    COUNT(CASE WHEN `Party_Addr` IS NULL OR `Party_Addr` = '' THEN 1 END) AS missing_Party_Addr,
    COUNT(CASE WHEN `Party_Reg_Code` IS NULL OR `Party_Reg_Code` = '' THEN 1 END) AS missing_Party_Reg_Code,
    COUNT(CASE WHEN `Party_Licence_Code` IS NULL OR `Party_Licence_Code` = '' THEN 1 END) AS missing_Party_Licence_Code,
    COUNT(CASE WHEN `Party_Adr_Dist` IS NULL OR `Party_Adr_Dist` = '' THEN 1 END) AS missing_Party_Adr_Dist,
    COUNT(CASE WHEN `Person_Name` IS NULL OR `Person_Name` = '' THEN 1 END) AS missing_Person_Name,
    COUNT(CASE WHEN `Party_Type` IS NULL OR `Party_Type` = '' THEN 1 END) AS missing_Party_Type,
    COUNT(CASE WHEN `Party_Status` IS NULL OR `Party_Status` = '' THEN 1 END) AS missing_Party_Status,
    COUNT(CASE WHEN `Industry_Category` IS NULL OR `Industry_Category` = '' THEN 1 END) AS missing_Industry_Category,
    COUNT(CASE WHEN `Main_Product` IS NULL OR `Main_Product` = '' THEN 1 END) AS missing_Main_Product,
    COUNT(CASE WHEN `Industry_Code` IS NULL OR `Industry_Code` = '' THEN 1 END) AS missing_Industry_Code,
    COUNT(CASE WHEN `Pro_Code` IS NULL OR `Pro_Code` = '' THEN 1 END) AS missing_Pro_Code,
    COUNT(CASE WHEN `PS` IS NULL OR `PS` = '' THEN 1 END) AS missing_PS
FROM
    `CrawlerTestDB`.`quality_report_data`;


-- 4. 整體完整性檢查
-- 工廠資料品質報告 (針對 2025-06 批次)
WITH base AS (
    -- 來源清單
    SELECT COUNT(DISTINCT Party_ID) AS id_count_in_list
    FROM CrawlerTestDB.target250808
),
result AS (
    -- 比對後的總筆數
    SELECT 
        COUNT(*) AS total_records,
        SUM(CASE WHEN match_status = '成功' THEN 1 ELSE 0 END) AS success_count,
        SUM(CASE WHEN match_status = '無工廠資料' THEN 1 ELSE 0 END) AS no_factory_count
    FROM CrawlerTestDB.quality_report_data
    -- WHERE batch_time = '2025-06-01 00:00:00'
),
multi AS (
    -- 一對多：同一 Party_ID 出現超過一次
    SELECT COUNT(*) AS multi_ids
    FROM (
        SELECT Party_ID
        FROM CrawlerTestDB.quality_report_data
        -- WHERE batch_time = '2025-06-01 00:00:00'
        GROUP BY Party_ID
        HAVING COUNT(*) > 1
    ) t
)
SELECT
    b.id_count_in_list         AS total_ids_in_list,   -- 清單總數 (169,911)
    r.total_records            AS total_records,       -- 比對結果總數 (179,774)
    r.success_count            AS success_count,       -- 成功筆數
    r.no_factory_count         AS no_factory_count,    -- 無工廠筆數
    m.multi_ids                AS multi_party_ids      -- 一對多的 Party_ID 數量 (16,570)
FROM base b
CROSS JOIN result r
CROSS JOIN multi m;

-- 5. 一對一 vs 一對多 分布數量
-- 工廠資料 一對一 vs 一對多 分布
WITH counts AS (
    SELECT Party_ID, COUNT(*) AS freq
    FROM CrawlerTestDB.quality_report_data
    WHERE batch_time = '2025-06-01 00:00:00'
      AND match_status = '成功'
    GROUP BY Party_ID
)
SELECT 
    SUM(CASE WHEN freq = 1 THEN 1 ELSE 0 END) AS one_to_one_ids,  -- 一對一 Party_ID 數量
    SUM(CASE WHEN freq > 1 THEN 1 ELSE 0 END) AS one_to_many_ids, -- 一對多 Party_ID 數量
    COUNT(*) AS total_ids   -- 總 Party_ID 數量 (應該等於 success_count 的 distinct 值)
FROM counts;
