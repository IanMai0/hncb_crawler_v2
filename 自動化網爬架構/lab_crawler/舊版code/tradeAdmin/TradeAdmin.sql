CREATE DATABASE `CrawlerTestDB`;

USE `CrawlerTestDB`;

-- 建立表格 國貿局基本資料主要表格
CREATE TABLE `CrawlerTestDB`.`tradeAdmin_basic_info` (
    uid CHAR(8) NOT NULL COMMENT '統一編號（8碼字串）',
    company_name VARCHAR(255) NULL COMMENT '公司名稱',
    phone VARCHAR(50) NULL COMMENT '電話',
    import_eligibility VARCHAR(50) NULL COMMENT '進口資格',
    export_eligibility VARCHAR(50) NULL COMMENT '出口資格',
    query_time DATETIME NOT NULL COMMENT '查詢時間',
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='貿易管理系統基本資料';


-- 建立表格 國貿局實績資料主要表格
CREATE TABLE IF NOT EXISTS `CrawlerTestDB`.`tradeAdmin_grade_info` (
    uid              CHAR(8)      NOT NULL COMMENT '統一編號（8 碼字串）',
    period_text      VARCHAR(20)  NOT NULL COMMENT '時間週期，如 114 01-06 / 113 01-12',
    company_name     VARCHAR(200)     NULL COMMENT '公司名稱',
    company_name_en  VARCHAR(200)     NULL COMMENT '公司名稱英文',

    -- 原始級距字串
    import_band      VARCHAR(20)      NULL COMMENT '總進口實績（級距字串，如 A (>=10), L (>0,<0.5), M (=0)）',
    export_band      VARCHAR(20)      NULL COMMENT '總出口實績（級距字串）',

    -- 從級距字串自動萃取的代碼（A~M）
    import_band_code CHAR(1) GENERATED ALWAYS AS (NULLIF(LEFT(import_band, 1), '')) VIRTUAL,
    export_band_code CHAR(1) GENERATED ALWAYS AS (NULLIF(LEFT(export_band, 1), '')) VIRTUAL,

    -- 預留數值上下限（若之後把級距對應為數值，可回填）
    import_val_lo    DECIMAL(10,3)    NULL COMMENT '進口實績下限（單位同網站定義）',
    import_val_hi    DECIMAL(10,3)    NULL COMMENT '進口實績上限（NULL 表示無上限）',
    export_val_lo    DECIMAL(10,3)    NULL COMMENT '出口實績下限',
    export_val_hi    DECIMAL(10,3)    NULL COMMENT '出口實績上限',

    stat_year_roc    SMALLINT         NULL COMMENT '統計時間年（民國年，如 114）',
    query_time       DATETIME     NOT NULL COMMENT '查詢時間',

    PRIMARY KEY (uid, period_text, stat_year_roc, query_time),

    KEY ix_year (stat_year_roc),
    KEY ix_query_time (query_time),
    KEY ix_uid_year (uid, stat_year_roc),
    KEY ix_import_code (import_band_code),
    KEY ix_export_code (export_band_code),

    -- 若你的 MySQL 版本支援 CHECK，可加上下面兩條；不支援可移除
    CHECK (import_band_code IN ('A','B','C','D','E','F','G','H','I','J','K','L','M') OR import_band_code IS NULL),
    CHECK (export_band_code IN ('A','B','C','D','E','F','G','H','I','J','K','L','M') OR export_band_code IS NULL)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='貿易管理系統—進出口實績級距（A~M）與對應上下限';

-- 建立前哨站表格：國貿局基本資料（原始）
CREATE TABLE IF NOT EXISTS `CrawlerTestDB`.`tradeAdmin_stage_basic_info` (
    uid_raw          VARCHAR(50)   NULL COMMENT '原始統一編號字串（可能不是 8 碼，所以先允許任意長度）',
    company_name_raw VARCHAR(255)  NULL COMMENT '公司名稱（原始爬取）',
    phone_raw        VARCHAR(100)  NULL COMMENT '電話（原始字串，未清理）',
    import_eligibility_raw VARCHAR(10) NULL COMMENT '進口資格（原始字串）',
    export_eligibility_raw VARCHAR(10) NULL COMMENT '出口資格（原始字串）',
    query_time_raw   VARCHAR(50)   NOT NULL COMMENT '查詢時間（原始字串格式，未轉換成 datetime）',

    -- ETL 流程控制欄位
    etl_status       VARCHAR(20)   DEFAULT 'pending' COMMENT 'ETL 狀態（pending/converted/failed）',
    etl_message      VARCHAR(500)  NULL COMMENT 'ETL 備註訊息（錯誤描述或處理記錄）'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='前哨站表格：保存國貿局基本資料原始數據，待 ETL 清理後再進正式表格';

-- 建立前哨站表格：國貿局實績資料（原始）
CREATE TABLE IF NOT EXISTS `CrawlerTestDB`.`tradeAdmin_stage_grade_info` (
    uid_raw          VARCHAR(50)   NULL COMMENT '原始統一編號字串（可能不是 8 碼）',
    period_text_raw  VARCHAR(50)   NULL COMMENT '時間週期原始字串',
    company_name_raw VARCHAR(255)  NULL COMMENT '公司名稱（原始）',
    company_name_en_raw VARCHAR(255) NULL COMMENT '公司英文名稱（原始）',
    import_band_raw  VARCHAR(50)   NULL COMMENT '總進口實績（原始字串 A (>=10), M (=0)）',
    export_band_raw  VARCHAR(50)   NULL COMMENT '總出口實績（原始字串）',
    stat_year_raw    VARCHAR(20)   NULL COMMENT '統計時間年（原始字串，民國格式）',
    query_time_raw   VARCHAR(50)   NOT NULL COMMENT '查詢時間（原始字串格式）',

    -- ETL 流程控制欄位
    etl_status       VARCHAR(20)   DEFAULT 'pending' COMMENT 'ETL 狀態（pending/converted/failed）',
    etl_message      VARCHAR(500)  NULL COMMENT 'ETL 備註訊息（錯誤描述或處理記錄）'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='前哨站表格：保存國貿局進出口實績原始數據，待 ETL 清理後再進正式表格';
