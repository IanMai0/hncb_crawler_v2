-- 1) 原始資料暫存：tmp_rawData（原封不動）
CREATE TABLE IF NOT EXISTS crawlerdb.tmp_rawData (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

  -- 批次與來源
  run_id VARCHAR(32) NOT NULL,                 -- RUN_YYYYMMDD_HHMMSS
  source_url VARCHAR(500) NOT NULL,
  local_zip_path VARCHAR(500) NOT NULL,
  local_csv_path VARCHAR(500) NOT NULL,

  -- 時間資訊
  downloaded_at DATETIME(6) NOT NULL,          -- 下載時間
  file_date DATE NULL,                         -- CSV 第2行日期（15-JAN-26 → 2026-01-15）

  -- CSV 結構
  row_num INT NOT NULL,                        -- CSV 行號（從 1 開始）
  row_type ENUM('HEADER','META','DATA') NOT NULL,

  -- 原始 16 欄（全部 TEXT，不轉型）
  c01 TEXT NULL,
  c02 TEXT NULL,
  c03 TEXT NULL,
  c04 TEXT NULL,
  c05 TEXT NULL,
  c06 TEXT NULL,
  c07 TEXT NULL,
  c08 TEXT NULL,
  c09 TEXT NULL,
  c10 TEXT NULL,
  c11 TEXT NULL,
  c12 TEXT NULL,
  c13 TEXT NULL,
  c14 TEXT NULL,
  c15 TEXT NULL,
  c16 TEXT NULL,

  loaded_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

  PRIMARY KEY (id),
  KEY idx_run_row (run_id, row_num),
  KEY idx_run_type (run_id, row_type),
  KEY idx_run_filedate (run_id, file_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- 2) 清洗後暫存：tmp_taxInfo（結構化）
CREATE TABLE IF NOT EXISTS crawlerdb.tmp_taxInfo (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

  -- 批次資訊
  run_id VARCHAR(32) NOT NULL,                 -- RUN_YYYYMMDD_HHMMSS
  source_file_date DATE NOT NULL,              -- CSV 第2行日期
  row_num INT NOT NULL,                        -- 對應 raw 的 row_num

  -- 核心欄位（ETL 後）
  party_addr VARCHAR(300) NULL,
  party_id VARCHAR(20) NOT NULL,
  parent_party_id VARCHAR(20) NULL,
  party_name VARCHAR(200) NULL,
  paidin_capital BIGINT NULL,
  setup_date DATE NULL,
  party_type VARCHAR(50) NULL,
  use_invoice CHAR(1) NULL,

  ind_code VARCHAR(20) NULL,
  ind_name VARCHAR(100) NULL,
  ind_code1 VARCHAR(20) NULL,
  ind_name1 VARCHAR(100) NULL,
  ind_code2 VARCHAR(20) NULL,
  ind_name2 VARCHAR(100) NULL,
  ind_code3 VARCHAR(20) NULL,
  ind_name3 VARCHAR(100) NULL,

  -- 比對用
  row_hash CHAR(64) NOT NULL,                  -- SHA256（ETL 後內容）

  etl_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

  PRIMARY KEY (id),

  -- 同一批次防止重跑插爆
  UNIQUE KEY uq_run_row (run_id, row_num),

  -- 查詢與維運索引
  KEY idx_run_party (run_id, party_id),
  KEY idx_party_hash (party_id, row_hash),
  KEY idx_file_date (source_file_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



-- 3) 主表（歷史版本）：taxInfo
CREATE TABLE IF NOT EXISTS crawlerdb.taxInfo (
  party_id VARCHAR(20) NOT NULL,

  party_addr VARCHAR(300) NULL,
  parent_party_id VARCHAR(20) NULL,
  party_name VARCHAR(200) NULL,
  paidin_capital BIGINT NULL,
  setup_date DATE NULL,
  party_type VARCHAR(50) NULL,
  use_invoice CHAR(1) NULL,

  ind_code VARCHAR(20) NULL,
  ind_name VARCHAR(100) NULL,
  ind_code1 VARCHAR(20) NULL,
  ind_name1 VARCHAR(100) NULL,
  ind_code2 VARCHAR(20) NULL,
  ind_name2 VARCHAR(100) NULL,
  ind_code3 VARCHAR(20) NULL,
  ind_name3 VARCHAR(100) NULL,

  row_hash CHAR(64) NOT NULL,

  source_file_date DATE NOT NULL,              -- 最新來源檔日期
  last_run_id VARCHAR(32) NOT NULL,             -- 最後一次更新該筆的 run

  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
    ON UPDATE CURRENT_TIMESTAMP(6),

  PRIMARY KEY (party_id),
  KEY idx_source_date (source_file_date),
  KEY idx_row_hash (row_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
