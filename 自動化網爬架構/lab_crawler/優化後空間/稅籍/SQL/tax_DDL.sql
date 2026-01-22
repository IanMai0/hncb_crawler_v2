CREATE TABLE IF NOT EXISTS `crawlerdb`.`tmp_rawData` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

  `run_id` VARCHAR(32) NOT NULL,
  `source_url` VARCHAR(500) NOT NULL,
  `local_zip_path` VARCHAR(500) NOT NULL,
  `local_csv_path` VARCHAR(500) NOT NULL,

  `downloaded_at` DATETIME(6) NOT NULL,
  `file_date` DATE NULL,

  `row_num` INT NOT NULL,
  `row_type` ENUM('HEADER','META','DATA') NOT NULL,

  `c01` TEXT NULL,
  `c02` TEXT NULL,
  `c03` TEXT NULL,
  `c04` TEXT NULL,
  `c05` TEXT NULL,
  `c06` TEXT NULL,
  `c07` TEXT NULL,
  `c08` TEXT NULL,
  `c09` TEXT NULL,
  `c10` TEXT NULL,
  `c11` TEXT NULL,
  `c12` TEXT NULL,
  `c13` TEXT NULL,
  `c14` TEXT NULL,
  `c15` TEXT NULL,
  `c16` TEXT NULL,

  `loaded_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`id`),
  KEY `idx_run_row` (`run_id`, `row_num`),
  KEY `idx_run_type` (`run_id`, `row_type`),
  KEY `idx_run_filedate` (`run_id`, `file_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



CREATE TABLE IF NOT EXISTS `crawlerdb`.`tmp_taxInfo` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

  `run_id` VARCHAR(32) NOT NULL,
  `source_file_date` DATE NOT NULL,
  `row_num` INT NOT NULL,

  `party_addr` VARCHAR(300) NULL,
  `party_id` VARCHAR(20) NOT NULL,
  `parent_party_id` VARCHAR(20) NULL,
  `party_name` VARCHAR(200) NULL,
  `paidin_capital` BIGINT NULL,
  `setup_date` DATE NULL,
  `party_type` VARCHAR(50) NULL,
  `use_invoice` CHAR(1) NULL,

  `ind_code` VARCHAR(20) NULL,
  `ind_name` VARCHAR(100) NULL,
  `ind_code1` VARCHAR(20) NULL,
  `ind_name1` VARCHAR(100) NULL,
  `ind_code2` VARCHAR(20) NULL,
  `ind_name2` VARCHAR(100) NULL,
  `ind_code3` VARCHAR(20) NULL,
  `ind_name3` VARCHAR(100) NULL,

  `row_hash` CHAR(64) NOT NULL,
  `etl_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_run_row` (`run_id`, `row_num`),
  KEY `idx_run_party` (`run_id`, `party_id`),
  KEY `idx_party_hash` (`party_id`, `row_hash`),
  KEY `idx_file_date` (`source_file_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



CREATE TABLE IF NOT EXISTS `crawlerdb`.`taxInfo` (
  `party_id` VARCHAR(20) NOT NULL,

  `party_addr` VARCHAR(300) NULL,
  `parent_party_id` VARCHAR(20) NULL,
  `party_name` VARCHAR(200) NULL,
  `paidin_capital` BIGINT NULL,
  `setup_date` DATE NULL,
  `party_type` VARCHAR(50) NULL,
  `use_invoice` CHAR(1) NULL,

  `ind_code` VARCHAR(20) NULL,
  `ind_name` VARCHAR(100) NULL,
  `ind_code1` VARCHAR(20) NULL,
  `ind_name1` VARCHAR(100) NULL,
  `ind_code2` VARCHAR(20) NULL,
  `ind_name2` VARCHAR(100) NULL,
  `ind_code3` VARCHAR(20) NULL,
  `ind_name3` VARCHAR(100) NULL,

  `row_hash` CHAR(64) NOT NULL,

  `source_file_date` DATE NOT NULL,
  `last_run_id` VARCHAR(32) NOT NULL,

  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`party_id`),
  KEY `idx_source_date` (`source_file_date`),
  KEY `idx_row_hash` (`row_hash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
