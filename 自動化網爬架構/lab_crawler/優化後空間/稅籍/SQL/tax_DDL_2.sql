-- 先確認今天到底寫了多少筆（不要盲刪）

-- raw 原始資料
SELECT COUNT(*)
FROM crawlerdb.tmp_rawData
WHERE run_id = 'RUN_20260120_091324';

-- clean 歷史快照
SELECT COUNT(*)
FROM crawlerdb.tmp_taxInfo
WHERE run_id = 'RUN_20260120_091324';

-- 最新狀態表（這一筆 run 影響了多少 party）
SELECT COUNT(*)
FROM crawlerdb.taxInfo
WHERE last_run_id = 'RUN_20260120_091324';

-- 正確刪除順序（很重要）
-- 為什麼有順序？
-- taxInfo 是「最新狀態表」
-- tmp_taxInfo / tmp_rawData 是歷史資料
-- 先刪 latest，再刪歷史，不然你會留下「孤兒狀態」

DELETE FROM crawlerdb.taxInfo
WHERE last_run_id = 'RUN_20260120_091324';

DELETE FROM crawlerdb.tmp_taxInfo
WHERE run_id = 'RUN_20260120_091324';

DELETE FROM crawlerdb.tmp_rawData
WHERE run_id = 'RUN_20260120_091324';

-- 刪完後確認
SELECT COUNT(*) FROM crawlerdb.tmp_rawData WHERE run_id = 'RUN_20260120_091324';
SELECT COUNT(*) FROM crawlerdb.tmp_taxInfo WHERE run_id = 'RUN_20260120_091324';
SELECT COUNT(*) FROM crawlerdb.taxInfo WHERE last_run_id = 'RUN_20260120_091324';

-- 驗收用的 DB 查核 SQL (run_id 記得換 )
SET @rid := 'RUN_20260120_104921';

SELECT
  (SELECT COUNT(*) FROM crawlerdb.tmp_rawData WHERE run_id=@rid AND row_type='DATA') AS raw_data_cnt,
  (SELECT COUNT(*) FROM crawlerdb.tmp_taxInfo WHERE run_id=@rid) AS tmp_cnt,
  (SELECT COUNT(*) FROM crawlerdb.taxInfo) AS main_cnt,
  (SELECT COUNT(*) FROM crawlerdb.taxInfo WHERE last_run_id=@rid) AS main_updated_by_this_run;
