-- 驗證：實際寫入筆數（用時間窗抓「本次 run 寫進 main 的筆數」）
SELECT
  COUNT(*) AS inserted_in_window
FROM crawlerdb.TaxInfo
WHERE Update_Time >= '2026-01-23 14:44:00'
  AND Update_Time <  '2026-01-23 14:48:30';

-- 找出今天新增了多版本的 Party_ID
select
  Party_ID,
  COUNT(*) AS version_cnt
FROM crawlerdb.TaxInfo
WHERE DATE(Update_Time) = CURRENT_DATE
GROUP BY Party_ID
HAVING COUNT(*) > 1
ORDER BY version_cnt DESC
LIMIT 50;  -- 限制前 50筆