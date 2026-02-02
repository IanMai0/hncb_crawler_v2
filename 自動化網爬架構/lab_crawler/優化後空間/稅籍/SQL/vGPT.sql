INSERT INTO crawlerdb.TaxInfo
( Party_ID, Party_Addr, Parent_Party_ID, Party_Name, PaidIn_Capital, Setup_Date,
  Party_Type, Use_Invoice, Ind_Code, Ind_Name, Ind_Code1, Ind_Name1, Ind_Code2,
  Ind_Name2, Ind_Code3, Ind_Name3 )
WITH
-- 1) TaxInfo 每個 Party_ID 嚴格取「最新一筆」（同 Update_Time 用 ID 當 tie-break）
latest AS (
  SELECT *
  FROM (
    SELECT
      t.*,
      ROW_NUMBER() OVER (
        PARTITION BY t.Party_ID
        ORDER BY t.Update_Time DESC, t.ID DESC
      ) AS rn
    FROM crawlerdb.TaxInfo t
  ) x
  WHERE x.rn = 1
),

-- 2) 若 Tmp_TaxInfo 可能同一 Party_ID 有多筆，先做去重/取最新（避免同批重複插）
tmp_latest AS (
  SELECT *
  FROM (
    SELECT
      a.*,
      ROW_NUMBER() OVER (
        PARTITION BY a.Party_ID
        ORDER BY a.Update_Time DESC
      ) AS rn
    FROM crawlerdb.Tmp_TaxInfo a
  ) y
  WHERE y.rn = 1
)

SELECT
  a.Party_ID, a.Party_Addr, a.Parent_Party_ID, a.Party_Name, a.PaidIn_Capital,
  a.Setup_Date, a.Party_Type, a.Use_Invoice, a.Ind_Code, a.Ind_Name,
  a.Ind_Code1, a.Ind_Name1, a.Ind_Code2, a.Ind_Name2, a.Ind_Code3, a.Ind_Name3
FROM tmp_latest a
LEFT JOIN latest t
  ON t.Party_ID = a.Party_ID
WHERE
  t.Party_ID IS NULL
  OR NOT (
    a.Party_Addr       <=> t.Party_Addr       AND
    a.Parent_Party_ID  <=> t.Parent_Party_ID  AND
    a.Party_Name       <=> t.Party_Name       AND
    a.PaidIn_Capital   <=> t.PaidIn_Capital   AND
    a.Setup_Date       <=> t.Setup_Date       AND
    a.Party_Type       <=> t.Party_Type       AND
    a.Use_Invoice      <=> t.Use_Invoice      AND
    a.Ind_Code         <=> t.Ind_Code         AND
    a.Ind_Name         <=> t.Ind_Name         AND
    a.Ind_Code1        <=> t.Ind_Code1        AND
    a.Ind_Name1        <=> t.Ind_Name1        AND
    a.Ind_Code2        <=> t.Ind_Code2        AND
    a.Ind_Name2        <=> t.Ind_Name2        AND
    a.Ind_Code3        <=> t.Ind_Code3        AND
    a.Ind_Name3        <=> t.Ind_Name3
  );

in
SELECT *
  FROM (
    SELECT
      a.*,
      ROW_NUMBER() OVER (
        PARTITION BY a.Party_ID
        ORDER BY a.Update_Time DESC
      ) AS rn
    FROM crawlerdb.Tmp_TaxInfo a
  ) y
  WHERE y.rn = 1

select j,b,c from #temp_table_a
left join
#temp_b
select* from col_a


insert into #temp_table_a
select
a,b,e,f
from col_a
where 

select * from #temp_table_a

select