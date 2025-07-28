create or replace view VW_MKT_PRODUCTCATEGORYSUMMARY(
	"Id",
	"Product Category Level 1",
	"Product Category Level 2",
	"Product Category Level 3",
	"Product Category Level 4",
	"Product Category Level 5"
) as 

SELECT
DENSE_RANK() OVER(ORDER BY "Product Category Level 1","Product Category Level 2", "Product Category Level 3", "Product Category Level 4", "Product Category Level 5") AS "Id"
,"Product Category Level 1"
,"Product Category Level 2"
,"Product Category Level 3"
,"Product Category Level 4"
,"Product Category Level 5"
FROM(
SELECT 
LEVEL1NAME AS "Product Category Level 1"
,LEVEL2NAME AS "Product Category Level 2"
,LEVEL3NAME AS "Product Category Level 3"
,LEVEL4NAME AS "Product Category Level 4"
,LEVEL5NAME AS "Product Category Level 5"
FROM CDP.PUBLIC.PRODUCTCATEGORYSUMMARY PS 
UNION
SELECT 
LEVEL1NAME AS "Product Category Level 1"
,LEVEL2NAME AS "Product Category Level 2"
,LEVEL3NAME AS "Product Category Level 3"
,CASE WHEN LEVEL3NAME = 'W PANTS' AND LEVEL4NAME <> 'W PANTS DENIM' THEN 'W PANTS NON DENIM'
WHEN LEVEL3NAME = 'M PANTS' AND (LEVEL4NAME = 'MENS PANTS DENIM' OR LEVEL4NAME = 'M PANTS DENIM') THEN 'M PANTS DENIM'
WHEN LEVEL3NAME = 'M PANTS' AND LEVEL4NAME <> 'MENS PANTS DENIM' AND LEVEL4NAME <> 'M PANTS DENIM' THEN 'M PANTS NON DENIM'
ELSE LEVEL4NAME END AS "Product Category Level 4"
,LEVEL5NAME AS "Product Category Level 5"
FROM CDP_JC.PUBLIC.PRODUCTCATEGORYSUMMARY PS
);