create or replace view VW_MKT_ORGANIZATIONSUMMARY(
	"Id",
	"Brand Name",
	"Store Type",
	"Type",
	"Brand Sort"
) as 

SELECT DISTINCT
"Id"
,"Brand Name"
,"Store Type"
,"Type"
,CASE WHEN "Brand Name" = 'J.Crew'
    THEN 1
    WHEN "Brand Name" = 'Factory'
    THEN 2
    WHEN "Brand Name" = 'Madewell'
    THEN 3
    END AS "Brand Sort"
FROM (
SELECT 
ID AS "Id"
,CASE WHEN LEVEL1NAME= 'JCrew'
THEN 'J.Crew'
ELSE LEVEL1NAME
END AS "Brand Name"
,CASE WHEN TYPE = 'Physical' THEN 'Retail' 
WHEN TYPE = 'Digital' THEN 'Direct'
ELSE TYPE 
END AS "Store Type"
,Type AS "Type"
FROM CDP_JC.PUBLIC.ORGANIZATIONSUMMARY
UNION
SELECT 
ID AS "Id"
,LEVEL1NAME AS "Brand Name"
,CAse when TYPE = 'Physical' THEN 'Retail' 
WHEN TYPE = 'Digital' THEN 'Direct'
ELSE TYPE
END AS "Store Type"
,Type AS "Type"
FROM CDP.PUBLIC.ORGANIZATIONSUMMARY);