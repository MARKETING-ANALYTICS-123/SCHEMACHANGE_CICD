CREATE OR REPLACE PROCEDURE "USP_POPULATE_LOYALTY_REDEMPTION_DATA"()
RETURNS VARCHAR(100)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN

TRUNCATE TABLE RPT."LOYALTY REDEMPTION DATA";

INSERT INTO RPT."LOYALTY REDEMPTION DATA"
(
    "Id",
	"% Transaction includes Redemption",
	"% Revenue from Redemption",
    "% Employee Transaction includes Redemption",
    "% Employee Revenue from Redemption",
    "% Non-Employee Transaction includes Redemption",
    "% Non-Employee Revenue from Redemption"
)
WITH CTE1 AS
(
    SELECT
    O."Id",
    ROUND(COUNT(DISTINCT "DMD Transactions") / 28, 0) AS "Total Transaction",
    ROUND(SUM("Net Revenue") / 28, 0) AS "Net Sales",
    ROUND(COUNT(DISTINCT CASE WHEN TT."Employee Flag" = 1 THEN "DMD Transactions" END) / 28, 0) AS "Employee Total Transaction",
    ROUND(SUM(CASE WHEN TT."Employee Flag" = 1 THEN "Net Revenue" END) / 28, 0) AS "Employee Net Sales",
    ROUND(COUNT(DISTINCT CASE WHEN TT."Employee Flag" = 0 THEN "DMD Transactions" END) / 28, 0) AS "Non-Employee Total Transaction",
    ROUND(SUM(CASE WHEN TT."Employee Flag" = 0 THEN "Net Revenue" END) / 28, 0) AS "Non-Employee Net Sales"
FROM
    RPT."ORGANIZATION SUMMARY" O 
    INNER JOIN RPT."PASSPORT LOYALTY" TT ON O."Id" = TT."Brand ID"
    INNER JOIN CDP.PUBLIC.TIMESUMMARY TS ON TS.ID = TT."Date Key"
    WHERE TS.ID >= 20240204
    AND TS.ID <= 20240817
    AND "Loyalty Flag" = ''Loyalty''
    GROUP BY ALL


    UNION ALL


    SELECT
    O."Id",
    ROUND(COUNT(DISTINCT "DMD Transactions") / 28, 0) AS "Total Transaction",
    ROUND(SUM("Net Revenue") / 28, 0) AS "Net Sales",
    ROUND(COUNT(DISTINCT CASE WHEN TT."Employee Flag" = 1 THEN "DMD Transactions" END) / 28, 0) AS "Employee Total Transaction",
    ROUND(SUM(CASE WHEN TT."Employee Flag" = 1 THEN "Net Revenue" END) / 28, 0) AS "Employee Net Sales",
    ROUND(COUNT(DISTINCT CASE WHEN TT."Employee Flag" = 0 THEN "DMD Transactions" END) / 28, 0) AS "Non-Employee Total Transaction",
    ROUND(SUM(CASE WHEN TT."Employee Flag" = 0 THEN "Net Revenue" END) / 28, 0) AS "Non-Employee Net Sales"
FROM
    RPT."ORGANIZATION SUMMARY" O 
    INNER JOIN RPT."MADEWELL LOYALTY" TT ON O."Id" = TT."Brand ID"
    INNER JOIN CDP.PUBLIC.TIMESUMMARY TS ON TS.ID = TT."Date Key"
    WHERE TS.ID >= 20240204
    AND TS.ID <= 20240817
    AND "Loyalty Flag" = ''Loyalty''
    GROUP BY ALL
    
),

CTE2 AS
(
    SELECT
    O."Id",
    ROUND(COUNT(DISTINCT "Transactions")/28, 0) AS "Redeemed Transaction",
    ROUND(COUNT(DISTINCT "Code")*5/28, 0) AS "Redeemed Value",
    ROUND(COUNT(DISTINCT CASE WHEN TT."Employee Flag" = 1 THEN "Transactions" END) / 28, 0) AS "Employee Redeemed Transaction",
    ROUND(COUNT(DISTINCT CASE WHEN TT."Employee Flag" = 1 THEN "Code" END)*5/28, 0) AS "Employee Redeemed Value",
    ROUND(COUNT(DISTINCT CASE WHEN TT."Employee Flag" = 0 THEN "Transactions" END) / 28, 0) AS "Non-Employee Redeemed Transaction",
    ROUND(COUNT(DISTINCT CASE WHEN TT."Employee Flag" = 0 THEN "Code" END)*5/28, 0) AS "Non-Employee Redeemed Value",
    FROM
    RPT."ORGANIZATION SUMMARY" O 
    INNER JOIN RPT."PASSPORT COUPON STATS AND USAGE" TT ON O."Id" = TT."Organization ID"
    INNER JOIN CDP.PUBLIC.TIMESUMMARY TS ON TS.ID = TT."Transaction Date Id"
    WHERE TS.ID >= 20240204
    AND TS.ID <= 20240817
    AND "Reward Name" = ''transactional_reward1''
    GROUP BY ALL

    UNION ALL

    SELECT
    O."Id",
    ROUND(COUNT(DISTINCT "Transactions")/28, 0) AS "Redeemed Transaction",
    ROUND(COUNT(DISTINCT "Code")*5/28, 0) AS "Redeemed Value",
    ROUND(COUNT(DISTINCT CASE WHEN TT."Employee Flag" = 1 THEN "Transactions" END) / 28, 0) AS "Employee Redeemed Transaction",
    ROUND(COUNT(DISTINCT CASE WHEN TT."Employee Flag" = 1 THEN "Code" END)*5/28, 0) AS "Employee Redeemed Value",
    ROUND(COUNT(DISTINCT CASE WHEN TT."Employee Flag" = 0 THEN "Transactions" END) / 28, 0) AS "Non-Employee Redeemed Transaction",
    ROUND(COUNT(DISTINCT CASE WHEN TT."Employee Flag" = 0 THEN "Code" END)*5/28, 0) AS "Non-Employee Redeemed Value",
    FROM
    RPT."ORGANIZATION SUMMARY" O 
    INNER JOIN RPT."MADEWELL COUPON STATS AND USAGE" TT ON O."Id" = TT."Organization Id"
    INNER JOIN CDP.PUBLIC.TIMESUMMARY TS ON TS.ID = TT."Transaction Date Id"
    WHERE TS.ID >= 20240204
    AND TS.ID <= 20240817
    AND "Reward Name" IS NULL
    GROUP BY ALL
)

SELECT 
O."Id",
CONCAT(
    CASE 
        WHEN "Total Transaction" = 0 THEN 0
        WHEN "Total Transaction" = 0 AND "Redeemed Transaction" = 0 THEN 0
        ELSE ROUND(("Redeemed Transaction" / "Total Transaction")*100, 2)
    END, 
    ''%''
) AS "% Transaction includes Redemption",
CONCAT(
    CASE 
        WHEN "Net Sales" = 0 THEN 0
        WHEN "Net Sales" = 0 AND "Redeemed Transaction" = 0 THEN 0
        ELSE ROUND(("Redeemed Value" /  "Net Sales")*100, 2)
    END, 
    ''%''
) AS "% Revenue from Redemption",

CONCAT(
    CASE 
        WHEN "Employee Total Transaction" = 0 THEN 0
        WHEN "Employee Total Transaction" = 0 AND "Employee Total Transaction" = 0 THEN 0
        ELSE ROUND(("Employee Redeemed Transaction" / "Employee Total Transaction") * 100, 2)
    END, 
    ''%''
) AS "% Employee Transaction includes Redemption",

CONCAT(ROUND(("Employee Redeemed Value" /  CASE WHEN "Employee Net Sales" = 0 THEN 1 ELSE "Employee Net Sales" END)*100, 2), ''%'') AS "% Employee Revenue from Redemption",
CONCAT(
    CASE 
        WHEN "Non-Employee Total Transaction" = 0 THEN 0
        WHEN "Non-Employee Total Transaction" = 0 AND "Non-Employee Total Transaction" = 0 THEN 0
        ELSE ROUND(("Non-Employee Redeemed Transaction" / "Non-Employee Total Transaction") * 100, 2)
    END, 
    ''%''
) AS "% Non-Employee Transaction includes Redemption",
CONCAT(ROUND(("Non-Employee Redeemed Value" /  CASE WHEN "Non-Employee Net Sales" = 0 THEN 1 ELSE "Non-Employee Net Sales" END)*100, 2), ''%'') AS "% Non-Employee Revenue from Redemption"
FROM
RPT."ORGANIZATION SUMMARY" O
LEFT JOIN CTE1 AS CT1 ON CT1."Id" = O."Id"
LEFT JOIN CTE2 AS CT2 ON CT2."Id" = O."Id"
WHERE "Redeemed Transaction" IS NOT NULL;

RETURN ''RPT."LOYALTY REDEMPTION TABLE" created or replaced successfully'';

END
';