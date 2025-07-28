CREATE OR REPLACE PROCEDURE "USP_POPULATE_DATE"()
RETURNS VARCHAR(100)
LANGUAGE SQL
EXECUTE AS OWNER
AS 'BEGIN
TRUNCATE TABLE RPT.DATE;
INSERT INTO RPT.DATE(
	"Id",
	"Fiscal Year Name",
	"Fiscal Year",
	"Fiscal Quarter Name",
	"Fiscal Quarter",
	"Fiscal Month",
	"Fiscal Week Name",
	"Fiscal Week",
	"Date",
	"Fiscal Day Of Month",
	"Fiscal Year Month",
	"Fiscal Day Of Week",
	"Fiscal Day Of Quarter",
	"Fiscal Day Of Year",
	"Fiscal Month Name",
	"Last Year Id",
	"Last Year Date",
	"Last Year Unshifted Id",
	"Calendar Year",
	"Calendar Month",
	"Calendar Month Name",
	"Calendar Quarter",
	"Calendar Week",
	"Calendar Week Name",
	"Fiscal Week Sequence Number",
	"Completed Week Flag",
    "Last Modified Date"
) 
WITH CompletedDate AS(
    SELECT ID 
    FROM CDP_JC.PUBLIC.TIMESUMMARY T
    WHERE 
    TO_DATE(T.DATEINUNIX) <=(SELECT MAX(DATE(DATEINUNIX)) FROM CDP_JC.PUBLIC.TIMESUMMARY WHERE DATE(DATEINUNIX)>=CURRENT_DATE-7 AND DATE(DATEINUNIX)<CURRENT_DATE AND FISCALDAYOFWEEK=7)
    AND 
    T.ID>=(SELECT MIN(ID) FROM CDP_JC.PUBLIC.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR-2 FROM CDP_JC.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX)>CURRENT_DATE-7 AND TO_DATE(DATEINUNIX)<=CURRENT_DATE AND FISCALDAYOFWEEK = 7))
)
SELECT 
T.ID,
"Fiscal Year Name",
"Fiscal Year",
"Fiscal Quarter Name",
"Fiscal Quarter",
"Fiscal Month",
"Fiscal Week Name",
"Fiscal Week",
T."Date",
"Fiscal Day Of Month",
"Fiscal Year Month",
"Fiscal Day Of Week",
"Fiscal Day Of Quarter",
"Fiscal Day Of Year",
COALESCE("Fiscal Month Name", "Month") AS "Fiscal Month Name",
COALESCE("Last Year Id", "LY Shift Date Key") AS "Last Year Id",
COALESCE("Last Year Date", TO_DATE(TO_CHAR("LY Shift Date Key"), ''YYYYMMDD'')) AS "Last Year Date",
COALESCE("Last Year Unshifted Id", "LY Date Key") AS "Last Year Unshifted Id",
"Calendar Year",
"Calendar Month",
"Calendar Month Name",
"Calendar Quarter",
"Calendar Week",
"Calendar Week Name",
DENSE_RANK() OVER (ORDER BY "Fiscal Year", "Fiscal Week") AS "Fiscal Week Sequence Number",
CASE WHEN C.ID IS NULL THEN 0 ELSE 1 END AS "Completed Week Flag",
CURRENT_TIMESTAMP AS "Last Modified Date"
FROM EDW_MARKETING_PRD.ANALYTICS."Dim Date" T
LEFT JOIN CompletedDate C ON T.ID = C.ID 
LEFT JOIN EDWPRD.RPT."Date Exp" DX ON DX."Date Key" = T.ID
WHERE 
    TO_DATE(T."Date") <= CURRENT_DATE - 1
AND
    T.ID >= (SELECT MIN(ID) FROM CDP_JC.PUBLIC.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR-2 FROM CDP_JC.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX)>CURRENT_DATE-7 AND TO_DATE(DATEINUNIX)<=CURRENT_DATE AND FISCALDAYOFWEEK = 7));

RETURN ''DATE table created successfully'';

END';