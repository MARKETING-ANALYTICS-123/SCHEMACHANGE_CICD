CREATE OR REPLACE PROCEDURE "USP_POPULATE_PASSPORT_MARKETING_LOYALTY"()
RETURNS VARCHAR(100)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_WEEK AS
SELECT DISTINCT FISCALYEAR, FISCALWEEK, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            FISCALWEEK,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
			AND TT.SUBTYPE in (''Demand'', ''Shipped'')
            -- AND FISCALMONTH = 1
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";
    

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_Feb AS
SELECT DISTINCT FISCALYEAR, FISCALMONTH, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            FISCALMONTH,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
			AND TT.SUBTYPE in (''Demand'', ''Shipped'')
            -- AND FISCALMONTH = 1
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";


-----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_Mar AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
			AND TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND FISCALMONTH IN (1,2)
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_Apr AS
SELECT DISTINCT FISCALYEAR, FISCALQUARTER, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            FISCALQUARTER,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
			AND TT.SUBTYPE in (''Demand'', ''Shipped'')
            -- AND FISCALMONTH IN (1,2,3)
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_May AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
			AND TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND FISCALMONTH IN (1,2,3,4)
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_Jun AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
			AND TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND FISCALMONTH IN (1,2,3,4,5)
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_Jul AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
			AND TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND FISCALMONTH IN (1,2,3,4,5,6)
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_Aug AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
			AND TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND FISCALMONTH IN (1,2,3,4,5,6,7)
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_Sep AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
			AND TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND FISCALMONTH IN (1,2,3,4,5,6,7,8)
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_Oct AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
			AND TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND FISCALMONTH IN (1,2,3,4,5,6,7,8,9)
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_Nov AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
			AND TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND FISCALMONTH IN (1,2,3,4,5,6,7,8,9,10)
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_Dec AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
			AND TT.SUBTYPE in (''Demand'', ''Shipped'')
         AND FISCALMONTH IN (1,2,3,4,5,6,7,8,9,10,11)
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

--------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_Jan AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
			AND TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND FISCALMONTH IN (1,2,3,4,5,6,7,8,9,10,11,12)
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";


----------------------------------------------------------------------------------------------- Till Previous Week ------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_PW_Feb AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
            AND FISCALMONTH = 1
            AND FISCALWEEK <= 3
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_PW_Mar AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
            AND FISCALMONTH IN (1,2)
            AND FISCALWEEK <= 8
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_PW_Apr AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
            AND FISCALMONTH IN (1,2,3)
            AND FISCALWEEK <= 12
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_PW_May AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
            AND FISCALMONTH IN (1,2,3,4)
            AND FISCALWEEK <= 16
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_PW_Jun AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
            AND FISCALMONTH IN (1,2,3,4,5)
            AND FISCALWEEK <= 21
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_PW_Jul AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
            AND FISCALMONTH IN (1,2,3,4,5,6)
            AND FISCALWEEK <= 25
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_PW_Aug AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
            AND FISCALMONTH IN (1,2,3,4,5,6,7)
            AND FISCALWEEK <= 29
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_PW_Sep AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
            AND FISCALMONTH IN (1,2,3,4,5,6,7,8)
            AND FISCALWEEK <= 34
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_PW_Oct AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
            AND FISCALMONTH IN (1,2,3,4,5,6,7,8,9)
            AND FISCALWEEK <= 38
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_PW_Nov AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
            AND FISCALMONTH IN (1,2,3,4,5,6,7,8,9,10)
            AND FISCALWEEK <= 42
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_PW_Dec AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
            AND FISCALMONTH IN (1,2,3,4,5,6,7,8,9,10,11)
            AND FISCALWEEK <= 47
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_PW_Jan AS
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
            AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                AND TS.FISCALDAYOFWEEK = 7
              )
            AND FISCALMONTH IN (1,2,3,4,5,6,7,8,9,10,11,12)
            AND FISCALWEEK <= 51
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

-----------------------------------------------------LY Loyalty till the current completed week---------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_LY_CURR AS
WITH CURRENT_WEEK AS(
    SELECT MAX(FISCALYEAR) - 1 AS FISCALYEAR, MAX(FISCALWEEK) AS FISCALWEEK FROM CDP.PUBLIC.TIMESUMMARY
    WHERE DATEINUNIX::DATE <= CURRENT_DATE::DATE AND DATEINUNIX::DATE >= CURRENT_DATE::DATE - 7
    AND FISCALDAYOFWEEK = 7
)
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND FISCALYEAR = (SELECT FISCALYEAR FROM CURRENT_WEEK)
            AND FISCALWEEK <= (SELECT FISCALWEEK FROM CURRENT_WEEK)
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";

----------------------------------------------------------------LY Loyalty till the current previous week-----------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE TEMP.FACT_GET_TIER_YEAR_FTY_LY_PW_CURR AS
WITH CURRENT_WEEK AS(
    SELECT MAX(FISCALYEAR) - 1 AS FISCALYEAR, MAX(FISCALWEEK) - 1 AS FISCALWEEK FROM CDP.PUBLIC.TIMESUMMARY
    WHERE DATEINUNIX::DATE <= CURRENT_DATE AND DATEINUNIX::DATE >= CURRENT_DATE - 7
    AND FISCALDAYOFWEEK = 7
),
FINAL_SELECTION AS (
    SELECT CASE WHEN FISCALWEEK = 0 THEN FISCALYEAR - 1 ELSE FISCALYEAR END AS FISCALYEAR,
    CASE WHEN FISCALWEEK = 0 THEN (SELECT MAX(FISCALWEEK) FROM CDP.PUBLIC.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR - 1 FROM CURRENT_WEEK)) ELSE FISCALWEEK END AS FISCALWEEK
    FROM CURRENT_WEEK
)
SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
    FROM RPT."PASSPORT LOYALTY TIER" TS
    INNER JOIN
    (
        SELECT FISCALYEAR,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND FISCALYEAR = (SELECT FISCALYEAR FROM FINAL_SELECTION)
            AND FISCALWEEK <= (SELECT FISCALWEEK FROM FINAL_SELECTION)
			GROUP BY ALL
    ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence";


--------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE Temp."PASSPORT MARKETING LOYALTY"(
    "Loyalty Key" NUMBER(38, 0),
    "Loyalty Tier Week Status" INT,
    "Feb Loyalty Tier Year Status" INT,
    "March Loyalty Tier Year Status" INT,
    "April Loyalty Tier Year Status" INT,
    "May Loyalty Tier Year Status" INT,
    "June Loyalty Tier Year Status" INT,
    "July Loyalty Tier Year Status" INT,
    "August Loyalty Tier Year Status" INT,
    "September Loyalty Tier Year Status" INT,
    "October Loyalty Tier Year Status" INT,
    "November Loyalty Tier Year Status" INT,
    "December Loyalty Tier Year Status" INT,
    "Jan Loyalty Tier Year Status" INT,
    "Curr Loyalty Tier Year Status" INT,
    "Feb Prev Loyalty Tier Year Status" INT,
    "March Prev Loyalty Tier Year Status" INT,
    "April Prev Loyalty Tier Year Status" INT,
    "May Prev Loyalty Tier Year Status" INT,
    "June Prev Loyalty Tier Year Status" INT,
    "July Prev Loyalty Tier Year Status" INT,
    "August Prev Loyalty Tier Year Status" INT,
    "September Prev Loyalty Tier Year Status" INT,
    "October Prev Loyalty Tier Year Status" INT,
    "November Prev Loyalty Tier Year Status" INT,
    "December Prev Loyalty Tier Year Status" INT,
    "Jan Prev Loyalty Tier Year Status" INT,
    "Curr Prev Loyalty Tier Year Status" INT,
    "Last Modified Date" TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP() 
);


INSERT INTO Temp."PASSPORT MARKETING LOYALTY"(
    "Loyalty Key",
    "Loyalty Tier Week Status",
    "Feb Loyalty Tier Year Status",
    "March Loyalty Tier Year Status",
    "April Loyalty Tier Year Status",
    "May Loyalty Tier Year Status",
    "June Loyalty Tier Year Status",
    "July Loyalty Tier Year Status",
    "August Loyalty Tier Year Status",
    "September Loyalty Tier Year Status",
    "October Loyalty Tier Year Status",
    "November Loyalty Tier Year Status",
    "December Loyalty Tier Year Status",
    "Jan Loyalty Tier Year Status",
    "Curr Loyalty Tier Year Status",
    "Feb Prev Loyalty Tier Year Status",
    "March Prev Loyalty Tier Year Status",
    "April Prev Loyalty Tier Year Status",
    "May Prev Loyalty Tier Year Status",
    "June Prev Loyalty Tier Year Status",
    "July Prev Loyalty Tier Year Status",
    "August Prev Loyalty Tier Year Status",
    "September Prev Loyalty Tier Year Status",
    "October Prev Loyalty Tier Year Status",
    "November Prev Loyalty Tier Year Status",
    "December Prev Loyalty Tier Year Status",
    "Jan Prev Loyalty Tier Year Status",
    "Curr Prev Loyalty Tier Year Status"
)

SELECT
DISTINCT
T."Loyalty Key",
CASE WEEKTS."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "Loyalty Tier Week Status",

CASE FEBTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "Feb Loyalty Tier Year Status",

CASE MARTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "March Loyalty Tier Year Status",

CASE APRTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "April Loyalty Tier Year Status",

CASE MAYTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "May Loyalty Tier Year Status",

CASE JUNTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "June Loyalty Tier Year Status",

CASE JULTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "July Loyalty Tier Year Status",

CASE AUGTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "August Loyalty Tier Year Status",

CASE SEPTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "September Loyalty Tier Year Status",

CASE OCTTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "October Loyalty Tier Year Status",

CASE NOVTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "November Loyalty Tier Year Status",

CASE DECTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "December Loyalty Tier Year Status",

CASE JANTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "Jan Loyalty Tier Year Status",

CASE CURRTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "Curr Loyalty Tier Year Status",

CASE FEBPTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "Feb Prev Loyalty Tier Year Status",

CASE MARPTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "March Prev Loyalty Tier Year Status",

CASE APRPTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "April Prev Loyalty Tier Year Status",

CASE MAYPTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "May Prev Loyalty Tier Year Status",

CASE JUNPTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "June Prev Loyalty Tier Year Status",

CASE JULPTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "July Prev Loyalty Tier Year Status",

CASE AUGPTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "August Prev Loyalty Tier Year Status",

CASE SEPPTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "September Prev Loyalty Tier Year Status",

CASE OCTPTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "October Prev Loyalty Tier Year Status",

CASE NOVPTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "November Prev Loyalty Tier Year Status",

CASE DECPTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "December Prev Loyalty Tier Year Status",

CASE JANPTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "Jan Prev Loyalty Tier Year Status",

CASE CURRPTY."Loyalty Tier"
  WHEN ''Green'' THEN 6
  WHEN ''Navy'' THEN 7
  WHEN ''Gold'' THEN 8
  ELSE 9
END AS "Curr Prev Loyalty Tier Year Status"

FROM RPT."PASSPORT MARKETING" T
JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON T."Transaction Date Id" = TS.ID
LEFT JOIN RPT."PASSPORT CUSTOMER" TT ON T."MasterCustomerId" = TT."Customer Id"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_WEEK WEEKTS ON TS.FISCALYEAR = WEEKTS.FISCALYEAR AND TT.MASTERCUSTOMERID = WEEKTS."MasterCustomer ID" AND TS.FISCALWEEK = WEEKTS.FISCALWEEK

LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_Feb FEBTY ON TS.FISCALYEAR = FEBTY.FISCALYEAR AND TT.MASTERCUSTOMERID = FEBTY."MasterCustomer ID" AND TS.FISCALMONTH = FEBTY.FISCALMONTH
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_Mar MARTY ON TS.FISCALYEAR = MARTY.FISCALYEAR AND TT.MASTERCUSTOMERID = MARTY."MasterCustomer ID"

LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_Apr APRTY ON TS.FISCALYEAR = APRTY.FISCALYEAR AND TT.MASTERCUSTOMERID = APRTY."MasterCustomer ID" AND TS.FISCALQUARTER = APRTY.FISCALQUARTER

LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_May MAYTY ON TS.FISCALYEAR = MAYTY.FISCALYEAR AND TT.MASTERCUSTOMERID = MAYTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_Jun JUNTY ON TS.FISCALYEAR = JUNTY.FISCALYEAR AND TT.MASTERCUSTOMERID = JUNTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_Jul JULTY ON TS.FISCALYEAR = JULTY.FISCALYEAR AND TT.MASTERCUSTOMERID = JULTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_Aug AUGTY ON TS.FISCALYEAR = AUGTY.FISCALYEAR AND TT.MASTERCUSTOMERID = AUGTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_Sep SEPTY ON TS.FISCALYEAR = SEPTY.FISCALYEAR AND TT.MASTERCUSTOMERID = SEPTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_Oct OCTTY ON TS.FISCALYEAR = OCTTY.FISCALYEAR AND TT.MASTERCUSTOMERID = OCTTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_Nov NOVTY ON TS.FISCALYEAR = NOVTY.FISCALYEAR AND TT.MASTERCUSTOMERID = NOVTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_Dec DECTY ON TS.FISCALYEAR = DECTY.FISCALYEAR AND TT.MASTERCUSTOMERID = DECTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_Jan JANTY ON TS.FISCALYEAR = JANTY.FISCALYEAR AND TT.MASTERCUSTOMERID = JANTY."MasterCustomer ID"

LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_LY_CURR CURRTY ON TS.FISCALYEAR = CURRTY.FISCALYEAR AND TT.MASTERCUSTOMERID = CURRTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_PW_Feb FEBPTY ON TS.FISCALYEAR = FEBPTY.FISCALYEAR AND TT.MASTERCUSTOMERID = FEBPTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_PW_Mar MARPTY ON TS.FISCALYEAR = MARPTY.FISCALYEAR AND TT.MASTERCUSTOMERID = MARPTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_PW_Apr APRPTY ON TS.FISCALYEAR = APRPTY.FISCALYEAR AND TT.MASTERCUSTOMERID = APRPTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_PW_May MAYPTY ON TS.FISCALYEAR = MAYPTY.FISCALYEAR AND TT.MASTERCUSTOMERID = MAYPTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_PW_Jun JUNPTY ON TS.FISCALYEAR = JUNPTY.FISCALYEAR AND TT.MASTERCUSTOMERID = JUNPTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_PW_Jul JULPTY ON TS.FISCALYEAR = JULPTY.FISCALYEAR AND TT.MASTERCUSTOMERID = JULPTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_PW_Aug AUGPTY ON TS.FISCALYEAR = AUGPTY.FISCALYEAR AND TT.MASTERCUSTOMERID = AUGPTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_PW_Sep SEPPTY ON TS.FISCALYEAR = SEPPTY.FISCALYEAR AND TT.MASTERCUSTOMERID = SEPPTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_PW_Oct OCTPTY ON TS.FISCALYEAR = OCTPTY.FISCALYEAR AND TT.MASTERCUSTOMERID = OCTPTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_PW_Nov NOVPTY ON TS.FISCALYEAR = NOVPTY.FISCALYEAR AND TT.MASTERCUSTOMERID = NOVPTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_PW_Dec DECPTY ON TS.FISCALYEAR = DECPTY.FISCALYEAR AND TT.MASTERCUSTOMERID = DECPTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_PW_Jan JANPTY ON TS.FISCALYEAR = JANPTY.FISCALYEAR AND TT.MASTERCUSTOMERID = JANPTY."MasterCustomer ID"
LEFT JOIN TEMP.FACT_GET_TIER_YEAR_FTY_LY_PW_CURR CURRPTY ON TS.FISCALYEAR = CURRPTY.FISCALYEAR AND TT.MASTERCUSTOMERID = CURRPTY."MasterCustomer ID";

DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_Feb;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_Mar;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_Apr;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_May;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_Jun;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_Jul;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_Aug;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_Sep;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_Oct;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_Nov;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_Dec;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_Jan;

DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_LY_CURR;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_PW_Feb;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_PW_Mar;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_PW_Apr;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_PW_May;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_PW_Jun;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_PW_Jul;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_PW_Aug;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_PW_Sep;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_PW_Oct;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_PW_Nov;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_PW_Dec;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_PW_Jan;
DROP TABLE IF EXISTS TEMP.FACT_GET_TIER_YEAR_FTY_LY_PW_CURR;

DROP TABLE IF EXISTS RPT."PASSPORT MARKETING LOYALTY";

ALTER TABLE Temp."PASSPORT MARKETING LOYALTY" RENAME TO RPT."PASSPORT MARKETING LOYALTY";

RETURN ''Table RPT."PASSPORT MARKETING LOYALTY" created or replaced successfully.'';
END;
';