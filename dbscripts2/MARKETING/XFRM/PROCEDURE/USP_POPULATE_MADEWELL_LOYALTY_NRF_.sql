CREATE OR REPLACE PROCEDURE "USP_POPULATE_MADEWELL_LOYALTY_NRF"()
RETURNS VARCHAR(100)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN

    CREATE OR REPLACE TABLE Temp."MADEWELL LOYALTY STATUS NRF" (
        "Loyalty Tier" NUMBER(4, 0),
    	"Customer Loyalty Flag" VARCHAR(100),
        "Channel Flag" VARCHAR(100),
    	"Date Key" NUMBER(38,0),
    	"Brand ID" VARCHAR(100),
    	"MasterCustomer ID" NUMBER(38,0),
    	"Reward Flag" VARCHAR(100),
    	"Sub Type" VARCHAR(100),
    	"Status Type" NUMBER(5, 0),
    	"FM Status Type" NUMBER(5, 0),
    	"FQ Status Type" NUMBER(5, 0),
    	"FY Status Type" NUMBER(5, 0),
    	"Loyalty Signup Date Id" NUMBER(38,0),
    	"Loyalty SignUp Channel" VARCHAR(200),
    	"Cashier Associate" VARCHAR(200),
    	"Email Capture Flag" VARCHAR(200),
        "Employee Flag" NUMBER(3, 0),
    	"Net Revenue" FLOAT,
    	"Net Units" FLOAT,
    	"Net Cost Basis" FLOAT,
    	"DMD Revenue" FLOAT,
    	"DMD Units" FLOAT,
    	"DMD Cost Basis" FLOAT,
    	"DMD Transactions" VARCHAR(200),
        "Loyalty Tier Week" NUMBER(4, 0),
        "Loyalty Tier Month" NUMBER(4, 0),
        "Loyalty Tier Quarter" NUMBER(4, 0),
        "Loyalty Tier Year" NUMBER(4, 0),
        "Loyalty Tier LY Same" NUMBER(4, 0)
    );

    INSERT INTO Temp."MADEWELL LOYALTY STATUS NRF" (
        "Loyalty Tier",
        "Customer Loyalty Flag",
        "Channel Flag",
        "Date Key",
        "Brand ID",
        "MasterCustomer ID",
        "Reward Flag",
        "Sub Type",
        "Status Type",
        "FM Status Type",
        "FQ Status Type",
        "FY Status Type",
        "Loyalty Signup Date Id",
        "Loyalty SignUp Channel",
        "Cashier Associate",
        "Email Capture Flag",
        "Employee Flag",
        "Net Revenue",
        "Net Units",
        "Net Cost Basis",
        "DMD Revenue",
        "DMD Units",
        "DMD Cost Basis",
        "DMD Transactions",
        "Loyalty Tier Week",
        "Loyalty Tier Month",
        "Loyalty Tier Quarter",
        "Loyalty Tier Year",
        "Loyalty Tier LY Same"
    )
    
    WITH FACT_GET_TIER_YEAR_MW_WEEK_NRF AS
    (
        SELECT DISTINCT 
        FISCALYEAR, 
        FISCALWEEK,
        TS."MasterCustomer ID", 
        "Loyalty Tier"
        FROM RPT."MADEWELL LOYALTY TIER" TS
        INNER JOIN
        (
            SELECT FISCALYEAR,
            FISCALWEEK,
            MASTERCUSTOMERID AS "MasterCustomerId",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
            FROM
            DS_DEV.PUBLIC.TRANSACTIONSUMMARY_MW_20240308 TT
            LEFT JOIN ANALYTICS.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
            WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
            AND TS.FISCALYEAR = 2023
            GROUP BY ALL
        ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence"
    ),


    FACT_GET_TIER_YEAR_MW_MONTH_NRF AS
    (
        SELECT DISTINCT 
        FISCALYEAR, 
        FISCALMONTH,
        TS."MasterCustomer ID", 
        "Loyalty Tier"
        FROM RPT."MADEWELL LOYALTY TIER" TS
        INNER JOIN
        (
            SELECT FISCALYEAR,
                FISCALMONTH,
                MASTERCUSTOMERID AS "MasterCustomerId",
                MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
                FROM
                DS_DEV.PUBLIC.TRANSACTIONSUMMARY_MW_20240308 TT
                LEFT JOIN ANALYTICS.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
                WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
                AND TS.FISCALYEAR = 2023
    			GROUP BY ALL
        ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence"
    ),


    FACT_GET_TIER_YEAR_MW_QUARTER_NRF AS
    (
        SELECT DISTINCT FISCALYEAR, FISCALQUARTER, TS."MasterCustomer ID", "Loyalty Tier"
        FROM RPT."MADEWELL LOYALTY TIER" TS
        INNER JOIN
        (
            SELECT FISCALYEAR,
                FISCALQUARTER,
                MASTERCUSTOMERID AS "MasterCustomerId",
                MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
                FROM
                DS_DEV.PUBLIC.TRANSACTIONSUMMARY_MW_20240308 TT
                LEFT JOIN ANALYTICS.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
                WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
                AND TS.FISCALYEAR = 2023
    			GROUP BY ALL
        ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence"
    ),

    FACT_GET_TIER_YEAR_MW_YEAR_NRF AS
    (
        SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
        FROM RPT."MADEWELL LOYALTY TIER" TS
        INNER JOIN
        (
            SELECT FISCALYEAR,
                MASTERCUSTOMERID AS "MasterCustomerId",
                MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
                FROM
                DS_DEV.PUBLIC.TRANSACTIONSUMMARY_MW_20240308 TT
                LEFT JOIN ANALYTICS.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
                WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
                AND TS.FISCALYEAR = 2023
    			GROUP BY ALL
        ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence"
    ),

    CURRENT_WEEK AS(
        SELECT MAX(FISCALYEAR) - 1 AS FISCALYEAR, MAX(FISCALWEEK) AS FISCALWEEK FROM EDW_MARKETING.ANALYTICS.TIMESUMMARY
        WHERE DATEINUNIX::DATE <= CURRENT_DATE::DATE AND DATEINUNIX::DATE >= CURRENT_DATE::DATE - 7
    ),


    FACT_GET_TIER_YEAR_MW_LY_CURR_NRF AS
    (
        SELECT DISTINCT FISCALYEAR, TS."MasterCustomer ID", "Loyalty Tier"
            FROM RPT."MADEWELL LOYALTY TIER" TS
            INNER JOIN
            (
                SELECT FISCALYEAR,
                    MASTERCUSTOMERID AS "MasterCustomerId",
                    MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence Number"
                    FROM
                    DS_DEV.PUBLIC.TRANSACTIONSUMMARY_MW_20240308 TT
                    LEFT JOIN ANALYTICS.TIMESUMMARY TS ON DATE(TT.TRANSACTIONDATE) = DATE(TS.DATEINUNIX)
                    WHERE TT.SUBTYPE in (''Demand'', ''Shipped'')
                    AND FISCALYEAR = (SELECT FISCALYEAR FROM CURRENT_WEEK)
                    AND FISCALWEEK <= (SELECT FISCALWEEK FROM CURRENT_WEEK)
        			GROUP BY ALL
            ) T ON TS."MasterCustomer ID" = T."MasterCustomerId" AND T."MasterCustomer Transaction Sequence Number" = TS."MasterCustomer Transaction Sequence"
    ),
    

    MWLAGDATA AS (
        SELECT
            CASE CSU."Loyalty Tier"
              WHEN ''Insider'' THEN 1
              WHEN ''Star'' THEN 2
              WHEN ''Icon'' THEN 3
              WHEN ''Icon+'' THEN 4
              ELSE 5
            END AS "Loyalty Tier",
            CASE WHEN TS.C_LOYALTYFLAG IS NOT NULL THEN 1 END As "Customer Loyalty Flag",
            CASE WHEN O.TYPE = ''Physical'' THEN ''Retail'' WHEN O.TYPE = ''Digital'' THEN ''Direct'' ELSE O.TYPE END AS "Channel Flag",
            T.ID AS "Date Key",
            OO."Id" AS "Brand ID",
            TS.MASTERCUSTOMERID AS "MasterCustomer ID",
            TS.SUBTYPE As "Sub Type",
            CASE C_STATUSTYPE 
                WHEN ''New'' THEN 1
                WHEN ''Continuing'' THEN 2
                WHEN ''Reactivated'' THEN 3
            END AS "Status Type",
            CASE C_STATUSTYPEFM
                WHEN ''New'' THEN 1
                WHEN ''Continuing'' THEN 2
                WHEN ''Reactivated'' THEN 3
            END AS "FM Status Type",
            CASE C_STATUSTYPEFQ
                WHEN ''New'' THEN 1
                WHEN ''Continuing'' THEN 2
                WHEN ''Reactivated'' THEN 3
            END AS "FQ Status Type",
            CASE C_STATUSTYPEFY
                WHEN ''New'' THEN 1
                WHEN ''Continuing'' THEN 2
                WHEN ''Reactivated'' THEN 3
            END AS "FY Status Type",
            CASE
                WHEN C.C_LOYALTYSIGNUPDATE IS NOT NULL THEN TL.ID
                ELSE NULL
            END AS "Loyalty Signup Date Id",
            Cast(C.C_LOYALTYSIGNUPDATE AS Date) AS "Loyalty Signup Date",
            CASE WHEN C.C_LoyaltySignUpChannel = ''POS'' THEN ''Retail''
                WHEN C.C_LoyaltySignUpChannel = ''ECOMM'' THEN ''Direct'' 
                ELSE C.C_LoyaltySignUpChannel 
            END AS "Loyalty SignUp Channel",
            concat(''00'', sourceemployeenumber) as "Cashier Associate",
            case when C.email = ''Unknown''
                then ''N''
                else ''Y''
            end as "Email Capture Flag",
            CASE WHEN (c.c_employeeflag = ''Y'' and c.c_employeeenddate is not NULL) THEN 1
                ELSE 0
            END AS "Employee Flag",
            TS.SALEREVENUE,
            TS.QUANTITY,
            TS.COSTBASIS,
            CASE
                WHEN TS.SUBTYPE IN (''Shipped'', ''Demand'') THEN TS.SALEREVENUE
            END AS DMD_R,
            CASE
                WHEN TS.SUBTYPE IN (''Shipped'', ''Demand'') THEN TS.QUANTITY
            END AS DMD_U,
            CASE
                WHEN TS.SUBTYPE IN (''Shipped'', ''Demand'') THEN TS.COSTBASIS
            END AS DMD_C,
            CASE
                WHEN R.TRANSACTIONID IS NOT NULL THEN ''Reward''
                ELSE ''No Reward''
            END AS "Reward Flag",
            CASE
                WHEN TS.SUBTYPE IN (''Shipped'', ''Demand'') THEN TS.TRANSACTIONID
                ELSE NULL
            END AS "Demand Transaction",
            
            CASE LTW."Loyalty Tier" 
              WHEN ''Insider'' THEN 1
              WHEN ''Star'' THEN 2
              WHEN ''Icon'' THEN 3
              WHEN ''Icon+'' THEN 4
              ELSE 5
            END AS "Loyalty Tier Week",
            
            CASE LTM."Loyalty Tier" 
                WHEN ''Insider'' THEN 1
                WHEN ''Star'' THEN 2
                WHEN ''Icon'' THEN 3
                WHEN ''Icon+'' THEN 4
                ELSE 5
            END AS "Loyalty Tier Month",
        
            CASE LTQ."Loyalty Tier" 
                WHEN ''Insider'' THEN 1
                WHEN ''Star'' THEN 2
                WHEN ''Icon'' THEN 3
                WHEN ''Icon+'' THEN 4
                ELSE 5
            END AS "Loyalty Tier Quarter",
        
            CASE LTY."Loyalty Tier" 
                WHEN ''Insider'' THEN 1
                WHEN ''Star'' THEN 2
                WHEN ''Icon'' THEN 3
                WHEN ''Icon+'' THEN 4
                ELSE 5
            END AS "Loyalty Tier Year",
        
            CASE LYC."Loyalty Tier" 
                WHEN ''Insider'' THEN 1
                WHEN ''Star'' THEN 2
                WHEN ''Icon'' THEN 3
                WHEN ''Icon+'' THEN 4
                ELSE 5
            END AS "Loyalty Tier LY Same"
        FROM
            DS_DEV.PUBLIC.TRANSACTIONSUMMARY_MW_20240308 TS
            LEFT JOIN CDP.PUBLIC.CUSTOMERSUMMARY C ON TS.MASTERCUSTOMERID = C.MASTERCUSTOMERID
            LEFT JOIN ANALYTICS.TIMESUMMARY T ON DATE(TS.TRANSACTIONDATE) = DATE(T.DATEINUNIX)
            LEFT JOIN ANALYTICS.TIMESUMMARY TL ON DATE(C.C_LOYALTYSIGNUPDATE) = DATE(TL.DATEINUNIX)
            LEFT JOIN RPT."MADEWELL LOYALTY TIER" CSU ON TS.MASTERCUSTOMERID = CSU."MasterCustomer ID" AND T.ID = CSU."Date Key" AND CSU."MasterCustomer Transaction Sequence" = TS.MASTERCUSTOMERTRANSACTIONSEQUENCE
            LEFT JOIN FACT_GET_TIER_YEAR_MW_WEEK_NRF LTW ON LTW."MasterCustomer ID" = TS.MASTERCUSTOMERID AND T.FISCALYEAR = LTW.FISCALYEAR AND LTW.FISCALWEEK = T.FISCALWEEK
            LEFT JOIN FACT_GET_TIER_YEAR_MW_MONTH_NRF LTM ON LTM."MasterCustomer ID" = TS.MASTERCUSTOMERID AND T.FISCALYEAR = LTM.FISCALYEAR AND LTM.FISCALMONTH = T.FISCALMONTH
            LEFT JOIN FACT_GET_TIER_YEAR_MW_QUARTER_NRF LTQ ON LTQ."MasterCustomer ID" = TS.MASTERCUSTOMERID AND T.FISCALYEAR = LTQ.FISCALYEAR AND LTQ.FISCALQUARTER = T.FISCALQUARTER
            LEFT JOIN FACT_GET_TIER_YEAR_MW_YEAR_NRF LTY ON LTY."MasterCustomer ID" = TS.MASTERCUSTOMERID AND T.FISCALYEAR = LTY.FISCALYEAR
            LEFT JOIN FACT_GET_TIER_YEAR_MW_LY_CURR_NRF LYC ON LYC."MasterCustomer ID" = TS.MASTERCUSTOMERID AND T.FISCALYEAR = LYC.FISCALYEAR
            LEFT JOIN CDP.PUBLIC.ORGANIZATIONSUMMARY O ON O.ID = TS.ORGANIZATIONID
            LEFT JOIN RPT."ORGANIZATION SUMMARY" OO ON OO.ORGANIZATIONID = TS.ORGANIZATIONID
            LEFT JOIN (
                SELECT
                    DISTINCT TIER,
                    TRANSACTIONID,
                    BRAND
                FROM
                    LOYALTY.JC_PRD.T_LOY_TRANS_REWARDS
                WHERE
                    REWARDS IS NOT NULL
            ) R ON R.TRANSACTIONID = TS.TRANSACTIONID
            AND TS.C_BRAND = LEFT(R.BRAND, 2)
        WHERE
            TS.MASTERCUSTOMERID <> ''-1''
            AND T.FISCALYEAR = 2023
        )

SELECT
"Loyalty Tier",
"Customer Loyalty Flag",
"Channel Flag",
"Date Key",
"Brand ID",
HASH("MasterCustomer ID"),
"Reward Flag",
"Sub Type",
"Status Type",
"FM Status Type",
"FQ Status Type",
"FY Status Type",
"Loyalty Signup Date Id",
"Loyalty SignUp Channel",
"Cashier Associate",
"Email Capture Flag",
"Employee Flag",
SUM(SALEREVENUE) AS "Net Revenue",
SUM(QUANTITY) AS "Net Units",
SUM(COSTBASIS) AS "Net Cost Basis",
SUM(DMD_R) AS "DMD Revenue",
SUM(DMD_U) AS "DMD Units",
SUM(DMD_C) AS "DMD Cost Basis",
"Demand Transaction",
"Loyalty Tier Week",
"Loyalty Tier Month",
"Loyalty Tier Quarter",
"Loyalty Tier Year",
"Loyalty Tier LY Same"
FROM
MWLAGDATA
GROUP BY ALL;

DROP TABLE IF EXISTS RPT."MADEWELL LOYALTY STATUS NRF";

ALTER TABLE Temp."MADEWELL LOYALTY STATUS NRF"  RENAME TO RPT."MADEWELL LOYALTY STATUS NRF";

RETURN ''Table RPT."MADEWELL LOYALTY STATUS" created or replaced successfully.'';

END';