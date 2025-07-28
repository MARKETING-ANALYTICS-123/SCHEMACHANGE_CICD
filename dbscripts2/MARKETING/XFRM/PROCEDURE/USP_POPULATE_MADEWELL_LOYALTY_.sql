CREATE OR REPLACE PROCEDURE "USP_POPULATE_MADEWELL_LOYALTY"()
RETURNS VARCHAR(100)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    StartDateId NUMBER(8, 0);
    EndDateId NUMBER(8, 0);
BEGIN

    StartDateId := (
        SELECT
            MAX(ID)
        FROM
            EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY
        WHERE
            FISCALYEAR = (
                SELECT
                    FISCALYEAR
                FROM
                    CDP.PUBLIC.TIMESUMMARY
                WHERE
                    TO_DATE(DATEINUNIX) > CURRENT_DATE - 7
                    AND TO_DATE(DATEINUNIX) <= CURRENT_DATE
                    AND FISCALDAYOFWEEK = 7
            ) - 2
            AND FISCALDAYOFMONTH = 1
            AND FISCALMONTH = 1
    );

    EndDateId := (
        SELECT
            MAX(ID)
        FROM
            CDP.PUBLIC.TIMESUMMARY TS
        WHERE
            TS.DATEINUNIX::DATE <= CURRENT_DATE::DATE
            AND TS.FISCALDAYOFWEEK = 7
    );

    
    CREATE OR REPLACE TABLE Temp."MADEWELL LOYALTY" (
    	"Loyalty Tier" NUMBER(4, 0),
    	"Customer Loyalty Flag" VARCHAR(100),
        "Channel Flag" VARCHAR(100),
    	"Date Key" NUMBER(38,0),
        "Promo Code" VARCHAR(100),
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
        "Promo Net Revenue" FLOAT,
        "Promo Net Units" INT,
        "Promo Net Cost Basis" FLOAT,
        "Promo Demand Revenue" FLOAT,
        "Promo Demand Units" INT,
        "Promo Demand Cost Basis" FLOAT,
        "Promo Rewards Amount" FLOAT,
        "Promo Shipping Cost" FLOAT,
        "Promo Other Cost" FLOAT,
        "Promo Demand Transaction" VARCHAR(200),
        "Loyalty Tier Week" NUMBER(4, 0),
        "Loyalty Tier Month" NUMBER(4, 0),
        "Loyalty Tier Quarter" NUMBER(4, 0),
        "Loyalty Tier Year" NUMBER(4, 0),
        "Loyalty Tier LY Same" NUMBER(4, 0)
    );

    INSERT INTO Temp."MADEWELL LOYALTY" (
        "Loyalty Tier",
        "Customer Loyalty Flag",
        "Channel Flag",
        "Date Key",
        "Promo Code",
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
        "Promo Net Revenue",
        "Promo Net Units",
        "Promo Net Cost Basis",
        "Promo Demand Revenue",
        "Promo Demand Units",
        "Promo Demand Cost Basis",
        "Promo Rewards Amount",
        "Promo Shipping Cost",
        "Promo Other Cost",
        "Promo Demand Transaction",
        "Loyalty Tier Week",
        "Loyalty Tier Month",
        "Loyalty Tier Quarter",
        "Loyalty Tier Year",
        "Loyalty Tier LY Same"
    )

    WITH PromoCodeDistribution AS
    (
        SELECT
            T.SOURCETRANSACTIONITEMNUMBER ,
            TIM.MESSAGEID,
            CASE 
                WHEN Tim.MESSAGEID IS NOT NULL THEN T.SALEREVENUE / COUNT(TIM.MESSAGEID) OVER (PARTITION BY T.SOURCETRANSACTIONITEMNUMBER, TRANSACTIONID)
                ELSE SALEREVENUE END
                AS SALEREVENUE,
           CASE 
                WHEN Tim.MESSAGEID IS NOT NULL THEN T.Quantity / COUNT (TIM.MESSAGEID) OVER (PARTITION BY T.SOURCETRANSACTIONITEMNUMBER, TRANSACTIONID)
                ELSE QUANTITY END
                AS QUANTITY,
           CASE 
                WHEN Tim.MESSAGEID IS NOT NULL THEN T.COSTBASIS / COUNT (TIM.MESSAGEID) OVER (PARTITION BY T.SOURCETRANSACTIONITEMNUMBER, TRANSACTIONID) 
                ELSE COSTBASIS END
                AS COSTBASIS
            FROM 
            CDP.PUBLIC.TRANSACTIONSUMMARY T
            LEFT JOIN CDP.PUBLIC.TIMESUMMARY TS ON TO_DATE(TS.DATEINUNIX) = TO_DATE(T.TRANSACTIONTIMESTAMP)
            LEFT JOIN CDP.PUBLIC.TRANSACTIONITEMMESSAGEXREF AS TIM ON T.SOURCETRANSACTIONITEMNUMBER = TIM.SOURCETRANSACTIONITEMNUMBER
          WHERE
            TS.ID >= :StartDateId
            AND TS.ID <= :EndDateId
            AND T.MASTERCUSTOMERID <> ''-1''
    )
	,
	TRANSFIL AS (
	SELECT * FROM  CDP.PUBLIC.TRANSACTIONSUMMARY TS 
        INNER JOIN CDP.PUBLIC.TIMESUMMARY T ON DATE(TS.TRANSACTIONDATE) = DATE(T.DATEINUNIX)
		AND T.ID >= :StartDateId
        AND T.ID <= :EndDateId
    )

	,

    ORDER_WITH_REWARD AS (
        SELECT 
          DISTINCT ORDER_ID
        FROM
          (
            SELECT 
              ORDER_ID,
              EXTERNAL_CUSTOMER_ID,
              LOYALTY_CUSTOMER_ID, 
              TRANSACTION_DATE,
              type,
              TRIM(t.VALUE) AS REWARD_CODE
            FROM
              "LOYALTY"."MW_PRD"."PRD_MW_EVENTS",
            LATERAL SPLIT_TO_TABLE(REWARD_CODE, '','') t
          ) a
        WHERE ORDER_ID IS NOT NULL
        AND REWARD_CODE IS NOT NULL
        AND type = ''purchase''
        GROUP BY ALL
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
        PCD.MESSAGEID AS "Promo Code",
        OO."Id" AS "Brand ID",
        C.MASTERCUSTOMERID AS "MasterCustomer ID",
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
        PCD.SALEREVENUE,
        PCD.QUANTITY,
        PCD.COSTBASIS,
        CASE
            WHEN TS.SUBTYPE IN (''Shipped'', ''Demand'') THEN PCD.SALEREVENUE
        END AS DMD_R,
        CASE
            WHEN TS.SUBTYPE IN (''Shipped'', ''Demand'') THEN PCD.QUANTITY
        END AS DMD_U,
        CASE
            WHEN TS.SUBTYPE IN (''Shipped'', ''Demand'') THEN PCD.COSTBASIS
        END AS DMD_C,
        CASE
            WHEN R.ORDER_ID IS NOT NULL THEN ''Reward''
            WHEN TS.TRANSACTIONTIMESTAMPFK < 20230928 AND TS.C_REWARDSREDEEMEDFLAG = ''Y'' THEN ''Reward''
            ELSE ''No Reward''
        END AS "Reward Flag",
        CASE
            WHEN TS.SUBTYPE IN (''Shipped'', ''Demand'') THEN TS.TRANSACTIONID
            ELSE NULL
        END AS "Demand Transaction",
        TS.SALEREVENUE AS "Promo Net Revenue",
        TS.QUANTITY AS "Promo Net Units",
        TS.COSTBASIS AS "Promo Net Cost Basis",
        CASE 
            WHEN ts.subtype IN (
                    ''Shipped''
                    ,''Demand''
                    )
                THEN ts.salerevenue
            END AS "Promo Demand Revenue",
        CASE 
            WHEN ts.subtype IN (
                    ''Shipped''
                    ,''Demand''
                    )
                THEN ts.quantity
            END AS "Promo Demand Units",
        CASE 
            WHEN ts.subtype IN (
                    ''Shipped''
                    ,''Demand''
                    )
                THEN ts.costbasis
            END AS "Promo Demand Cost Basis",
        CASE 
            WHEN TS.subtype IN (
                    ''Shipped''
                    ,''Demand''
                    )
                THEN TS.c_rewardsredeemed
            END AS "Promo Rewards Amount",
        CASE 
            WHEN TS.subtype IN (
                    ''Shipped''
                    ,''Demand''
                    )
                THEN COALESCE(TS.shippingcost, 0)
            END AS "Promo Shipping Cost",
        CASE 
            WHEN TS.subtype IN (
                    ''Shipped''
                    ,''Demand''
                    )
                THEN COALESCE(TS.OtherCost, 0)
            END AS "Promo Other Cost",
        ts.transactionid AS "Promo Demand Transaction",
        
        CASE CSU."Loyalty Tier" 
              WHEN ''Insider'' THEN 1
              WHEN ''Star'' THEN 2
              WHEN ''Icon'' THEN 3
              WHEN ''Icon+'' THEN 4
              ELSE 5
        END AS "Loyalty Tier Week",
        
        CASE CSU."Loyalty Tier Month" 
              WHEN ''Insider'' THEN 1
              WHEN ''Star'' THEN 2
              WHEN ''Icon'' THEN 3
              WHEN ''Icon+'' THEN 4
              ELSE 5
        END AS "Loyalty Tier Month",
    
        CASE CSU."Loyalty Tier Quarter" 
              WHEN ''Insider'' THEN 1
              WHEN ''Star'' THEN 2
              WHEN ''Icon'' THEN 3
              WHEN ''Icon+'' THEN 4
              ELSE 5
        END AS "Loyalty Tier Quarter",
    
        CASE CSU."Loyalty Tier Year" 
              WHEN ''Insider'' THEN 1
              WHEN ''Star'' THEN 2
              WHEN ''Icon'' THEN 3
              WHEN ''Icon+'' THEN 4
              ELSE 5
        END AS "Loyalty Tier Year",
    
        CASE CSU."Loyalty Tier Year LY" 
              WHEN ''Insider'' THEN 1
              WHEN ''Star'' THEN 2
              WHEN ''Icon'' THEN 3
              WHEN ''Icon+'' THEN 4
              ELSE 5
        END AS "Loyalty Tier LY Same"
        FROM
        CDP.PUBLIC.CUSTOMERSUMMARY C
        LEFT JOIN TRANSFIL TS ON TS.MASTERCUSTOMERID = C.MASTERCUSTOMERID
        LEFT JOIN PromoCodeDistribution PCD ON TS.SOURCETRANSACTIONITEMNUMBER = PCD.SOURCETRANSACTIONITEMNUMBER
        LEFT JOIN CDP.PUBLIC.TIMESUMMARY T ON DATE(TS.TRANSACTIONDATE) = DATE(T.DATEINUNIX)
        LEFT JOIN CDP.PUBLIC.TIMESUMMARY TL ON DATE(C.C_LOYALTYSIGNUPDATE) = DATE(TL.DATEINUNIX)
        LEFT JOIN RPT."MADEWELL LOYALTY TIER" CSU ON TS.MASTERCUSTOMERID = CSU."MasterCustomer ID" AND T.ID = CSU."Date Key"  AND CSU."MasterCustomer Transaction Sequence" = TS.MASTERCUSTOMERTRANSACTIONSEQUENCE
        LEFT JOIN CDP.PUBLIC.ORGANIZATIONSUMMARY O ON O.ID = TS.ORGANIZATIONID
        LEFT JOIN RPT."ORGANIZATION SUMMARY" OO ON OO.ORGANIZATIONID = TS.ORGANIZATIONID
        LEFT JOIN ORDER_WITH_REWARD R ON R.ORDER_ID = TS.TRANSACTIONID
        WHERE
        C.MASTERCUSTOMERID <> ''-1''
        

        UNION ALL

        SELECT
        5 AS "Loyalty Tier",
        CASE WHEN TS.C_LOYALTYFLAG IS NOT NULL THEN 1 END As "Customer Loyalty Flag",
        CASE WHEN O.TYPE = ''Physical'' THEN ''Retail'' WHEN O.TYPE = ''Digital'' THEN ''Direct'' ELSE O.TYPE END AS "Channel Flag",
        T.ID AS "Date Key",
        NULL AS "Promo Code",
        OO."Id" AS "Brand ID",
        C.MASTERCUSTOMERID AS "MasterCustomer ID",
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
            WHEN R.ORDER_ID IS NOT NULL THEN ''Reward''
            WHEN TS.TRANSACTIONTIMESTAMPFK < 20230928 AND TS.C_REWARDSREDEEMEDFLAG = ''Y'' THEN ''Reward''
            ELSE ''No Reward''
        END AS "Reward Flag",
        CASE
            WHEN TS.SUBTYPE IN (''Shipped'', ''Demand'') THEN TS.TRANSACTIONID
            ELSE NULL
        END AS "Demand Transaction",
        0 AS "Promo Net Revenue",
        0 AS "Promo Net Units",
        0 AS "Promo Net Cost Basis",
        0 AS "Promo Demand Revenue",
        0 AS "Promo Demand Units",
        0 AS "Promo Demand Cost Basis",
        0 AS "Promo Rewards Amount",
        0 AS "Promo Shipping Cost",
        0 AS "Promo Other Cost",
        TS.TRANSACTIONID AS "Promo Demand Transaction",
        5 AS "Loyalty Tier Week",
        5 AS "Loyalty Tier Month",
        5 AS "Loyalty Tier Quarter",
        5 AS "Loyalty Tier Year",
        5 AS "Loyalty Tier LY Same"
    FROM
        CDP.PUBLIC.CUSTOMERSUMMARY C
        LEFT JOIN CDP.PUBLIC.TRANSACTIONSUMMARY TS ON TS.MASTERCUSTOMERID = C.MASTERCUSTOMERID
        LEFT JOIN CDP.PUBLIC.TIMESUMMARY T ON DATE(TS.TRANSACTIONDATE) = DATE(T.DATEINUNIX)
        LEFT JOIN CDP.PUBLIC.ORGANIZATIONSUMMARY O ON O.ID = TS.ORGANIZATIONID
        LEFT JOIN RPT."ORGANIZATION SUMMARY" OO ON OO.ORGANIZATIONID = TS.ORGANIZATIONID
        LEFT JOIN CDP.PUBLIC.TIMESUMMARY TL ON DATE(C.C_LOYALTYSIGNUPDATE) = DATE(TL.DATEINUNIX)
        LEFT JOIN ORDER_WITH_REWARD R ON R.ORDER_ID = TS.TRANSACTIONID
    WHERE
        C.MASTERCUSTOMERID <> ''-1''
        AND TL.FISCALYEAR >= 2022
		AND T.ID < :StartDateId OR T.ID > :EndDateId
)

SELECT
"Loyalty Tier",
"Customer Loyalty Flag",
"Channel Flag",
"Date Key",
"Promo Code",
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
SUM("Promo Net Revenue") AS "Promo Net Revenue",
SUM("Promo Net Units") AS "Promo Net Units",
SUM("Promo Net Cost Basis") AS "Promo Net Cost Basis",
SUM("Promo Demand Revenue") AS "Promo Demand Revenue",
SUM("Promo Demand Units") AS "Promo Demand Units",
SUM("Promo Demand Cost Basis") AS "Promo Demand Cost Basis",
SUM("Promo Rewards Amount") AS "Promo Rewards Amount",
SUM("Promo Shipping Cost") AS "Promo Shipping Cost",
SUM("Promo Other Cost") AS "Promo Other Cost",
"Promo Demand Transaction",
"Loyalty Tier Week",
"Loyalty Tier Month",
"Loyalty Tier Quarter",
"Loyalty Tier Year",
"Loyalty Tier LY Same"
FROM
MWLAGDATA
GROUP BY ALL;

DROP TABLE IF EXISTS RPT."MADEWELL LOYALTY";

ALTER TABLE Temp."MADEWELL LOYALTY"  RENAME TO RPT."MADEWELL LOYALTY";

RETURN ''Table RPT."MADEWELL LOYALTY" created or replaced successfully.'';

END;
';