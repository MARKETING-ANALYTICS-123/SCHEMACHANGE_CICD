CREATE OR REPLACE PROCEDURE "USP_POPULATE_MADEWELL_COUPON_STATS_AND_USAGE"()
RETURNS VARCHAR(100)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN

create or replace TABLE Temp."MADEWELL COUPON STATS AND USAGE" (
	"Issue Date Id" NUMBER(38,0),
	"Brand" VARCHAR(10),
	"Redemption Brand" VARCHAR(10),
	"Loyalty Tier" VARCHAR(100),
	"Brand Sort" NUMBER(38,0),
	"Code" VARCHAR(200),
	"Reward Name" VARCHAR(200),
	"Status" VARCHAR(10),
	"Loyalty Coupon Id" NUMBER(38,0),
	"Expiration Date Id" NUMBER(38,0),
	"Used Date Id" NUMBER(38,0),
	"Transaction Date Id" NUMBER(38,0),
	"Employee Flag" NUMBER(3,0),
	"Organization Id" VARCHAR(20),
	"Days to Usage" NUMBER(38,0),
	"Distribution Channel" VARCHAR(16777216),
    "Status Type" NUMBER(4, 0),
	"DMD Revenue" FLOAT,
	"DMD Units" NUMBER(38,0),
	"DMD Cost Basis" FLOAT,
	"Rewards Amount" FLOAT,
	"Transactions" VARCHAR(200)
);

INSERT INTO Temp."MADEWELL COUPON STATS AND USAGE"(
	"Issue Date Id",
	"Brand",
    "Redemption Brand",
	"Loyalty Tier",
	"Brand Sort",
	"Code",
	"Reward Name",
	"Status",
	"Loyalty Coupon Id",
	"Expiration Date Id",
	"Used Date Id",
	"Transaction Date Id",
    "Organization Id",
    "Employee Flag",
	"Days to Usage",
	"Distribution Channel",
    "Status Type",
	"DMD Revenue",
	"DMD Units",
	"DMD Cost Basis",
	"Rewards Amount",
	"Transactions"
) 

WITH GET_TIER_DETAIL AS
(
    SELECT EXTERNAL_CUSTOMER_ID,
    SNAPSHOT_DATE,
    LAST_ACTIVITY,
    TOP_TIER_NAME,
    BALANCE,
    ROW_NUMBER() OVER (
            PARTITION BY
              external_customer_id, snapshot_date
            ORDER BY
              last_activity desc nulls last, balance desc 
          ) AS rn
    FROM loyalty.mw_prd.prd_mw_customers_snapshot ss
    INNER JOIN cdp.PUBLIC.timesummary t ON DATE (ss.snapshot_date) = DATE(t.dateinunix) + 1
    WHERE t.fiscaldayofweek = 7
),

mw_final_unique_loyalty AS (
    SELECT DISTINCT EXTERNAL_CUSTOMER_ID, SNAPSHOT_DATE, TOP_TIER_NAME
    FROM GET_TIER_DETAIL
    WHERE rn = 1
),

DMD_TRANS AS (
    SELECT DISTINCT transactionid
        ,ts.mastercustomerid
        ,SUBSTR(ts.c_loyaltyflag, 5) AS c_loyaltyflag
        ,ts.c_loyaltytier
        ,transactiondate
        ,o.type as channel
        ,o.c_brand
        ,OO."Id" AS "Organization Id"
        ,CASE WHEN (c.c_employeeflag = ''Y'' and c.c_employeeenddate is not NULL) THEN 1
            ELSE 0 END AS "Employee Flag"
        ,C_STATUSTYPE
        ,sum(salerevenue) AS salerevenue
        ,sum(quantity) AS quantity
        ,sum(costbasis) AS costbasis
        ,max(c_rewardsredeemed) AS rewards,
    FROM cdp.PUBLIC.transactionsummary ts
    LEFT JOIN cdp.public.customersummary c on ts.mastercustomerid = c.mastercustomerid
    LEFT JOIN cdp.PUBLIC.organizationsummary o on ts.organizationid = o.id
    LEFT JOIN RPT."ORGANIZATION SUMMARY" OO on ts.organizationid = OO.organizationid
    WHERE ts.subtype IN (
            ''Shipped''
            ,''Demand''
            )
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),

ALL_REWARD AS
(
    SELECT 
      DISTINCT ORDER_ID,
      EXTERNAL_CUSTOMER_ID,
      LOYALTY_CUSTOMER_ID, 
      TRANSACTION_DATE,
      REWARD_CODE AS REWARD_CODE,
      NULL AS POS_CODE
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

    UNION ALL

    SELECT 
      DISTINCT ORDER_ID,
      EXTERNAL_CUSTOMER_ID,
      LOYALTY_CUSTOMER_ID, 
      TRANSACTION_DATE,
      REWARD_CODE,
      POS_CODE
    FROM
      (
        SELECT 
          DISTINCT ORDER_ID,
          EXTERNAL_CUSTOMER_ID,
          LOYALTY_CUSTOMER_ID, 
          TRANSACTION_DATE,
          TYPE,
          TRIM(L.VALUE) AS POS_CODE,
          TRIM(T.VALUE) AS REWARD_CODE
        FROM
          "LOYALTY"."MW_PRD"."PRD_MW_EVENTS",
          LATERAL SPLIT_TO_TABLE(OFFERS_REDEEMED, '','') T,
          LATERAL SPLIT_TO_TABLE(OFFER_POS_CODE, '','') L
          WHERE T.INDEX = L.INDEX
      )
    WHERE ORDER_ID IS NOT NULL
    AND REWARD_CODE IS NOT NULL
    AND TYPE = ''purchase''
),


USED_COUPONS_AFTER_NOV2023 AS
(
    SELECT 
    	rd.ID AS "Issue Date Id"
    	,''Madewell'' AS "Brand"
        ,TS.c_brand AS "Redemption Brand"
        ,COALESCE(CSU.TOP_TIER_NAME, ''Non-Loyalty'') AS "Loyalty Tier"
        ,COALESCE(C."CODE", R.ORDER_ID) AS "Code"
        ,COALESCE(C.POS_CODE, R.POS_CODE) AS "Reward Name"
        ,C."STATUS" AS "Status"
        ,C.LOYALTY_COUPON_ID AS "Loyalty Coupon Id"
        ,ed.ID AS "Expiration Date Id"
        ,ud.ID AS "Used Date Id"
        ,td.ID AS "Transaction Date Id"
        ,"Organization Id"
        ,"Employee Flag"
        ,DATEDIFF(day, DATE (C.REDEEMED_AT), DATE (C.USED_AT)) AS "Days to Usage"
        ,CASE WHEN TS.channel = ''Digital'' THEN ''Direct'' WHEN TS.channel = ''Physical'' THEN ''Retail'' END AS "Distribution Channel"
        ,CASE C_STATUSTYPE WHEN ''New'' THEN 1 WHEN ''Continuing'' THEN 2 WHEN ''Reactivated'' THEN 3 END AS "Status Type"
        ,SUM(CASE WHEN R.REWARD_CODE IS NOT NULL THEN TS.SALEREVENUE END) AS "DMD Revenue"
    	,SUM(CASE WHEN R.REWARD_CODE IS NOT NULL THEN TS.QUANTITY END) AS "DMD Units"
    	,SUM(CASE WHEN R.REWARD_CODE IS NOT NULL THEN TS.COSTBASIS END) AS "DMD Cost Basis"
    	,SUM(CASE WHEN R.REWARD_CODE IS NOT NULL THEN TS.Rewards END) AS "Rewards Amount"
    	,TS.TRANSACTIONID AS "Transaction ID"
    FROM (
            SELECT * FROM ALL_REWARD
    	) R
    LEFT JOIN (
    	   SELECT DISTINCT *
            FROM LOYALTY.MW_PRD.PRD_MW_COUPONS
    ) C ON C.LOYALTY_CUSTOMER_ID = R.LOYALTY_CUSTOMER_ID AND C.EXTERNAL_CUSTOMER_ID = R.EXTERNAL_CUSTOMER_ID AND C.code = R.REWARD_CODE
    
    LEFT JOIN mw_final_unique_loyalty CSU ON C.EXTERNAL_CUSTOMER_ID = csu.EXTERNAL_CUSTOMER_ID
          AND DATE (C.REDEEMED_AT) < DATE (csu.snapshot_date)
          AND DATE (C.REDEEMED_AT) >= DATEADD (WEEK, -1, DATE (csu.snapshot_date))
    LEFT JOIN DMD_TRANS TS ON R.ORDER_ID = TS.TRANSACTIONID AND R.EXTERNAL_CUSTOMER_ID = TS.C_LOYALTYFLAG
    LEFT JOIN cdp.public.timesummary rd ON DATE(C.REDEEMED_AT) = DATE(rd.dateinunix)
    LEFT JOIN cdp.public.timesummary ed ON DATE(C.EXPIRES_AT) = DATE(ed.dateinunix)
    LEFT JOIN cdp.public.timesummary ud ON DATE(C.USED_AT) = DATE(ud.dateinunix)
    LEFT JOIN cdp.public.timesummary td ON DATE(R.transaction_date) = DATE(td.dateinunix)
    LEFT OUTER JOIN cdp.PUBLIC.customersummary cs ON ts.mastercustomerid = cs.mastercustomerid
    GROUP BY ALL
),



USED_COUPONS_BEFORE_NOV2023 AS
(
    SELECT
    rd.ID AS "Issue Date Id"
	,''Madewell'' AS "Brand"
    ,TS.c_brand AS "Redemption Brand"
    ,COALESCE(CSU.TOP_TIER_NAME, ''Non-Loyalty'') AS "Loyalty Tier"
    ,C."CODE" AS "Code"
    ,C.POS_CODE AS "Reward Name"
    ,C."STATUS" AS "Status"
    ,C.LOYALTY_COUPON_ID AS "Loyalty Coupon Id"
    ,ed.ID AS "Expiration Date Id"
    ,ud.ID AS "Used Date Id"
    ,tt.ID AS "Transaction Date Id"
    ,"Organization Id"
    ,"Employee Flag"
    ,DATEDIFF(day, DATE (C.REDEEMED_AT), DATE (C.USED_AT)) AS "Days to Usage"
    ,CASE WHEN TS.channel = ''Digital'' THEN ''Direct'' WHEN TS.channel = ''Physical'' THEN ''Retail'' END AS "Distribution Channel"
    ,CASE C_STATUSTYPE WHEN ''New'' THEN 1 WHEN ''Continuing'' THEN 2 WHEN ''Reactivated'' THEN 3 END AS "Status Type"
    ,SUM(CASE WHEN R.REWARDS IS NOT NULL THEN TS.SALEREVENUE END) AS "DMD Revenue"
    ,SUM(CASE WHEN R.REWARDS IS NOT NULL THEN TS.QUANTITY END) AS "DMD Units"
    ,SUM(CASE WHEN R.REWARDS IS NOT NULL THEN TS.COSTBASIS END) AS "DMD Cost Basis"
    ,SUM(CASE WHEN R.REWARDS IS NOT NULL THEN TS.Rewards END) AS "Rewards Amount"
    ,TS.TRANSACTIONID AS "Transaction ID"
    FROM 
    (
        SELECT DISTINCT *
    	FROM LOYALTY.MW_PRD.PRD_MW_COUPONS
    ) C
    INNER JOIN (
        SELECT 
        DISTINCT 
        rewards
        ,distr_chan
        ,transactionid
        ,BRAND
        FROM LOYALTY.JC_PRD.T_LOY_TRANS_REWARDS
        WHERE ORDERDATE <= ''20230803''
    ) R ON lower(C."CODE") = lower(R.REWARDS)
    LEFT JOIN mw_final_unique_loyalty CSU ON C.EXTERNAL_CUSTOMER_ID = csu.EXTERNAL_CUSTOMER_ID
          AND DATE (C.REDEEMED_AT) < DATE (csu.snapshot_date)
          AND DATE (C.REDEEMED_AT) >= DATEADD (WEEK, -1, DATE (csu.snapshot_date))
    LEFT JOIN DMD_TRANS TS
        ON R.TRANSACTIONID = TS.TRANSACTIONID
    LEFT JOIN cdp.public.timesummary rd ON DATE(C.REDEEMED_AT) = DATE(rd.dateinunix)
    LEFT JOIN cdp.public.timesummary ed ON DATE(C.EXPIRES_AT) = DATE(ed.dateinunix)
    LEFT JOIN cdp.public.timesummary ud ON DATE(C.USED_AT) = DATE(ud.dateinunix)
    LEFT JOIN cdp.public.timesummary tt ON DATE(TS.transactiondate) = DATE(tt.dateinunix)
    GROUP BY ALL
),

COUPON_USED_BUT_NO_TRANSACTION AS
(
    SELECT 
        DISTINCT
    	rd.ID AS "Issue Date Id"
    	,''Madewell'' AS "Brand"
        ,NULL AS "Redemption Brand"
        ,COALESCE(CSU.TOP_TIER_NAME, ''Non-Loyalty'') AS "Loyalty Tier"
        ,C."CODE" AS "Code"
        ,C.POS_CODE AS "Reward Name"
        ,C."STATUS" AS "Status"
        ,C.LOYALTY_COUPON_ID AS "Loyalty Coupon Id"
        ,ed.ID AS "Expiration Date Id"
        ,ud.ID AS "Used Date Id"
        ,CASE WHEN ud.ID <= ''20230803'' THEN ud.ID ELSE NULL END AS "Transaction Date Id"
        ,NULL AS "Organization Id"
        ,0 AS "Employee Flag"
        ,DATEDIFF(day, DATE (C.REDEEMED_AT), DATE (C.USED_AT)) AS "Days to Usage"
        ,NULL AS "Distribution Channel"
        ,NULL AS "Status Type"
        ,NULL AS "DMD Revenue"
        ,NULL AS "DMD Units"
        ,NULL AS "DMD Cost Basis"
        ,NULL AS "Rewards Amount"
        ,NULL AS "Transaction ID"
    FROM (
            SELECT DISTINCT *
            FROM LOYALTY.MW_PRD.PRD_MW_COUPONS
            WHERE CODE NOT IN (SELECT DISTINCT "Code" FROM USED_COUPONS_AFTER_NOV2023)
            AND CODE NOT IN (SELECT DISTINCT "Code" FROM USED_COUPONS_BEFORE_NOV2023)
    	) C
    LEFT JOIN mw_final_unique_loyalty CSU ON C.EXTERNAL_CUSTOMER_ID = csu.EXTERNAL_CUSTOMER_ID
          AND DATE (C.REDEEMED_AT) < DATE (csu.snapshot_date)
          AND DATE (C.REDEEMED_AT) >= DATEADD (WEEK, -1, DATE (csu.snapshot_date))
    LEFT JOIN cdp.public.timesummary rd ON DATE(C.REDEEMED_AT) = DATE(rd.dateinunix)
    LEFT JOIN cdp.public.timesummary ed ON DATE(C.EXPIRES_AT) = DATE(ed.dateinunix)
    LEFT JOIN cdp.public.timesummary ud ON DATE(C.USED_AT) = DATE(ud.dateinunix)
)


SELECT
"Issue Date Id"
,"Brand"
,"Redemption Brand"
,"Loyalty Tier"
,CASE 
    WHEN "Brand" = ''Madewell''
        THEN 3
    END AS "Brand Sort"
,"Code"
,"Reward Name"
,"Status"
,"Loyalty Coupon Id"
,"Expiration Date Id"
,"Used Date Id"
,"Transaction Date Id"
,"Organization Id"
,coalesce("Employee Flag", 0) AS "Employee Flag"
,"Days to Usage"
,"Distribution Channel"
,"Status Type"
,"DMD Revenue"
,"DMD Units"
,"DMD Cost Basis"
,"Rewards Amount"
,"Transaction ID"
FROM 
USED_COUPONS_BEFORE_NOV2023

UNION ALL

SELECT
"Issue Date Id"
,"Brand"
,"Redemption Brand"
,"Loyalty Tier"
,CASE 
    WHEN "Brand" = ''Madewell''
        THEN 3
    END AS "Brand Sort"
,"Code"
,"Reward Name"
,"Status"
,"Loyalty Coupon Id"
,"Expiration Date Id"
,"Used Date Id"
,"Transaction Date Id"
,"Organization Id"
,coalesce("Employee Flag", 0) AS "Employee Flag"
,"Days to Usage"
,"Distribution Channel"
,"Status Type"
,"DMD Revenue"
,"DMD Units"
,"DMD Cost Basis"
,"Rewards Amount"
,"Transaction ID"
FROM
USED_COUPONS_AFTER_NOV2023

UNION ALL

SELECT 
"Issue Date Id"
,"Brand"
,"Redemption Brand"
,"Loyalty Tier"
,CASE 
    WHEN "Brand" = ''Madewell''
        THEN 3
    END AS "Brand Sort"
,"Code"
,"Reward Name"
,"Status"
,"Loyalty Coupon Id"
,"Expiration Date Id"
,"Used Date Id"
,"Transaction Date Id"
,"Organization Id"
,coalesce("Employee Flag", 0) AS "Employee Flag"
,"Days to Usage"
,"Distribution Channel"
,"Status Type"
,"DMD Revenue"
,"DMD Units"
,"DMD Cost Basis"
,"Rewards Amount"
,"Transaction ID"
FROM
COUPON_USED_BUT_NO_TRANSACTION;

DROP TABLE IF EXISTS RPT."MADEWELL COUPON STATS AND USAGE";

ALTER TABLE Temp."MADEWELL COUPON STATS AND USAGE" RENAME TO RPT."MADEWELL COUPON STATS AND USAGE";

RETURN ''RPT."MADEWELL COUPON STATS AND USAGE" created or replaced successfully'';
END
';