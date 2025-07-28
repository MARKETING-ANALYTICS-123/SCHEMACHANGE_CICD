CREATE OR REPLACE PROCEDURE "USP_POPULATE_COUPON_VALIDATION_KPI"()
RETURNS VARCHAR(100)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
CREATE OR REPLACE TABLE RPT."COUPON VALIDATION KPI"
AS
WITH ALL_REWARD AS
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

DMD_TRANS_JC AS (
    SELECT DISTINCT transactionid
        ,ts.mastercustomerid
        ,ts.c_loyaltyflag
        ,ts.c_loyaltytier
        ,t.id AS "Transaction Date Id"
        ,o.type as channel
        ,o.c_brand
        ,OO."Id" AS "Organization ID"
        ,CASE WHEN (c.c_employeeflag = ''Y'' and c.c_employeeenddate is not NULL) THEN 1
            ELSE 0 END AS "Employee Flag"
        ,C_STATUSTYPEBRAND
        ,sum(salerevenue) AS salerevenue
        ,sum(quantity) AS quantity
        ,sum(costbasis) AS costbasis
        ,max(c_rewardsredeemed) AS rewards
    FROM cdp_jc.PUBLIC.transactionsummary ts
    LEFT JOIN cdp_jc.public.customersummary c on ts.mastercustomerid = c.mastercustomerid
    LEFT JOIN cdp_jc.PUBLIC.organizationsummary o on ts.organizationid = o.id
    LEFT JOIN RPT."ORGANIZATION SUMMARY" OO on ts.organizationid = OO.organizationid
    LEFT JOIN cdp_jc.public.timesummary t on date(ts.transactiondate) = date(t.dateinunix)
    WHERE ts.subtype IN (
            ''Shipped''
            ,''Demand''
            )
    AND fiscalyear >= 2022
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),

ALL_REWARD_JC AS
(
    SELECT
      DISTINCT ORDER_ID,
      EXTERNAL_CUSTOMER_ID,
      LOYALTY_CUSTOMER_ID, 
      TRANSACTION_DATE,
      REWARD_CODE AS REWARD_CODE,
      ''transactional_reward1'' AS POS_CODE,
      TIER_AT_EVENT
    FROM
      (
        SELECT 
          ORDER_ID,
          EXTERNAL_CUSTOMER_ID,
          LOYALTY_CUSTOMER_ID, 
          TRANSACTION_DATE,
          type,
          TIER_AT_EVENT,
          TRIM(t.VALUE) AS REWARD_CODE
        FROM
          "LOYALTY"."JC_PRD"."PRD_JC_EVENTS",
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
      POS_CODE,
      TIER_AT_EVENT
    FROM
      (
        SELECT 
          DISTINCT ORDER_ID,
          EXTERNAL_CUSTOMER_ID,
          LOYALTY_CUSTOMER_ID, 
          TRANSACTION_DATE,
          TYPE,
          TIER_AT_EVENT,
          TRIM(T.VALUE) AS REWARD_CODE,
          TRIM(L.VALUE) AS POS_CODE
        FROM
          "LOYALTY"."JC_PRD"."PRD_JC_EVENTS",
          LATERAL SPLIT_TO_TABLE(OFFERS_REDEEMED, '','') T,
          LATERAL SPLIT_TO_TABLE(OFFER_POS_CODE, '','') L
          WHERE T.INDEX = L.INDEX
      )
    WHERE ORDER_ID IS NOT NULL
    AND REWARD_CODE IS NOT NULL
    AND TYPE = ''purchase''
)


SELECT
''Madewell'' AS BRAND,
TT.ID AS Date,
NULL AS REWARD_ISSUED,
COUNT(DISTINCT CASE WHEN C."CODE" IS NULL THEN R.ORDER_ID ELSE C."CODE" END) AS REWARD_REDEEMED,
COUNT(DISTINCT TS.TRANSACTIONID) AS TRANS_COUNT,
SUM(Quantity) AS UNITS,
SUM(Salerevenue) AS DMD,
SUM(Costbasis) AS DMD_C
FROM (
        SELECT * FROM ALL_REWARD
    ) R
LEFT JOIN (
       SELECT DISTINCT *
        FROM LOYALTY.MW_PRD.PRD_MW_COUPONS
) C ON C.LOYALTY_CUSTOMER_ID = R.LOYALTY_CUSTOMER_ID AND C.EXTERNAL_CUSTOMER_ID = R.EXTERNAL_CUSTOMER_ID AND C.code = R.REWARD_CODE
JOIN DMD_TRANS TS ON R.ORDER_ID = TS.TRANSACTIONID AND R.EXTERNAL_CUSTOMER_ID = TS.C_LOYALTYFLAG
JOIN CDP.PUBLIC.TIMESUMMARY TT ON TT.DATEINUNIX::DATE = R.transaction_date::DATE
WHERE FISCALYEAR >= 2023
GROUP BY ALL


UNION

SELECT
''JCFA'' AS BRAND,
TT.ID AS Date,
NULL AS REWARD_ISSUED,
COUNT(DISTINCT CASE WHEN C."CODE" IS NULL THEN R.ORDER_ID ELSE C."CODE" END) AS REWARD_REDEEMED,
COUNT(DISTINCT R.ORDER_ID) AS TRANS_COUNT,
SUM(CASE WHEN R.REWARD_CODE IS NOT NULL THEN Quantity END) AS UNITS,
SUM(CASE WHEN R.REWARD_CODE IS NOT NULL THEN Salerevenue END) AS DMD,
SUM(CASE WHEN R.REWARD_CODE IS NOT NULL THEN Costbasis END) AS DMD_C
FROM (
        SELECT * FROM ALL_REWARD_JC
    ) R
LEFT JOIN (
       SELECT DISTINCT *
        FROM LOYALTY.JC_PRD.PRD_JC_COUPONS
) C ON C.LOYALTY_CUSTOMER_ID = R.LOYALTY_CUSTOMER_ID AND C.EXTERNAL_CUSTOMER_ID = R.EXTERNAL_CUSTOMER_ID AND C.code = R.REWARD_CODE
LEFT JOIN DMD_TRANS_JC TS ON R.ORDER_ID = TS.TRANSACTIONID
JOIN CDP_JC.PUBLIC.TIMESUMMARY TT ON TT.DATEINUNIX::DATE = R.transaction_date::DATE
WHERE FISCALYEAR >= 2023
GROUP BY ALL


UNION

SELECT
''Madewell'' AS BRAND,
TS.ID AS Date,
COUNT(DISTINCT CODE) AS REWARD_ISSUED,
NULL AS REWARD_REDEEMED,
NULL AS TRANS_COUNT,
NULL AS UNITS,
NULL AS DMD,
NULL AS DMD_C
FROM
LOYALTY.MW_PRD.PRD_MW_COUPONS C
JOIN CDP.PUBLIC.TIMESUMMARY TS ON DATE(C.REDEEMED_AT) = DATE(TS.DATEINUNIX)
WHERE FISCALYEAR >= 2023
GROUP BY ALL

UNION

SELECT
''JCFA'' AS BRAND,
TS.ID AS Date,
COUNT(DISTINCT CODE) AS REWARD_ISSUED,
NULL AS REWARD_REDEEMED,
NULL AS TRANS_COUNT,
NULL AS UNITS,
NULL AS DMD,
NULL AS DMD_C
FROM
LOYALTY.JC_PRD.PRD_JC_COUPONS C
JOIN CDP_JC.PUBLIC.TIMESUMMARY TS ON DATE(C.REDEEMED_AT) = DATE(TS.DATEINUNIX)
WHERE FISCALYEAR >= 2023
GROUP BY ALL;';