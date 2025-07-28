create or replace view VW_LTY_CUSTOMER_BY_TIER_VIEW_PASSPORT(
	"Loyalty Tier",
	"Loyalty Flag",
	"Tier Sort",
	"Date Key",
	"Channel Flag",
	"Brand ID",
	"MasterCustomer ID",
	"MasterCustomer Transaction Sequence",
	"Reward Flag",
	"Net Revenue",
	"Net Units",
	"Net Cost Basis",
	"DMD Revenue",
	"DMD Units",
	"DMD Cost Basis",
	"DMD Transactions"
) as

------------------------------------------------------- PASSPORT 2023 SNAPSHOT LOGIC ----------------------------------------------------------------

WITH PA_CUSTOMERS_SNAPSHOT_2023 AS (
        SELECT DISTINCT
          ts.mastercustomerid,
          ss.external_customer_id,
          ss.snapshot_date,
          ss.last_activity,
          ss.record_created_date,
          ss.top_tier_name,
          balance
        FROM
          loyalty.jc_prd.prd_jc_customers_snapshot ss
          INNER JOIN cdp_jc.PUBLIC.timesummary t ON DATE (ss.snapshot_date) = DATE(t.dateinunix) + 1
          INNER JOIN cdp_jc.PUBLIC.customer c ON c.c_loyaltyid = ss.external_customer_id
          INNER JOIN cdp_jc.PUBLIC.mastercustomer mc ON mc.customerid = c.id
          INNER JOIN cdp_jc.PUBLIC.transactionsummary ts ON ts.mastercustomerid = mc.mastercustomerid
        WHERE
          ts.mastercustomerid<>'-1'
          AND mc.customerid LIKE 'LOY_%'
          AND t.fiscaldayofweek = 7
),

fa_cust_loyalty_unique_toptier AS
(
    SELECT mastercustomerid,
        external_customer_id,
        snapshot_date,
        top_tier_name,
        record_created_date,
        last_activity,
        balance,
        ROW_NUMBER() OVER (PARTITION BY mastercustomerid, snapshot_date order by last_activity desc nulls last, balance desc) AS rn
        FROM PA_CUSTOMERS_SNAPSHOT_2023
),

fa_final_unique_loyalty AS 
(
     SELECT mastercustomerid,
        external_customer_id,
        snapshot_date,
        top_tier_name,
        record_created_date
     FROM fa_cust_loyalty_unique_toptier
     WHERE rn = 1
),

----------------------------------------------------------- PASSPORT 2023 LOGIC ------------------------------------------------------------------------
JCLAGDATA AS (
    SELECT
      COALESCE(CASE 
        WHEN csu.top_tier_name IS NULL AND csu.external_customer_id IS NOT NULL THEN 'Green'
        ELSE csu.top_tier_name
        END,
        CASE WHEN LOWER(R.Tier) LIKE '%green%' THEN 'Green'
        WHEN LOWER(R.Tier) LIKE '%navy%' THEN 'Navy'
        WHEN LOWER(R.Tier) LIKE '%gold%' THEN 'Gold'
        END,
        CASE WHEN LOWER(ts.c_loyaltytier) LIKE '%green%' THEN 'Green'
        WHEN LOWER(ts.c_loyaltytier) LIKE '%navy%' THEN 'Navy'
        WHEN LOWER(ts.c_loyaltytier) LIKE '%gold%' THEN 'Gold'
        WHEN LOWER(ts.c_loyaltytier) IS NULL AND DATE (ts.transactiondate) >= DATE (c.c_loyaltysignupdate) THEN 'Green'
        ELSE 'Non-Loyalty'
        END
      ) AS "Loyalty Tier"
    ,CASE 
        WHEN "Loyalty Tier" = 'Green'
            THEN 1
        WHEN "Loyalty Tier" = 'Navy'
            THEN 2
        WHEN "Loyalty Tier" = 'Gold'
            THEN 3
        END AS "Tier Sort"
    ,T.id AS "Date Key"
    ,CASE 
        WHEN TS.C_DEVICETYPE = 'DROP'
            THEN 'Wholesale'
        WHEN TS.ORGANIZATIONID IN (
                'LBDF'
                ,'LBDI'
                )
        AND TS.C_DEVICETYPE <> 'DROP'
            THEN 'Ecomm'
        ELSE 'Stores'
        END AS "Channel Flag"
    ,ORGANIZATIONID AS "Brand ID"
    ,TS.mastercustomerid AS "MasterCustomer ID"
    ,TS.mastercustomertransactionsequence AS "MasterCustomer Transaction Sequence"
    ,CASE 
        WHEN R.TRANSACTIONID IS NOT NULL
            THEN 'Reward'
        WHEN R.TRANSACTIONID IS NULL
            THEN 'No Reward'
        END AS "Reward Flag"
    ,TS.SALEREVENUE
    ,TS.QUANTITY
    ,TS.COSTBASIS
    ,CASE 
        WHEN ts.subtype IN (
                'Shipped'
                ,'Demand'
                )
            THEN ts.salerevenue
        END AS dmd_r
    ,CASE 
        WHEN ts.subtype IN (
                'Shipped'
                ,'Demand'
                )
            THEN ts.quantity
        END AS dmd_u
    ,CASE 
        WHEN ts.subtype IN (
                'Shipped'
                ,'Demand'
                )
            THEN ts.costbasis
        END AS dmd_c
    ,CASE 
        WHEN ts.subtype IN (
                'Shipped'
                ,'Demand'
                )
            THEN ts.transactionid
        ELSE NULL
    END AS "Demand Transaction"
FROM cdp_jc.PUBLIC.transactionsummary ts
LEFT JOIN cdp_jc.PUBLIC.CUSTOMERSUMMARY C ON TS.MASTERCUSTOMERID = C.MASTERCUSTOMERID
LEFT OUTER JOIN cdp_jc.PUBLIC.timesummary t
            ON DATE(ts.transactiondate) = DATE(t.dateinunix)
LEFT OUTER JOIN fa_final_unique_loyalty csu
            ON ts.mastercustomerid = csu.mastercustomerid
            AND DATE(TRANSACTIONDATE) < DATE(csu.snapshot_date) AND DATE(TRANSACTIONDATE) >= DATEADD(WEEK, -1, DATE(csu.snapshot_date))
LEFT JOIN cdp_jc.PUBLIC.organizationsummary O ON O.id=Ts.organizationid
LEFT JOIN (
    SELECT DISTINCT TIER
        ,TRANSACTIONID,Brand
    FROM LOYALTY.JC_PRD.T_LOY_TRANS_REWARDS
    WHERE REWARDS IS NOT NULL
    ) R ON R.TRANSACTIONID = TS.TRANSACTIONID AND TS.C_BRAND = LEFT(R.BRAND,2)
WHERE 
ts.mastercustomerid <> '-1'
AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM CDP.PUBLIC.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR - 2 FROM CDP_JC.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7) AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
    SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
    WHERE TS.DATEINUNIX::DATE <= current_date::DATE
    AND TS.FISCALDAYOFWEEK = 7
  )
)

SELECT
  "Loyalty Tier",
  CASE
    WHEN "Loyalty Tier" IS NOT NULL
    AND "Loyalty Tier" <> 'Non-Loyalty' THEN 'Loyalty'
    ELSE 'Non-Loyalty'
  END AS "Loyalty Flag",
  "Tier Sort",
  "Date Key",
  CASE
    WHEN "Channel Flag"='Ecomm' THEN 'Direct'
    WHEN "Channel Flag"='Stores' THEN 'Retail'
  END AS "Channel Flag",
  "Brand ID",
  "MasterCustomer ID",
  "MasterCustomer Transaction Sequence",
  "Reward Flag",
  SUM(SALEREVENUE) AS "Net Revenue",
  SUM(QUANTITY) AS "Net Units",
  SUM(COSTBASIS) AS "Net Cost Basis",
  SUM(dmd_r) AS "DMD Revenue",
  SUM(dmd_u) AS "DMD Units",
  SUM(dmd_c) AS "DMD Cost Basis",
  COUNT(DISTINCT "Demand Transaction") AS "DMD Transactions"
FROM
  JCLAGData
GROUP BY ALL;