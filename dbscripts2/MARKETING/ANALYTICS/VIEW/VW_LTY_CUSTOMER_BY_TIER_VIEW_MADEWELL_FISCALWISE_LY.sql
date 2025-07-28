create or replace view VW_LTY_CUSTOMER_BY_TIER_VIEW_MADEWELL_FISCALWISE_LY(
	"Date Key",
	"Brand Name",
	"MasterCustomer ID",
	"MasterCustomer Transaction Sequence",
	"Channel Flag",
	"Loyalty Tier",
	"Loyalty Flag",
	"Net Revenue",
	"Net Units",
	"Net Cost Basis",
	"DMD Revenue",
	"DMD Units",
	"DMD Cost Basis",
	"DMD Transactions",
	"Fiscal Status"
) as

WITH MW_CUSTOMERS_SNAPSHOT_2023 AS (
    SELECT DISTINCT
    ts.mastercustomerid,
    ss.external_customer_id,
    ss.snapshot_date,
    ss.last_activity,
    ss.top_tier_name,
    balance
  FROM
    loyalty.mw_prd.prd_mw_customers_snapshot ss
    INNER JOIN cdp.PUBLIC.timesummary t ON DATE (ss.snapshot_date) = DATE(t.dateinunix) + 1
    INNER JOIN cdp.PUBLIC.customer c ON c.c_loyaltyid = ss.external_customer_id
    INNER JOIN cdp.PUBLIC.mastercustomer mc ON mc.customerid = c.id
    INNER JOIN cdp.PUBLIC.transactionsummary ts ON ts.mastercustomerid=mc.mastercustomerid
  WHERE
    ts.mastercustomerid<>'-1'
    AND mc.customerid LIKE 'LOY_%'
    AND t.fiscaldayofweek = 7
  ),
  mw_cust_loyalty_unique_toptier AS (
    SELECT
      mastercustomerid,
      external_customer_id,
      snapshot_date,
      top_tier_name,
      last_activity,
      balance,
      ROW_NUMBER() OVER (
        PARTITION BY
          mastercustomerid, snapshot_date
        ORDER BY
          last_activity desc nulls last, balance desc 
      ) AS rn
    FROM
      MW_CUSTOMERS_SNAPSHOT_2023
  ),
  mw_final_unique_loyalty AS (
    SELECT
      mastercustomerid,
      external_customer_id,
      snapshot_date,
      top_tier_name,
      last_activity
    FROM
      mw_cust_loyalty_unique_toptier
    WHERE
      rn = 1
  ),
  ----------------------------------------------------------- MADEWELL 2023 LOGIC ------------------------------------------------------------------------
  
  MWLAGDATA AS (
    SELECT
      COALESCE(
        CASE WHEN csu.top_tier_name IS NULL AND csu.external_customer_id IS NOT NULL THEN 'Insider' 
        ELSE csu.top_tier_name END,
        CASE WHEN LOWER(R.Tier) LIKE '%insider%' THEN 'Insider'
        WHEN LOWER(R.Tier) LIKE '%star%' THEN 'Star'
        WHEN LOWER(R.Tier) LIKE '%icon+%' THEN 'Icon+'
        WHEN LOWER(R.Tier) LIKE '%icon%' THEN 'Icon'
        END,
        CASE WHEN LOWER(ts.c_loyaltytier) LIKE '%insider%' THEN 'Insider' 
        WHEN LOWER(ts.c_loyaltytier) LIKE '%star%' THEN 'Star' 
        WHEN LOWER(ts.c_loyaltytier) LIKE '%vip%' THEN 'Icon+'
        WHEN LOWER(ts.c_loyaltytier) LIKE '%icon+%' THEN 'Icon+'  
        WHEN LOWER(ts.c_loyaltytier) LIKE '%icon%' THEN 'Icon' 
        WHEN LOWER(ts.c_loyaltytier) IS NULL AND date (ts.transactiondate) >= date (c.c_loyaltysignupdate) THEN 'Insider' ELSE 'Non-Loyalty' END
	   ) AS "Loyalty Tier",
      TS.TRANSACTIONTIMESTAMPFK AS "Date Key",
      TS.mastercustomerid AS "MasterCustomer ID",
      TS.mastercustomertransactionsequence AS "MasterCustomer Transaction Sequence"
    FROM
      cdp.PUBLIC.transactionsummary ts
      LEFT JOIN CDP.PUBLIC.CUSTOMERSUMMARY C ON TS.MASTERCUSTOMERID = C.MASTERCUSTOMERID
	  LEFT JOIN EDW_MARKETING.ANALYTICS.VW_MKT_TIMESUMMARY MT ON MT."Last Year Id" = ts.TRANSACTIONTIMESTAMPFK
      LEFT OUTER JOIN cdp.PUBLIC.timesummary t ON t.ID = MT.ID
      LEFT OUTER JOIN mw_final_unique_loyalty csu ON ts.mastercustomerid = csu.mastercustomerid
          AND DATE (TRANSACTIONDATE) < DATE (csu.snapshot_date)
          AND DATE (TRANSACTIONDATE) >= DATEADD (WEEK, -1, DATE (csu.snapshot_date))
      LEFT JOIN cdp.PUBLIC.organizationsummary O ON O.id = Ts.organizationid
      LEFT JOIN (
        SELECT DISTINCT
          TIER,
          TRANSACTIONID,
          Brand
        FROM
          LOYALTY.JC_PRD.T_LOY_TRANS_REWARDS
        WHERE
          REWARDS IS NOT NULL
      ) R ON R.TRANSACTIONID=TS.TRANSACTIONID
      AND TS.C_BRAND = LEFT (R.BRAND, 2)
    WHERE
      ts.mastercustomerid <> '-1'
      AND TO_DATE(t.DATEINUNIX) >= (SELECT TO_DATE(DATEINUNIX) FROM CDP.PUBLIC.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7) - 1 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
      AND TO_DATE(t.DATEINUNIX) <= (
        SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
        WHERE TS.DATEINUNIX::DATE <= current_date::DATE
        AND TS.FISCALDAYOFWEEK = 7
      )

  ),
  ----------------------------------------------------------- MADEWELL 2022 LOGIC ------------------------------------------------------------------------

FINAL_TABLE AS
(
SELECT
  DISTINCT
  "Loyalty Tier",
  CASE
    WHEN "Loyalty Tier" IS NOT NULL
    AND "Loyalty Tier" <> 'Non-Loyalty' THEN 'Loyalty'
    ELSE 'Non-Loyalty'
  END AS "Loyalty Flag",
  "Date Key",
  "MasterCustomer ID",
  "MasterCustomer Transaction Sequence"
FROM
  MWLAGData
),

GET_SUMMARIZED_DATA_WEEK AS (
    SELECT 
        FISCALYEAR, 
        FISCALWEEK,
        CASE
          WHEN ts.c_devicetype='DROP' THEN 'Wholesale'
          WHEN ts.organizationid='LBDM'
          AND ts.c_devicetype<>'DROP' THEN 'Direct'
          ELSE 'Retail'
        END AS "Channel Flag",
        MASTERCUSTOMERID AS "MasterCustomer ID",
        MAX(TS.TRANSACTIONTIMESTAMPFK) AS "Date Key",
        SUM(SALEREVENUE) AS "Net Revenue",
        SUM(QUANTITY) AS "Net Units",
        SUM(COSTBASIS) AS "Net Cost Basis",
        SUM(CASE WHEN SUBTYPE IN ('Demand', 'Shipped') THEN SALEREVENUE END) AS "DMD Revenue",
        SUM(CASE WHEN SUBTYPE IN ('Demand', 'Shipped') THEN QUANTITY END) AS "DMD Units",
        SUM(CASE WHEN SUBTYPE IN ('Demand', 'Shipped') THEN COSTBASIS END) AS "DMD Cost Basis",
        COUNT(DISTINCT CASE WHEN SUBTYPE IN ('Demand', 'Shipped') THEN TRANSACTIONID END) AS "DMD Transactions"
        FROM CDP.PUBLIC.TRANSACTIONSUMMARY TS
        LEFT JOIN EDW_MARKETING.ANALYTICS.VW_MKT_TIMESUMMARY MT ON MT."Last Year Id" = ts.TRANSACTIONTIMESTAMPFK
		LEFT OUTER JOIN cdp.PUBLIC.timesummary t ON t.ID = MT.ID
        WHERE MASTERCUSTOMERID <> '-1'
        AND TO_DATE(t.DATEINUNIX) >= (SELECT TO_DATE(DATEINUNIX) FROM CDP.PUBLIC.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7) - 1 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
      AND TO_DATE(t.DATEINUNIX) <= (
        SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
        WHERE TS.DATEINUNIX::DATE <= current_date::DATE
        AND TS.FISCALDAYOFWEEK = 7
      )
    GROUP BY ALL
),

GET_LOYALTY_TIER_WEEK AS (
    SELECT
        DISTINCT
        FISCALYEAR,
        FISCALWEEK,
        GT."MasterCustomer ID",
		GT."MasterCustomer Transaction Sequence",
        "Loyalty Flag",
        "Loyalty Tier"
    FROM 
        (
          SELECT FISCALYEAR, FISCALWEEK,
            MASTERCUSTOMERID AS "MasterCustomer ID",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence",
            MAX(TT.TRANSACTIONTIMESTAMPFK) AS "Date Key"
            FROM
            CDP.PUBLIC.TRANSACTIONSUMMARY TT
			LEFT JOIN EDW_MARKETING.ANALYTICS.VW_MKT_TIMESUMMARY MT ON MT."Last Year Id" = TT.TRANSACTIONTIMESTAMPFK
			LEFT OUTER JOIN cdp.PUBLIC.timesummary t ON t.ID = MT.ID
            WHERE MASTERCUSTOMERID <> '-1'
            AND TO_DATE(t.DATEINUNIX) >= (SELECT TO_DATE(DATEINUNIX) FROM CDP.PUBLIC.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7) - 1 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
			AND TO_DATE(t.DATEINUNIX) <= (
				SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
				WHERE TS.DATEINUNIX::DATE <= current_date::DATE
				AND TS.FISCALDAYOFWEEK = 7
			)
        GROUP BY ALL
        ) AS GT
    LEFT JOIN FINAL_TABLE TS ON GT."Date Key" = TS."Date Key" AND GT."MasterCustomer Transaction Sequence" = TS."MasterCustomer Transaction Sequence" AND GT."MasterCustomer ID" = TS."MasterCustomer ID"
),

GET_SUMMARIZED_DATA_MONTH AS (
    SELECT 
        FISCALYEAR, 
        FISCALMONTH,
        CASE
          WHEN ts.c_devicetype='DROP' THEN 'Wholesale'
          WHEN ts.organizationid='LBDM'
          AND ts.c_devicetype<>'DROP' THEN 'Direct'
          ELSE 'Retail'
        END AS "Channel Flag",
        MASTERCUSTOMERID AS "MasterCustomer ID",
        MAX(TS.TRANSACTIONTIMESTAMPFK) AS "Date Key",
        SUM(SALEREVENUE) AS "Net Revenue",
        SUM(QUANTITY) AS "Net Units",
        SUM(COSTBASIS) AS "Net Cost Basis",
        SUM(CASE WHEN SUBTYPE IN ('Demand', 'Shipped') THEN SALEREVENUE END) AS "DMD Revenue",
        SUM(CASE WHEN SUBTYPE IN ('Demand', 'Shipped') THEN QUANTITY END) AS "DMD Units",
        SUM(CASE WHEN SUBTYPE IN ('Demand', 'Shipped') THEN COSTBASIS END) AS "DMD Cost Basis",
        COUNT(DISTINCT CASE WHEN SUBTYPE IN ('Demand', 'Shipped') THEN TRANSACTIONID END) AS "DMD Transactions"
        FROM CDP.PUBLIC.TRANSACTIONSUMMARY TS
        LEFT JOIN EDW_MARKETING.ANALYTICS.VW_MKT_TIMESUMMARY MT ON MT."Last Year Id" = ts.TRANSACTIONTIMESTAMPFK
		LEFT OUTER JOIN cdp.PUBLIC.timesummary t ON t.ID = MT.ID
        WHERE MASTERCUSTOMERID <> '-1'
        AND TO_DATE(t.DATEINUNIX) >= (SELECT TO_DATE(DATEINUNIX) FROM CDP.PUBLIC.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7) - 1 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
      AND TO_DATE(t.DATEINUNIX) <= (
        SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
        WHERE TS.DATEINUNIX::DATE <= current_date::DATE
        AND TS.FISCALDAYOFWEEK = 7
      )
    GROUP BY ALL
),

GET_LOYALTY_TIER_MONTH AS (
    SELECT
        DISTINCT
        FISCALYEAR,
        FISCALMONTH,
        GT."MasterCustomer ID",
		GT."MasterCustomer Transaction Sequence",
        "Loyalty Flag",
        "Loyalty Tier"
    FROM 
        ( 
            SELECT 
			FISCALYEAR, 
			FISCALMONTH,
            MASTERCUSTOMERID AS "MasterCustomer ID",
            MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence",
            MAX(TT.TRANSACTIONTIMESTAMPFK) AS "Date Key"
            FROM
            CDP.PUBLIC.TRANSACTIONSUMMARY TT
            LEFT JOIN EDW_MARKETING.ANALYTICS.VW_MKT_TIMESUMMARY MT ON MT."Last Year Id" = TT.TRANSACTIONTIMESTAMPFK
			LEFT OUTER JOIN cdp.PUBLIC.timesummary t ON t.ID = MT.ID
            WHERE MASTERCUSTOMERID <> '-1'
            AND TO_DATE(t.DATEINUNIX) >= (SELECT TO_DATE(DATEINUNIX) FROM CDP.PUBLIC.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7) - 1 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
      AND TO_DATE(t.DATEINUNIX) <= (
        SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
        WHERE TS.DATEINUNIX::DATE <= current_date::DATE
        AND TS.FISCALDAYOFWEEK = 7
      )
            GROUP BY ALL
        ) AS GT
    LEFT JOIN FINAL_TABLE TS ON GT."Date Key" = TS."Date Key" AND GT."MasterCustomer Transaction Sequence" = TS."MasterCustomer Transaction Sequence" AND GT."MasterCustomer ID" = TS."MasterCustomer ID"
),

GET_SUMMARIZED_DATA_QUARTER AS (
    SELECT 
        FISCALYEAR, 
        FISCALQUARTER,
        CASE
          WHEN ts.c_devicetype='DROP' THEN 'Wholesale'
          WHEN ts.organizationid='LBDM'
          AND ts.c_devicetype<>'DROP' THEN 'Direct'
          ELSE 'Retail'
        END AS "Channel Flag",
        MASTERCUSTOMERID AS "MasterCustomer ID",
        MAX(TS.TRANSACTIONTIMESTAMPFK) AS "Date Key",
        SUM(SALEREVENUE) AS "Net Revenue",
        SUM(QUANTITY) AS "Net Units",
        SUM(COSTBASIS) AS "Net Cost Basis",
        SUM(CASE WHEN SUBTYPE IN ('Demand', 'Shipped') THEN SALEREVENUE END) AS "DMD Revenue",
        SUM(CASE WHEN SUBTYPE IN ('Demand', 'Shipped') THEN QUANTITY END) AS "DMD Units",
        SUM(CASE WHEN SUBTYPE IN ('Demand', 'Shipped') THEN COSTBASIS END) AS "DMD Cost Basis",
        COUNT(DISTINCT CASE WHEN SUBTYPE IN ('Demand', 'Shipped') THEN TRANSACTIONID END) AS "DMD Transactions"
        FROM CDP.PUBLIC.TRANSACTIONSUMMARY TS
        LEFT JOIN EDW_MARKETING.ANALYTICS.VW_MKT_TIMESUMMARY MT ON MT."Last Year Id" = ts.TRANSACTIONTIMESTAMPFK
		LEFT OUTER JOIN cdp.PUBLIC.timesummary t ON t.ID = MT.ID
        WHERE MASTERCUSTOMERID <> '-1'
        AND TO_DATE(t.DATEINUNIX) >= (SELECT TO_DATE(DATEINUNIX) FROM CDP.PUBLIC.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7) - 1 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
      AND TO_DATE(t.DATEINUNIX) <= (
        SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
        WHERE TS.DATEINUNIX::DATE <= current_date::DATE
        AND TS.FISCALDAYOFWEEK = 7
      )
    GROUP BY ALL
),

GET_LOYALTY_TIER_QUARTER AS (
    SELECT
        DISTINCT
        FISCALYEAR,
        FISCALQUARTER,
        GT."MasterCustomer ID",
		GT."MasterCustomer Transaction Sequence",
        "Loyalty Flag",
        "Loyalty Tier"
    FROM 
        (SELECT 
        FISCALYEAR,
        FISCALQUARTER,
        MASTERCUSTOMERID AS "MasterCustomer ID",
        MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence",
        MAX(TT.TRANSACTIONTIMESTAMPFK) AS "Date Key"
        FROM
        CDP.PUBLIC.TRANSACTIONSUMMARY TT
        LEFT JOIN EDW_MARKETING.ANALYTICS.VW_MKT_TIMESUMMARY MT ON MT."Last Year Id" = TT.TRANSACTIONTIMESTAMPFK
		LEFT OUTER JOIN cdp.PUBLIC.timesummary t ON t.ID = MT.ID
        WHERE MASTERCUSTOMERID <> '-1'
        AND TO_DATE(t.DATEINUNIX) >= (SELECT TO_DATE(DATEINUNIX) FROM CDP.PUBLIC.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7) - 1 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
		  AND TO_DATE(t.DATEINUNIX) <= (
			SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
			WHERE TS.DATEINUNIX::DATE <= current_date::DATE
			AND TS.FISCALDAYOFWEEK = 7
		  )
        GROUP BY ALL) AS GT
    LEFT JOIN FINAL_TABLE TS ON GT."Date Key" = TS."Date Key" AND GT."MasterCustomer Transaction Sequence" = TS."MasterCustomer Transaction Sequence" AND GT."MasterCustomer ID" = TS."MasterCustomer ID"
),

GET_SUMMARIZED_DATA_YEAR AS (
    SELECT 
        FISCALYEAR,
        CASE
          WHEN ts.c_devicetype='DROP' THEN 'Wholesale'
          WHEN ts.organizationid='LBDM'
          AND ts.c_devicetype<>'DROP' THEN 'Direct'
          ELSE 'Retail'
        END AS "Channel Flag",
        MASTERCUSTOMERID AS "MasterCustomer ID",
        MAX(TS.TRANSACTIONTIMESTAMPFK) AS "Date Key",
        SUM(SALEREVENUE) AS "Net Revenue",
        SUM(QUANTITY) AS "Net Units",
        SUM(COSTBASIS) AS "Net Cost Basis",
        SUM(CASE WHEN SUBTYPE IN ('Demand', 'Shipped') THEN SALEREVENUE END) AS "DMD Revenue",
        SUM(CASE WHEN SUBTYPE IN ('Demand', 'Shipped') THEN QUANTITY END) AS "DMD Units",
        SUM(CASE WHEN SUBTYPE IN ('Demand', 'Shipped') THEN COSTBASIS END) AS "DMD Cost Basis",
        COUNT(DISTINCT CASE WHEN SUBTYPE IN ('Demand', 'Shipped') THEN TRANSACTIONID END) AS "DMD Transactions"
        FROM CDP.PUBLIC.TRANSACTIONSUMMARY TS
        LEFT JOIN EDW_MARKETING.ANALYTICS.VW_MKT_TIMESUMMARY MT ON MT."Last Year Id" = ts.TRANSACTIONTIMESTAMPFK
		LEFT OUTER JOIN cdp.PUBLIC.timesummary t ON t.ID = MT.ID
        WHERE MASTERCUSTOMERID <> '-1'
        AND TO_DATE(t.DATEINUNIX) >= (SELECT TO_DATE(DATEINUNIX) FROM CDP.PUBLIC.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7) - 1 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
		AND TO_DATE(t.DATEINUNIX) <= (
			SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
			WHERE TS.DATEINUNIX::DATE <= current_date::DATE
			AND TS.FISCALDAYOFWEEK = 7
		  )
    GROUP BY ALL
),

GET_LOYALTY_TIER_YEAR AS (
    SELECT
        DISTINCT
        FISCALYEAR,
        GT."MasterCustomer ID",
		GT."MasterCustomer Transaction Sequence",
        "Loyalty Flag",
        "Loyalty Tier"
    FROM 
        (SELECT 
        FISCALYEAR,
        MASTERCUSTOMERID AS "MasterCustomer ID",
        MAX(MASTERCUSTOMERTRANSACTIONSEQUENCE) AS "MasterCustomer Transaction Sequence",
        MAX(TT.TRANSACTIONTIMESTAMPFK) AS "Date Key"
        FROM
        CDP.PUBLIC.TRANSACTIONSUMMARY TT
        LEFT JOIN EDW_MARKETING.ANALYTICS.VW_MKT_TIMESUMMARY MT ON MT."Last Year Id" = tt.TRANSACTIONTIMESTAMPFK
		LEFT OUTER JOIN cdp.PUBLIC.timesummary t ON t.ID = MT.ID
        WHERE MASTERCUSTOMERID <> '-1'
        AND TO_DATE(t.DATEINUNIX) >= (SELECT TO_DATE(DATEINUNIX) FROM CDP.PUBLIC.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7) - 1 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
		AND TO_DATE(t.DATEINUNIX) <= (
			SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
			WHERE TS.DATEINUNIX::DATE <= current_date::DATE
			AND TS.FISCALDAYOFWEEK = 7
		  )
        GROUP BY ALL
        ) AS GT
    LEFT JOIN FINAL_TABLE TS ON GT."Date Key" = TS."Date Key" AND GT."MasterCustomer Transaction Sequence" = TS."MasterCustomer Transaction Sequence" AND GT."MasterCustomer ID" = TS."MasterCustomer ID"
)

SELECT 
	TS."Date Key",
	'Madewell' AS "Brand Name",
	TS."MasterCustomer ID",
	MT."MasterCustomer Transaction Sequence",
	"Channel Flag",
	"Loyalty Tier",
	"Loyalty Flag",
	"Net Revenue",
	"Net Units",
	"Net Cost Basis",
	"DMD Revenue",
	"DMD Units",
	"DMD Cost Basis",
	"DMD Transactions",
	1 AS "Fiscal Status"
FROM GET_SUMMARIZED_DATA_WEEK TS
LEFT JOIN GET_LOYALTY_TIER_WEEK MT
ON MT."MasterCustomer ID" = TS."MasterCustomer ID"
AND MT.FISCALYEAR = TS.FISCALYEAR
AND MT.FISCALWEEK = TS.FISCALWEEK

UNION ALL

SELECT 
	TS."Date Key",
	'Madewell' AS "Brand Name",
	TS."MasterCustomer ID",
	MT."MasterCustomer Transaction Sequence",
	"Channel Flag",
	"Loyalty Tier",
	"Loyalty Flag",
	"Net Revenue",
	"Net Units",
	"Net Cost Basis",
	"DMD Revenue",
	"DMD Units",
	"DMD Cost Basis",
	"DMD Transactions",
	2 AS "Fiscal Status"
FROM GET_SUMMARIZED_DATA_MONTH TS
LEFT JOIN GET_LOYALTY_TIER_MONTH MT
ON MT."MasterCustomer ID" = TS."MasterCustomer ID"
AND MT.FISCALYEAR = TS.FISCALYEAR
AND MT.FISCALMONTH = TS.FISCALMONTH

UNION ALL

SELECT 
	TS."Date Key",
	'Madewell' AS "Brand Name",
	TS."MasterCustomer ID",
	MT."MasterCustomer Transaction Sequence",
	"Channel Flag",
	"Loyalty Tier",
	"Loyalty Flag",
	"Net Revenue",
	"Net Units",
	"Net Cost Basis",
	"DMD Revenue",
	"DMD Units",
	"DMD Cost Basis",
	"DMD Transactions",
	3 AS "Fiscal Status"
FROM GET_SUMMARIZED_DATA_QUARTER TS
LEFT JOIN GET_LOYALTY_TIER_QUARTER MT
ON MT."MasterCustomer ID" = TS."MasterCustomer ID"
AND MT.FISCALYEAR = TS.FISCALYEAR
AND MT.FISCALQUARTER = TS.FISCALQUARTER

UNION ALL

SELECT 
	TS."Date Key",
	'Madewell' AS "Brand Name",
	TS."MasterCustomer ID",
	MT."MasterCustomer Transaction Sequence",
	"Channel Flag",
	"Loyalty Tier",
	"Loyalty Flag",
	"Net Revenue",
	"Net Units",
	"Net Cost Basis",
	"DMD Revenue",
	"DMD Units",
	"DMD Cost Basis",
	"DMD Transactions",
	4 AS "Fiscal Status"
FROM GET_SUMMARIZED_DATA_YEAR TS
LEFT JOIN GET_LOYALTY_TIER_YEAR MT
ON MT."MasterCustomer ID" = TS."MasterCustomer ID"
AND MT.FISCALYEAR = TS.FISCALYEAR;