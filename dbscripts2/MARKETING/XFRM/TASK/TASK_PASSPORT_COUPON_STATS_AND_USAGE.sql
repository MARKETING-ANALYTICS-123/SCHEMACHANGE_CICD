create or replace task TASK_PASSPORT_COUPON_STATS_AND_USAGE
	warehouse=MARKETING_PRD_S_WH
	after EDW_MARKETING_PRD.XFRM.TASK_PASSPORT_LOYALTY_TIER
	as DECLARE 
    V_QUERY_TIME TIMESTAMP;
    V_LAST_MODIFIED_TIME TIMESTAMP;
    V_LOYALTY_SNAPSHOT_COUNT INT;
    V_LOYALTY_CUSTOMER INT;
    V_SNAPSHOT_DATE TIMESTAMP;
    V_QUERY_STATUS VARCHAR(16672673);

BEGIN 
  V_QUERY_TIME := (
    SELECT 
      CAST(value AS TIMESTAMP)
    FROM 
      CDP_JC.PUBLIC.A1_STATUS 
    WHERE 
      category = 'pipeline' 
      AND source = 'bi' 
      AND key = 'end_time' 
      AND CURRENT_TIMESTAMP > value 
      AND value NOT IN ('0') 
    LIMIT 1
  );


  V_QUERY_STATUS := (
    SELECT
        CAST(VALUE AS STRING) AS STATUS
    FROM
        CDP_JC.PUBLIC.A1_STATUS
    WHERE
        category = 'pipeline'
        AND source = 'bi'
        AND KEY = 'status'
    LIMIT
        1
);

  -- Get the table's last modified time by calling stored procedure
  V_LAST_MODIFIED_TIME := (
    SELECT
        CAST(MAX(last_altered) AS TIMESTAMP)
    FROM
        INFORMATION_SCHEMA.TABLES
    WHERE
        TABLE_CATALOG = 'EDW_MARKETING_PRD'
        AND TABLE_SCHEMA = 'RPT'
        AND TABLE_NAME = 'PASSPORT COUPON STATS AND USAGE'
  );

  V_SNAPSHOT_DATE := (
    SELECT 
      DATE(TS.DATEINUNIX) 
    FROM 
      CDP_JC.PUBLIC.TIMESUMMARY TS 
      INNER JOIN cdp_jc.PUBLIC.timesummary t ON DATE(TS.dateinunix) = DATE(t.dateinunix) + 1 
    WHERE 
      TS.FISCALYEAR = (
        SELECT DISTINCT FISCALYEAR 
        FROM CDP_JC.PUBLIC.TIMESUMMARY 
        WHERE DATEINUNIX::DATE = CURRENT_DATE::DATE
      ) 
      AND TS.FISCALWEEK = (
        SELECT DISTINCT FISCALWEEK 
        FROM CDP_JC.PUBLIC.TIMESUMMARY 
        WHERE DATEINUNIX::DATE = CURRENT_DATE::DATE
      ) 
      AND TS.FISCALDAYOFWEEK = 1
  );

  V_LOYALTY_SNAPSHOT_COUNT := (
    SELECT 
      COUNT(1) 
    FROM 
      loyalty.jc_prd.prd_jc_customers_snapshot 
    WHERE 
      SNAPSHOT_DATE::DATE = :V_SNAPSHOT_DATE::DATE
  );

  V_LOYALTY_CUSTOMER := (
    SELECT 
      COUNT(DISTINCT mastercustomerid) 
    FROM 
      cdp_jc.public.customersummary cs 
    WHERE 
      c_loyaltysignupdate IS NOT NULL
  ); 

  -- If last modified time of the table is less than Acquia refresh time, then call the procedure to update the table
  IF ( V_QUERY_TIME > V_LAST_MODIFIED_TIME AND V_LOYALTY_SNAPSHOT_COUNT > V_LOYALTY_CUSTOMER AND V_QUERY_STATUS = 'SUCCESS') 
  THEN 
    CALL XFRM.USP_POPULATE_PASSPORT_COUPON_STATS_AND_USAGE();
    CALL XFRM.USP_POPULATE_COUPON_VALIDATION_KPI();
  END IF;

END;