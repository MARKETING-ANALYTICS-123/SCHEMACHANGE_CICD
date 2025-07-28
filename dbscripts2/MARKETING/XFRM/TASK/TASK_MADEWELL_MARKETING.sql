create or replace task TASK_MADEWELL_MARKETING
	warehouse=MARKETING_PRD_XS_WH
	schedule='USING CRON */15 9-22 * * * America/New_York'
	as DECLARE
        V_QUERY_TIME TIMESTAMP;
        V_LAST_MODIFIED_TIME TIMESTAMP;
        V_TABLE_NAME STRING := 'MADEWELL MARKETING';
        V_QUERY_STATUS VARCHAR(16672673);
    BEGIN
      V_QUERY_TIME := (
        SELECT
            CAST(value AS TIMESTAMP)
        FROM
            CDP.PUBLIC.A1_STATUS
        WHERE
            category = 'pipeline'
            AND source = 'bi'
            AND key = 'end_time'
            AND CURRENT_TIMESTAMP > value
            AND value NOT IN ('0')
        LIMIT
            1
    );
    
    V_QUERY_STATUS := (
        SELECT
            CAST(VALUE AS STRING) AS STATUS
        FROM
            CDP.PUBLIC.A1_STATUS
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
            AND TABLE_NAME = 'MADEWELL MARKETING'
    );
        
        -- If last modified time of the table is less than Acquia refresh time, then call the procedure to update the table
    IF(V_QUERY_TIME > V_LAST_MODIFIED_TIME  AND V_QUERY_STATUS = 'SUCCESS') THEN
        CALL XFRM.USP_POPULATE_PRODUCT_CATEGORY_SUMMARY();
        CALL XFRM.USP_POPULATE_PRODUCT_SUMMARY();
        CALL XFRM.USP_POPULATE_ORGANIZATION_SUMMARY();
        CALL XFRM.USP_POPULATE_GA_CHANNEL();
        CALL XFRM.USP_POPULATE_PROMO_DIMENSION();
        CALL XFRM.USP_POPULATE_MADEWELL_CUSTOMER();
        CALL XFRM.USP_POPULATE_MADEWELL_MARKETING();
    END IF;
END;