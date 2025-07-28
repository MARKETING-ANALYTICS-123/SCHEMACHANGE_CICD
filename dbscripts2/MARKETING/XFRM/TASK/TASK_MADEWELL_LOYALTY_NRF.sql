create or replace task TASK_MADEWELL_LOYALTY_NRF
	warehouse=MARKETING_PRD_XS_WH
	after EDW_MARKETING_PRD.XFRM.TASK_MADEWELL_LOYALTY_TIER
	as DECLARE 
    V_QUERY_TIME TIMESTAMP;
    V_LAST_MODIFIED_TIME TIMESTAMP;
    V_LOYALTY_TIER TIMESTAMP;
    V_QUERY_STATUS VARCHAR(16672673);
BEGIN -- Get the last Acquia refresh time by calling stored procedure
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
        AND TABLE_NAME = 'MADEWELL LOYALTY STATUS NRF'
);

V_LOYALTY_TIER := (
    SELECT
        CAST(MAX(last_altered) AS TIMESTAMP)
    FROM
        INFORMATION_SCHEMA.TABLES
    WHERE
        TABLE_CATALOG = 'EDW_MARKETING_PRD'
        AND TABLE_SCHEMA = 'RPT'
        AND TABLE_NAME = 'MADEWELL LOYALTY TIER'
);


        -- If last modified time of the table is less than Acquia refresh time, then call the procedure to update the table
        IF(V_QUERY_TIME > V_LAST_MODIFIED_TIME AND V_LOYALTY_TIER > V_LAST_MODIFIED_TIME AND V_QUERY_STATUS = 'SUCCESS') THEN
            CALL XFRM.USP_POPULATE_MADEWELL_LOYALTY_NRF();
        END IF;
    END;