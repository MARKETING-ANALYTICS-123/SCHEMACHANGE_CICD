create or replace task TASK_PASSPORT_MARKETING_NRF
	warehouse=MARKETING_PRD_XS_WH
	after EDW_MARKETING_PRD.XFRM.TASK_PASSPORT_MARKETING
	as DECLARE
        V_QUERY_TIME TIMESTAMP;
        V_LAST_MODIFIED_TIME TIMESTAMP;
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
            LIMIT
                1
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
                AND TABLE_NAME = 'PASSPORT MARKETING NRF'
        );

        -- If last modified time of the table is less than Acquia refresh time, then call the procedure to update the table
       IF(V_QUERY_TIME > V_LAST_MODIFIED_TIME AND V_QUERY_STATUS = 'SUCCESS') THEN
           CALL XFRM.USP_POPULATE_PASSPORT_MARKETING_NRF();
       END IF;
    END;