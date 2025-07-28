create or replace task TASK_DIMENSIONS
	warehouse=MARKETING_PRD_XS_WH
	schedule='USING CRON 0 3 * * * America/New_York'
	as BEGIN
    CALL XFRM.USP_POPULATE_DATE();
END;