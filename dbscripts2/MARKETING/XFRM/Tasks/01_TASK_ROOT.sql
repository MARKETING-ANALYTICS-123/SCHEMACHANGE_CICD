CREATE OR REPLACE TASK "01_TASK_ROOT"
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
  -- Root task: simple insert or refresh statement
  INSERT INTO XFRM.MY_TABLE_ROOT (id, val) VALUES (1, 'root');
