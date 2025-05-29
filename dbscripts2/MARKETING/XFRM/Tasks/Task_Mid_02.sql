CREATE OR REPLACE TASK task_mid
  WAREHOUSE = COMPUTE_WH
  AFTER task_root_01
AS
  SELECT 'This is the middle task' AS msg;
