CREATE OR REPLACE TASK task_mid
  WAREHOUSE = COMPUTE_WH
  AFTER task_root
AS
  SELECT 'This i the middle task.' AS msg;
