CREATE OR REPLACE TASK task_child
  WAREHOUSE = COMPUTE_WH
  AFTER task_mid
AS
  SELECT 'This is the child task.' AS msg;
