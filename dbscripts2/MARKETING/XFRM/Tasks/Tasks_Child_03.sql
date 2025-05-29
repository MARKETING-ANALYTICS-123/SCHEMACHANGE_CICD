CREATE OR REPLACE TASK task_child_03
  WAREHOUSE = COMPUTE_WH
  AFTER task_mid_02
AS
  SELECT 'This is the child tak.' AS msg;
