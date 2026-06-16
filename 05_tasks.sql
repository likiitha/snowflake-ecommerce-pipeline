
-- 05_tasks.sql
-- Task DAG — runs all silver stored procs daily at 2 AM UTC


USE DATABASE ecommerce_db;
USE SCHEMA orchestration;
USE WAREHOUSE etl_wh;

CREATE OR REPLACE TASK orchestration.root_task
    WAREHOUSE = etl_wh
    SCHEDULE  = 'USING CRON 0 2 * * * UTC'
AS SELECT 1;

CREATE OR REPLACE TASK orchestration.t_fact_orders
    WAREHOUSE = etl_wh
    AFTER     orchestration.root_task
AS CALL silver.sp_build_fact_orders();

CREATE OR REPLACE TASK orchestration.t_fact_items
    WAREHOUSE = etl_wh
    AFTER     orchestration.root_task
AS CALL silver.sp_build_fact_order_items();

CREATE OR REPLACE TASK orchestration.t_fact_payments
    WAREHOUSE = etl_wh
    AFTER     orchestration.root_task
AS CALL silver.sp_build_fact_payments();

CREATE OR REPLACE TASK orchestration.t_fact_reviews
    WAREHOUSE = etl_wh
    AFTER     orchestration.root_task
AS CALL silver.sp_build_fact_reviews();

CREATE OR REPLACE TASK orchestration.t_dim_customers
    WAREHOUSE = etl_wh
    AFTER     orchestration.root_task
AS CALL silver.sp_build_dim_customers();

CREATE OR REPLACE TASK orchestration.t_dim_products
    WAREHOUSE = etl_wh
    AFTER     orchestration.root_task
AS CALL silver.sp_build_dim_products();

-- Resume in reverse dependency order
ALTER TASK orchestration.t_dim_products  RESUME;
ALTER TASK orchestration.t_fact_reviews  RESUME;
ALTER TASK orchestration.t_fact_payments RESUME;
ALTER TASK orchestration.t_dim_customers RESUME;
ALTER TASK orchestration.t_fact_items    RESUME;
ALTER TASK orchestration.t_fact_orders   RESUME;
ALTER TASK orchestration.root_task       RESUME;

-- Check task status
SELECT name, state, scheduled_time, completed_time, error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
ORDER BY scheduled_time DESC;
