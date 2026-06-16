
-- 06_gold_dynamic_tables.sql
-- 5 auto-refreshing gold analytics tables
-- TARGET_LAG = '1 hour' means Snowflake refreshes every hour


USE DATABASE ecommerce_db;
USE SCHEMA gold;
USE WAREHOUSE etl_wh;

-- ── 1. Daily revenue 
CREATE OR REPLACE DYNAMIC TABLE gold.rpt_daily_revenue
    TARGET_LAG = '1 hour'
    WAREHOUSE  = etl_wh
AS
SELECT
    DATE_TRUNC('day', o.purchase_ts)       AS order_date,
    COUNT(DISTINCT o.order_id)             AS total_orders,
    COUNT(DISTINCT o.customer_id)          AS unique_customers,
    ROUND(SUM(i.total_value), 2)           AS gross_revenue,
    ROUND(AVG(i.total_value), 2)           AS aov,
    ROUND(SUM(CASE WHEN o.is_late THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0) * 100, 2)  AS late_delivery_pct
FROM silver.fact_orders o
JOIN silver.fact_order_items i USING (order_id)
WHERE o.order_status = 'delivered'
GROUP BY 1
ORDER BY 1;

-- ── 2. Product performance
CREATE OR REPLACE DYNAMIC TABLE gold.rpt_product_performance
    TARGET_LAG = '1 hour'
    WAREHOUSE  = etl_wh
AS
SELECT
    p.product_id,
    p.category_name_en,
    COUNT(DISTINCT i.order_id)     AS total_orders,
    ROUND(SUM(i.price), 2)         AS total_revenue,
    ROUND(AVG(i.price), 2)         AS avg_price,
    ROUND(AVG(r.review_score), 2)  AS avg_review_score,
    COUNT(r.review_id)             AS review_count
FROM silver.dim_products p
JOIN silver.fact_order_items i  USING (product_id)
JOIN silver.fact_orders o       USING (order_id)
LEFT JOIN silver.fact_reviews r USING (order_id)
WHERE o.order_status = 'delivered'
GROUP BY 1, 2
ORDER BY total_revenue DESC;

-- ── 3. Customer segments 
CREATE OR REPLACE DYNAMIC TABLE gold.rpt_customer_segments
    TARGET_LAG = '1 hour'
    WAREHOUSE  = etl_wh
AS
WITH rfm AS (
    SELECT
        o.customer_id,
        c.city,
        c.state,
        COUNT(DISTINCT o.order_id)                           AS frequency,
        ROUND(SUM(i.total_value), 2)                         AS monetary,
        DATEDIFF('day', MAX(o.purchase_ts), CURRENT_DATE())  AS recency_days
    FROM silver.fact_orders o
    JOIN silver.fact_order_items i USING (order_id)
    JOIN silver.dim_customers c    USING (customer_id)
    WHERE c.is_current = TRUE
    GROUP BY 1, 2, 3
)
SELECT *,
    CASE
        WHEN recency_days <= 90  AND monetary >= 500 THEN 'Champions'
        WHEN recency_days <= 180 AND frequency >= 2  THEN 'Loyal'
        WHEN recency_days <= 90                      THEN 'Recent'
        WHEN recency_days > 365                      THEN 'Churned'
        ELSE 'At Risk'
    END AS segment
FROM rfm;

-- ── 4. Payment analysis
CREATE OR REPLACE DYNAMIC TABLE gold.rpt_payment_analysis
    TARGET_LAG = '1 hour'
    WAREHOUSE  = etl_wh
AS
SELECT
    payment_type,
    COUNT(DISTINCT order_id)             AS order_count,
    ROUND(SUM(payment_value), 2)         AS total_value,
    ROUND(AVG(payment_installments), 1)  AS avg_installments,
    ROUND(AVG(payment_value), 2)         AS avg_payment
FROM silver.fact_payments
GROUP BY 1
ORDER BY total_value DESC;

-- ── 5. Seller performance
CREATE OR REPLACE DYNAMIC TABLE gold.rpt_seller_performance
    TARGET_LAG = '1 hour'
    WAREHOUSE  = etl_wh
AS
SELECT
    i.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(DISTINCT i.order_id)                         AS orders_fulfilled,
    ROUND(SUM(i.total_value), 2)                       AS total_revenue,
    ROUND(AVG(r.review_score), 2)                      AS avg_review,
    SUM(CASE WHEN o.is_late THEN 1 ELSE 0 END)         AS late_deliveries
FROM silver.fact_order_items i
JOIN silver.fact_orders o        USING (order_id)
JOIN ecommerce_db.bronze.raw_sellers s ON s.seller_id = i.seller_id
LEFT JOIN silver.fact_reviews r  USING (order_id)
GROUP BY 1, 2, 3
ORDER BY total_revenue DESC;

-- ── Verify all 5 gold tables 
SELECT 'rpt_daily_revenue'       AS table_name, COUNT(*) FROM gold.rpt_daily_revenue       UNION ALL
SELECT 'rpt_product_performance',               COUNT(*) FROM gold.rpt_product_performance  UNION ALL
SELECT 'rpt_customer_segments',                 COUNT(*) FROM gold.rpt_customer_segments    UNION ALL
SELECT 'rpt_payment_analysis',                  COUNT(*) FROM gold.rpt_payment_analysis     UNION ALL
SELECT 'rpt_seller_performance',                COUNT(*) FROM gold.rpt_seller_performance;
