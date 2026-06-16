
-- 03_silver_tables.sql
-- Creates all silver target tables (typed, cleaned schema)
-- Run before 04_silver_transforms.sql


USE DATABASE ecommerce_db;
USE SCHEMA silver;
USE WAREHOUSE etl_wh;

CREATE OR REPLACE TABLE silver.fact_orders (
    order_id                    VARCHAR PRIMARY KEY,
    customer_id                 VARCHAR,
    order_status                VARCHAR,
    purchase_ts                 TIMESTAMP_NTZ,
    approved_ts                 TIMESTAMP_NTZ,
    delivered_ts                TIMESTAMP_NTZ,
    estimated_delivery_ts       TIMESTAMP_NTZ,
    delivery_delay_days         FLOAT,
    is_late                     BOOLEAN,
    dw_load_ts                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE silver.fact_order_items (
    order_id        VARCHAR,
    order_item_id   INTEGER,
    product_id      VARCHAR,
    seller_id       VARCHAR,
    price           FLOAT,
    freight_value   FLOAT,
    total_value     FLOAT,
    dw_load_ts      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE silver.fact_payments (
    order_id             VARCHAR,
    payment_sequential   INTEGER,
    payment_type         VARCHAR,
    payment_installments INTEGER,
    payment_value        FLOAT,
    dw_load_ts           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE silver.fact_reviews (
    review_id          VARCHAR PRIMARY KEY,
    order_id           VARCHAR,
    review_score       INTEGER,
    has_comment        BOOLEAN,
    review_creation_ts TIMESTAMP_NTZ,
    dw_load_ts         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE silver.dim_customers (
    customer_id          VARCHAR,
    customer_unique_id   VARCHAR,
    zip_code_prefix      VARCHAR,
    city                 VARCHAR,
    state                VARCHAR,
    is_current           BOOLEAN DEFAULT TRUE,
    dw_start_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    dw_end_ts            TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE silver.dim_products (
    product_id       VARCHAR PRIMARY KEY,
    category_name_pt VARCHAR,
    category_name_en VARCHAR,
    weight_g         FLOAT,
    length_cm        FLOAT,
    height_cm        FLOAT,
    width_cm         FLOAT,
    volume_cm3       FLOAT,
    dw_load_ts       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
