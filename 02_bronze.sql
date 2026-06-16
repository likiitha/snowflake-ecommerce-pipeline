-- ============================================================
-- 02_bronze.sql
-- Creates 8 raw tables, loads data via COPY INTO, adds Streams
-- Run after 01_setup.sql
-- ============================================================

USE DATABASE ecommerce_db;
USE SCHEMA bronze;
USE WAREHOUSE etl_wh;

-- ── ORDERS ───────────────────────────────────────────────────
CREATE OR REPLACE TABLE bronze.raw_orders (
    order_id                        VARCHAR,
    customer_id                     VARCHAR,
    order_status                    VARCHAR,
    order_purchase_timestamp        VARCHAR,
    order_approved_at               VARCHAR,
    order_delivered_carrier_date    VARCHAR,
    order_delivered_customer_date   VARCHAR,
    order_estimated_delivery_date   VARCHAR,
    _load_ts TIMESTAMP_NTZ          DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO bronze.raw_orders (
    order_id, customer_id, order_status,
    order_purchase_timestamp, order_approved_at,
    order_delivered_carrier_date, order_delivered_customer_date,
    order_estimated_delivery_date
)
FROM @bronze.s3_olist_stage/olist_orders_dataset.csv
FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"')
ON_ERROR = 'CONTINUE';

-- ── ORDER ITEMS ───────────────────────────────────────────────
CREATE OR REPLACE TABLE bronze.raw_order_items (
    order_id            VARCHAR,
    order_item_id       VARCHAR,
    product_id          VARCHAR,
    seller_id           VARCHAR,
    shipping_limit_date VARCHAR,
    price               VARCHAR,
    freight_value       VARCHAR,
    _load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO bronze.raw_order_items (
    order_id, order_item_id, product_id,
    seller_id, shipping_limit_date, price, freight_value
)
FROM @bronze.s3_olist_stage/olist_order_items_dataset.csv
FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"')
ON_ERROR = 'CONTINUE';

-- ── CUSTOMERS ─────────────────────────────────────────────────
CREATE OR REPLACE TABLE bronze.raw_customers (
    customer_id              VARCHAR,
    customer_unique_id       VARCHAR,
    customer_zip_code_prefix VARCHAR,
    customer_city            VARCHAR,
    customer_state           VARCHAR,
    _load_ts TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO bronze.raw_customers (
    customer_id, customer_unique_id, customer_zip_code_prefix,
    customer_city, customer_state
)
FROM @bronze.s3_olist_stage/olist_customers_dataset.csv
FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"')
ON_ERROR = 'CONTINUE';

-- ── PRODUCTS ──────────────────────────────────────────────────
CREATE OR REPLACE TABLE bronze.raw_products (
    product_id                 VARCHAR,
    product_category_name      VARCHAR,
    product_name_lenght        VARCHAR,
    product_description_lenght VARCHAR,
    product_photos_qty         VARCHAR,
    product_weight_g           VARCHAR,
    product_length_cm          VARCHAR,
    product_height_cm          VARCHAR,
    product_width_cm           VARCHAR,
    _load_ts TIMESTAMP_NTZ     DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO bronze.raw_products (
    product_id, product_category_name, product_name_lenght,
    product_description_lenght, product_photos_qty, product_weight_g,
    product_length_cm, product_height_cm, product_width_cm
)
FROM @bronze.s3_olist_stage/olist_products_dataset.csv
FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"')
ON_ERROR = 'CONTINUE';

-- ── PAYMENTS ──────────────────────────────────────────────────
CREATE OR REPLACE TABLE bronze.raw_payments (
    order_id             VARCHAR,
    payment_sequential   VARCHAR,
    payment_type         VARCHAR,
    payment_installments VARCHAR,
    payment_value        VARCHAR,
    _load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO bronze.raw_payments (
    order_id, payment_sequential, payment_type,
    payment_installments, payment_value
)
FROM @bronze.s3_olist_stage/olist_order_payments_dataset.csv
FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"')
ON_ERROR = 'CONTINUE';

-- ── REVIEWS ───────────────────────────────────────────────────
CREATE OR REPLACE TABLE bronze.raw_reviews (
    review_id               VARCHAR,
    order_id                VARCHAR,
    review_score            VARCHAR,
    review_comment_title    VARCHAR,
    review_comment_message  VARCHAR,
    review_creation_date    VARCHAR,
    review_answer_timestamp VARCHAR,
    _load_ts TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO bronze.raw_reviews (
    review_id, order_id, review_score, review_comment_title,
    review_comment_message, review_creation_date, review_answer_timestamp
)
FROM @bronze.s3_olist_stage/olist_order_reviews_dataset.csv
FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"')
ON_ERROR = 'CONTINUE';

-- ── SELLERS ───────────────────────────────────────────────────
CREATE OR REPLACE TABLE bronze.raw_sellers (
    seller_id              VARCHAR,
    seller_zip_code_prefix VARCHAR,
    seller_city            VARCHAR,
    seller_state           VARCHAR,
    _load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO bronze.raw_sellers (
    seller_id, seller_zip_code_prefix, seller_city, seller_state
)
FROM @bronze.s3_olist_stage/olist_sellers_dataset.csv
FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"')
ON_ERROR = 'CONTINUE';

-- ── CATEGORY TRANSLATION ──────────────────────────────────────
CREATE OR REPLACE TABLE bronze.raw_category_translation (
    product_category_name         VARCHAR,
    product_category_name_english VARCHAR,
    _load_ts TIMESTAMP_NTZ        DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO bronze.raw_category_translation (
    product_category_name, product_category_name_english
)
FROM @bronze.s3_olist_stage/product_category_name_translation.csv
FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"')
ON_ERROR = 'CONTINUE';

-- ── STREAMS on every bronze table ────────────────────────────
CREATE OR REPLACE STREAM bronze.stream_orders
    ON TABLE bronze.raw_orders;
CREATE OR REPLACE STREAM bronze.stream_order_items
    ON TABLE bronze.raw_order_items;
CREATE OR REPLACE STREAM bronze.stream_customers
    ON TABLE bronze.raw_customers;
CREATE OR REPLACE STREAM bronze.stream_products
    ON TABLE bronze.raw_products;
CREATE OR REPLACE STREAM bronze.stream_payments
    ON TABLE bronze.raw_payments;
CREATE OR REPLACE STREAM bronze.stream_reviews
    ON TABLE bronze.raw_reviews;

-- ── Row count check ───────────────────────────────────────────
SELECT 'raw_orders'       AS table_name, COUNT(*) AS row_count FROM bronze.raw_orders       UNION ALL
SELECT 'raw_order_items',                COUNT(*) FROM bronze.raw_order_items  UNION ALL
SELECT 'raw_customers',                  COUNT(*) FROM bronze.raw_customers    UNION ALL
SELECT 'raw_products',                   COUNT(*) FROM bronze.raw_products     UNION ALL
SELECT 'raw_payments',                   COUNT(*) FROM bronze.raw_payments     UNION ALL
SELECT 'raw_reviews',                    COUNT(*) FROM bronze.raw_reviews      UNION ALL
SELECT 'raw_sellers',                    COUNT(*) FROM bronze.raw_sellers
ORDER BY row_count DESC;
