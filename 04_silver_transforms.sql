
-- 04_silver_transforms.sql
-- Snowpark Python stored procedures registered via Snowflake
-- Paste and run each CREATE PROCEDURE block in Worksheets


USE DATABASE ecommerce_db;
USE SCHEMA silver;
USE WAREHOUSE etl_wh;

CREATE STAGE IF NOT EXISTS silver.proc_stage;

-- ── Proc 1: fact_orders ──────────────────────────────────────
CREATE OR REPLACE PROCEDURE silver.sp_build_fact_orders()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from snowflake.snowpark.functions import col, to_timestamp, trim, lower, lit, when

def run(session):
    src = (
        session.table("ecommerce_db.bronze.stream_orders")
        .select(
            col("order_id"),
            col("customer_id"),
            trim(lower(col("order_status"))).alias("order_status"),
            to_timestamp(col("order_purchase_timestamp"),
                         "yyyy-MM-dd HH:mm:ss").alias("purchase_ts"),
            to_timestamp(col("order_approved_at"),
                         "yyyy-MM-dd HH:mm:ss").alias("approved_ts"),
            to_timestamp(col("order_delivered_customer_date"),
                         "yyyy-MM-dd HH:mm:ss").alias("delivered_ts"),
            to_timestamp(col("order_estimated_delivery_date"),
                         "yyyy-MM-dd HH:mm:ss").alias("estimated_delivery_ts"),
        )
        .filter(col("order_id").isNotNull())
        .withColumn("delivery_delay_days",
            when(
                col("delivered_ts").isNotNull() &
                col("estimated_delivery_ts").isNotNull(),
                (col("delivered_ts").cast("FLOAT") -
                 col("estimated_delivery_ts").cast("FLOAT")) / 86400
            ).otherwise(lit(None))
        )
        .withColumn("is_late",
            when(col("delivery_delay_days") > lit(0), lit(True))
            .otherwise(lit(False))
        )
    )
    src.write.mode("append").save_as_table("ecommerce_db.silver.fact_orders")
    return f"fact_orders: {src.count()} rows loaded"
$$;

-- ── Proc 2: fact_order_items ─────────────────────────────────
CREATE OR REPLACE PROCEDURE silver.sp_build_fact_order_items()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from snowflake.snowpark.functions import col, lit

def run(session):
    src = (
        session.table("ecommerce_db.bronze.stream_order_items")
        .select(
            col("order_id"),
            col("order_item_id").cast("INTEGER"),
            col("product_id"),
            col("seller_id"),
            col("price").cast("FLOAT"),
            col("freight_value").cast("FLOAT"),
        )
        .withColumn("total_value",
            col("price").cast("FLOAT") + col("freight_value").cast("FLOAT"))
        .filter(col("price").cast("FLOAT") > lit(0))
    )
    src.write.mode("append").save_as_table("ecommerce_db.silver.fact_order_items")
    return f"fact_order_items: {src.count()} rows loaded"
$$;

-- ── Proc 3: fact_payments ────────────────────────────────────
CREATE OR REPLACE PROCEDURE silver.sp_build_fact_payments()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from snowflake.snowpark.functions import col

def run(session):
    src = (
        session.table("ecommerce_db.bronze.stream_payments")
        .select(
            col("order_id"),
            col("payment_sequential").cast("INTEGER"),
            col("payment_type"),
            col("payment_installments").cast("INTEGER"),
            col("payment_value").cast("FLOAT"),
        )
        .filter(col("order_id").isNotNull())
    )
    src.write.mode("append").save_as_table("ecommerce_db.silver.fact_payments")
    return f"fact_payments: {src.count()} rows loaded"
$$;

-- ── Proc 4: fact_reviews ─────────────────────────────────────
CREATE OR REPLACE PROCEDURE silver.sp_build_fact_reviews()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from snowflake.snowpark.functions import col, to_timestamp, trim, lit, when

def run(session):
    src = (
        session.table("ecommerce_db.bronze.stream_reviews")
        .select(
            col("review_id"),
            col("order_id"),
            col("review_score").cast("INTEGER"),
            when(
                col("review_comment_message").isNotNull() &
                (trim(col("review_comment_message")) != lit("")),
                lit(True)
            ).otherwise(lit(False)).alias("has_comment"),
            to_timestamp(col("review_creation_date"),
                         "yyyy-MM-dd HH:mm:ss").alias("review_creation_ts")
        )
        .filter(col("review_id").isNotNull())
        .dropDuplicates(["review_id"])
    )
    src.write.mode("append").save_as_table("ecommerce_db.silver.fact_reviews")
    return f"fact_reviews: {src.count()} rows loaded"
$$;

-- ── Proc 5: dim_customers (SCD Type 2) ──────────────────────
CREATE OR REPLACE PROCEDURE silver.sp_build_dim_customers()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from snowflake.snowpark.functions import col, trim, upper, current_timestamp

def run(session):
    src = (
        session.table("ecommerce_db.bronze.stream_customers")
        .select(
            col("customer_id"),
            col("customer_unique_id"),
            col("customer_zip_code_prefix").alias("zip_code_prefix"),
            trim(upper(col("customer_city"))).alias("city"),
            trim(upper(col("customer_state"))).alias("state"),
        )
        .dropDuplicates(["customer_id"])
    )
    session.sql("""
        MERGE INTO ecommerce_db.silver.dim_customers tgt
        USING ecommerce_db.silver.dim_customers src
        ON tgt.customer_id = src.customer_id
           AND tgt.is_current = TRUE
           AND (tgt.city <> src.city OR tgt.state <> src.state)
        WHEN MATCHED THEN
            UPDATE SET tgt.is_current = FALSE,
                       tgt.dw_end_ts  = CURRENT_TIMESTAMP()
    """).collect()
    src.write.mode("append").save_as_table("ecommerce_db.silver.dim_customers")
    return f"dim_customers: {src.count()} rows loaded"
$$;

-- ── Proc 6: dim_products ─────────────────────────────────────
CREATE OR REPLACE PROCEDURE silver.sp_build_dim_products()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from snowflake.snowpark.functions import col, coalesce, round as round_

def run(session):
    products    = session.table("ecommerce_db.bronze.stream_products")
    translation = session.table("ecommerce_db.bronze.raw_category_translation")
    src = (
        products
        .join(translation,
              products["product_category_name"] ==
              translation["product_category_name"], "left")
        .select(
            products["product_id"],
            products["product_category_name"].alias("category_name_pt"),
            coalesce(translation["product_category_name_english"],
                     products["product_category_name"]).alias("category_name_en"),
            col("product_weight_g").cast("FLOAT").alias("weight_g"),
            col("product_length_cm").cast("FLOAT").alias("length_cm"),
            col("product_height_cm").cast("FLOAT").alias("height_cm"),
            col("product_width_cm").cast("FLOAT").alias("width_cm"),
        )
        .withColumn("volume_cm3",
            round_(col("length_cm") * col("height_cm") * col("width_cm"), 2))
        .dropDuplicates(["product_id"])
    )
    src.write.mode("append").save_as_table("ecommerce_db.silver.dim_products")
    return f"dim_products: {src.count()} rows loaded"
$$;

-- ── Test: call each proc manually once ───────────────────────
CALL silver.sp_build_fact_orders();
CALL silver.sp_build_fact_order_items();
CALL silver.sp_build_fact_payments();
CALL silver.sp_build_fact_reviews();
CALL silver.sp_build_dim_customers();
CALL silver.sp_build_dim_products();
