-- ============================================================
-- 01_setup.sql
-- Creates database, schemas, warehouse, S3 integration, stage
-- Run once in Snowflake Worksheets
-- ============================================================

CREATE DATABASE IF NOT EXISTS ecommerce_db;

CREATE SCHEMA IF NOT EXISTS ecommerce_db.bronze;
CREATE SCHEMA IF NOT EXISTS ecommerce_db.silver;
CREATE SCHEMA IF NOT EXISTS ecommerce_db.gold;
CREATE SCHEMA IF NOT EXISTS ecommerce_db.orchestration;

CREATE WAREHOUSE IF NOT EXISTS etl_wh
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'Warehouse for Olist ETL pipeline';

-- S3 integration (replace with your AWS account ID and role name)
CREATE OR REPLACE STORAGE INTEGRATION s3_olist_integration
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    ENABLED                   = TRUE
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::YOUR_ACCOUNT_ID:role/snowflake-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://your-ecommerce-bucket/olist/');


DESC INTEGRATION s3_olist_integration;
-- Copy: STORAGE_AWS_IAM_USER_ARN  → paste into IAM role trust policy Principal
-- Copy: STORAGE_AWS_EXTERNAL_ID   → paste into IAM role trust policy Condition

-- External stage
CREATE OR REPLACE STAGE ecommerce_db.bronze.s3_olist_stage
    STORAGE_INTEGRATION = s3_olist_integration
    URL                 = 's3://your-ecommerce-bucket/olist/'
    FILE_FORMAT         = (
        TYPE                         = 'CSV'
        PARSE_HEADER                 = TRUE
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_BLANK_LINES             = TRUE
        NULL_IF                      = ('NULL', 'null', '')
    );

-- Verify stage can see your S3 files
LIST @ecommerce_db.bronze.s3_olist_stage;
