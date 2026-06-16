Snowflake E-commerce Batch ETL Pipeline
End-to-end batch data engineering project built on the Olist Brazilian E-commerce dataset using AWS S3 and Snowflake — developed entirely through Snowflake Web UI and AWS Console, no local setup required.
---
Architecture
```
Olist CSVs (Kaggle)
      │
      ▼
Amazon S3
(raw file storage — s3://bucket/olist/)
      │
      ▼
Snowpipe (auto-ingest via SQS notification)
      │
      ▼
Bronze Layer
(raw tables — VARIANT columns + Streams for CDC)
      │
      ▼
Silver Layer
(Snowpark Python stored procedures — typed FACT + DIM tables)
      │
      ▼
Gold Layer
(Dynamic Tables — 5 auto-refreshing analytics reports)
```
---
Tech Stack
Layer	Tool / Service
Raw file storage	Amazon S3
Auto-ingest	Snowpipe (SQS event notification)
Data warehouse	Snowflake
Bronze → Silver transforms	Snowpark Python (stored procedures)
Orchestration	Snowflake Tasks (DAG — daily at 2 AM UTC)
Gold layer	Snowflake Dynamic Tables (auto-refresh every hour)
Development environment	Snowflake Web UI + AWS Console (no local install)
---
Dataset
Olist Brazilian E-commerce Public Dataset  
Source: kaggle.com/datasets/olistbr/brazilian-ecommerce
Real marketplace data from Olist, the largest department store in Brazilian marketplaces.
File	Rows	Description
olist_orders_dataset.csv	99,441	Full order lifecycle (placed → delivered)
olist_order_items_dataset.csv	112,650	Line items — product, price, freight per order
olist_customers_dataset.csv	99,441	Customer ID, city, state, zip code
olist_products_dataset.csv	32,951	Product catalog — category, dimensions, weight
olist_order_payments_dataset.csv	103,886	Payment type, installments, value
olist_order_reviews_dataset.csv	99,224	Review score (1–5) and comment text
olist_sellers_dataset.csv	3,095	Seller location details
product_category_name_translation.csv	71	Portuguese → English category names
> **Note:** CSV files are not stored in this repository.  
> Download from Kaggle and upload to your S3 bucket before running.
---
Project Structure
```
snowflake-ecommerce-pipeline/
│
├── README.md
├── .gitignore
│
└── sql/
    ├── 01_setup.sql                  -- Database, schemas, warehouse, S3 stage, IAM integration
    ├── 02_bronze.sql                 -- 8 raw tables + COPY INTO + Streams
    ├── 03_silver_tables.sql          -- Typed silver table definitions
    ├── 04_silver_transforms.sql      -- Snowpark Python stored procedures
    ├── 05_tasks.sql                  -- Task DAG orchestration
    └── 06_gold_dynamic_tables.sql    -- 5 gold Dynamic Tables (final output)
```
---
How to Run
All SQL was executed directly in Snowflake Web UI → Worksheets.  
No Python installation or CLI required.
Prerequisites
Snowflake account (free trial at snowflake.com)
AWS account with S3 bucket created
Olist CSVs downloaded from Kaggle
---
Step 1 — Upload CSVs to S3
Log in to AWS Console → S3
Create a bucket (e.g. `my-ecommerce-bucket`)
Create a folder inside it called `olist/`
Upload all 8 Olist CSV files into `s3://my-ecommerce-bucket/olist/`
---
Step 2 — Run SQL files in order
Open each file in Snowflake Web UI → Projects → Worksheets and run:
File	What it does
`01_setup.sql`	Creates database `ecommerce_db`, 4 schemas, warehouse `etl_wh`, S3 storage integration and external stage
`02_bronze.sql`	Creates 8 raw tables, loads data with `COPY INTO`, creates Streams on each table
`03_silver_tables.sql`	Creates typed target tables: `fact_orders`, `fact_order_items`, `fact_payments`, `fact_reviews`, `dim_customers`, `dim_products`
`04_silver_transforms.sql`	Registers 6 Snowpark Python stored procedures — one per silver table
`05_tasks.sql`	Creates Task DAG — root task triggers 6 child tasks daily at 2 AM UTC
`06_gold_dynamic_tables.sql`	Creates 5 Dynamic Tables in the gold schema — auto-refresh every hour
> **Important:** After running `01_setup.sql`, copy the `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` values from `DESC INTEGRATION` output and paste them into your AWS IAM role trust policy before proceeding.
---
Medallion Architecture
Bronze Layer — Raw Ingest
All 8 CSV files loaded as-is into Snowflake using `COPY INTO`
Every column stored as `VARCHAR` — no transformations, no business logic
Purpose: immutable audit trail; safe to re-derive silver from this at any time
Streams created on each table to track new/changed rows (CDC)
Silver Layer — Cleaned & Typed
Snowpark Python stored procedures cast `VARCHAR` → proper types (`FLOAT`, `INTEGER`, `TIMESTAMP_NTZ`, `BOOLEAN`)
`fact_orders` — calculates `delivery_delay_days` and `is_late` flag
`fact_order_items` — derives `total_value = price + freight_value`
`fact_reviews` — derives `has_comment` boolean from review text
`dim_customers` — SCD Type 2: tracks city/state changes with `is_current` flag
`dim_products` — joins with category translation, computes `volume_cm3`
All procs use streams → only new rows are processed on each run
Gold Layer — Analytics Ready
5 Dynamic Tables declared as SQL — Snowflake refreshes them automatically every hour
No manual scheduling needed — upstream silver changes trigger re-computation
---
Gold Layer Output Tables
Table	Description	Key Metrics
`gold.rpt_daily_revenue`	Day-by-day sales summary	GMV, AOV, unique customers, late delivery %
`gold.rpt_product_performance`	Per-product analytics	Revenue, order count, avg price, avg review score
`gold.rpt_customer_segments`	RFM-based segmentation	Champions, Loyal, Recent, At Risk, Churned
`gold.rpt_payment_analysis`	Payment method breakdown	Order count, total value, avg installments
`gold.rpt_seller_performance`	Per-seller metrics	Revenue, orders fulfilled, avg review, late deliveries
---
Key Concepts Demonstrated
Medallion architecture — clear Bronze / Silver / Gold separation with defined responsibilities per layer
Snowpipe auto-ingest — S3 file events trigger automatic `COPY INTO` via SQS, no manual loading
Snowpark Python — DataFrame-style transforms running natively inside Snowflake, no Spark cluster needed
Streams (CDC) — only new or changed rows flow downstream, preventing full-table reprocessing
SCD Type 2 — full history of customer location changes preserved with `is_current` + `dw_end_ts`
Task DAG — dependency-chained orchestration; child tasks run only after parent succeeds
Dynamic Tables — gold layer auto-refreshes incrementally when upstream silver data changes
Zero local setup — entire pipeline built and run through Snowflake Web UI and AWS Console
---
Schemas in Snowflake
```
ecommerce_db
├── bronze          -- raw tables + streams
├── silver          -- fact + dim tables + stored procedures
├── gold            -- dynamic tables (analytics output)
└── orchestration   -- task DAG
```
---
Author
Built as a data engineering portfolio project demonstrating end-to-end batch ETL on Snowflake.
