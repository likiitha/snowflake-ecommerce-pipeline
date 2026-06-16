# Snowflake E-commerce Batch ETL Pipeline

End-to-end batch data engineering project built on the
Olist Brazilian E-commerce dataset using AWS S3 and Snowflake.

## Architecture

```
Olist CSVs (Kaggle)
      ↓
Amazon S3  (raw file storage)
      ↓
Snowpipe   (auto-ingest via SQS)
      ↓
Bronze     (raw VARIANT tables + Streams)
      ↓
Silver     (Snowpark Python stored procs — FACT + DIM tables)
      ↓
Gold       (Dynamic Tables — analytics-ready aggregates)
```

## Tech Stack

| Layer | Tool |
|---|---|
| Cloud storage | Amazon S3 |
| Auto-ingest | Snowpipe (SQS notification) |
| Data warehouse | Snowflake |
| Bronze → Silver | Snowpark Python (stored procedures) |
| Orchestration | Snowflake Tasks (DAG) |
| Gold layer | Snowflake Dynamic Tables |
| Development | Snowflake Web UI + AWS Console |

## Dataset

**Olist Brazilian E-commerce**
Source: [kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

| File | Rows | Description |
|---|---|---|
| olist_orders_dataset.csv | 99,441 | Order lifecycle |
| olist_order_items_dataset.csv | 112,650 | Line items per order |
| olist_customers_dataset.csv | 99,441 | Customer details |
| olist_products_dataset.csv | 32,951 | Product catalog |
| olist_order_payments_dataset.csv | 103,886 | Payment info |
| olist_order_reviews_dataset.csv | 99,224 | Customer reviews |
| olist_sellers_dataset.csv | 3,095 | Seller details |
| product_category_name_translation.csv | 71 | Portuguese → English |

> CSVs are not stored in this repo. Download from Kaggle and upload to S3.

## Project Structure

```
sql/
├── 01_setup.sql                 -- Database, schemas, warehouse, S3 stage
├── 02_bronze.sql                -- Raw tables, COPY INTO, Streams
├── 03_silver_tables.sql         -- Silver table definitions
├── 04_silver_transforms.sql     -- Snowpark stored procedures
├── 05_tasks.sql                 -- Task DAG (daily orchestration)
└── 06_gold_dynamic_tables.sql   -- 5 gold Dynamic Tables
```

## How to Run

All SQL was executed directly in **Snowflake Web UI → Worksheets**.
No local Python or CLI setup required.

### Step 1 — Upload CSVs to S3
- Download all Olist CSVs from Kaggle
- Create an S3 bucket in AWS Console
- Upload all CSVs into `s3://your-bucket/olist/`

### Step 2 — Run SQL files in order
Open each file in Snowflake Worksheets and run:

```
01_setup.sql             → creates DB, schemas, warehouse, external stage
02_bronze.sql            → loads raw data from S3, creates streams
03_silver_tables.sql     → creates typed silver target tables
04_silver_transforms.sql → registers Snowpark stored procedures
05_tasks.sql             → sets up Task DAG (runs daily at 2 AM UTC)
06_gold_dynamic_tables.sql → creates 5 auto-refreshing gold tables
```

## Gold Layer Output Tables

| Table | Description |
|---|---|
| `gold.rpt_daily_revenue` | Daily GMV, AOV, late delivery % |
| `gold.rpt_product_performance` | Revenue, orders, avg review per product |
| `gold.rpt_customer_segments` | RFM-based segmentation (Champions, Loyal, etc.) |
| `gold.rpt_payment_analysis` | Payment method breakdown |
| `gold.rpt_seller_performance` | Seller revenue, reviews, late deliveries |

## Key Concepts Demonstrated

- **Snowpipe auto-ingest** — files land in S3, Snowflake loads automatically via SQS
- **Bronze / Silver / Gold** — medallion architecture with clear separation of concerns
- **Snowpark Python** — DataFrame-style transforms without a Spark cluster
- **SCD Type 2** — full customer location change history in `dim_customers`
- **Streams** — CDC on bronze tables so silver only processes new rows
- **Task DAG** — dependency-chained daily orchestration
- **Dynamic Tables** — gold layer auto-refreshes when upstream data changes
