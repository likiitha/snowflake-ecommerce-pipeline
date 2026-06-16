 
# Snowflake E-commerce Batch ETL Pipeline

End-to-end data engineering project using the Olist Brazilian E-commerce dataset.
Covers the full pipeline from raw CSV ingestion to an analytics dashboard.

## Architecture

```
Olist CSVs → Amazon S3 → Snowpipe → Bronze (Raw)
  → Silver (Snowpark Python) → Gold (Dynamic Tables) 
```

## Tech Stack

| Layer | Technology |
|---|---|
| Storage | Amazon S3 |
| Ingestion | Snowpipe (auto-ingest via SQS) |
| Bronze | Snowflake raw tables + Streams |
| Transform | Snowpark Python (stored procedures) |
| Orchestration | Snowflake Tasks (DAG) |
| Gold | Snowflake Dynamic Tables |

## Dataset

[Olist Brazilian E-commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
— 9 CSV files, 100k orders, real Brazilian marketplace data.

Download and place all CSVs in the `data/` folder before running.

## Project Structure

```
├── ingestion/upload_to_s3.py       # uploads CSVs to S3
├── silver/silver_transforms.py     # Snowpark stored procedures
├── sql/
│   ├── 01_setup.sql                # DB, warehouse, stage
│   ├── 02_bronze.sql               # raw tables + COPY INTO + streams
│   ├── 03_silver_tables.sql        # silver table definitions
│   ├── 04_tasks.sql                # Task DAG
│   ├── 05_gold_dynamic_tables.sql  # 5 gold Dynamic Tables

```

## Setup

### Prerequisites
- AWS account with S3 bucket created
- Snowflake account (free trial works)
- Python 3.10+

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure credentials
```bash
# AWS
aws configure

# Snowflake — edit the config dict in silver/silver_transforms.py
```

### 3. Upload data to S3
```bash
python ingestion/upload_to_s3.py
```

### 4. Run SQL in Snowflake Worksheets (in order)
```
sql/01_setup.sql
sql/02_bronze.sql
sql/03_silver_tables.sql
sql/04_tasks.sql
sql/05_gold_dynamic_tables.sql
```

### 5. Register Snowpark stored procedures
```bash
python silver/silver_transforms.py
```


## Key Features

- **Snowpipe auto-ingest** — files land in S3, Snowflake loads them automatically
- **Snowpark Python** — PySpark-style DataFrame transforms without a Spark cluster
- **SCD Type 2** on dim_customers — full history of customer location changes
- **Dynamic Tables** — gold layer auto-refreshes hourly, no manual scheduling
- **Task DAG** — daily orchestration with dependency chain
- **5 gold reports** — revenue, products, customer RFM, payments, seller performance
