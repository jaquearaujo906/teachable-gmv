# Architecture – Daily GMV Data Pipeline

---

## Overview

This project implements a production-style ETL pipeline to calculate Daily GMV by subsidiary using an event-driven data model.

The architecture follows the Medallion pattern:

Bronze → Silver → Gold

It is designed to be:

- Reproducible
- Incremental
- Idempotent
- Resilient to duplicates and late-arriving data

---

## Technology Stack

- Apache Spark (PySpark)
- Python 3
- Local filesystem (simulating S3 / Data Lake)
- CSV as ingestion format
- SQL for analytical queries

This stack can be easily migrated to:

- AWS S3 + Glue + Athena
- Databricks
- BigQuery
- Redshift
- Snowflake

---

## Data Layers

### Bronze Layer – Raw Events

Purpose:

- Store raw incoming events
- Preserve original structure
- No transformations applied

Data:

- purchase.csv
- product_item.csv
- purchase_extra_info.csv

Storage:

data_lake/bronze/

---

### Silver Layer – Clean & Deduplicated

Purpose:

- Resolve CDC events
- Remove duplicates
- Keep latest version per purchase_id
- Enforce data types

Technique:

Window function:

row_number() over (partition by purchase_id order by transaction_datetime desc)

Storage:

data_lake/silver/

---

### Gold Layer – Analytical Table

Purpose:

- Business-ready dataset
- No joins required by analysts
- Optimized for aggregation

Table:

daily_gmv

Columns:

- transaction_date
- subsidiary
- gmv

Partition:

transaction_date

Storage:

data_lake/gold/daily_gmv/

---

## Pipeline Flow

1. Read Bronze CSV files
2. Cast and normalize data types
3. Deduplicate using window functions
4. Write Silver layer
5. Join Silver tables
6. Filter paid purchases
7. Calculate GMV
8. Aggregate by day and subsidiary
9. Write Gold layer partitions

---

## Incremental Processing Strategy

The pipeline supports incremental execution using a date window:

--start-date  
--end-date  

Example:

python etl_gmv.py --start-date 2023-02-01 --end-date 2023-02-28

Only affected partitions are recomputed.

---

## Late Arriving Data Handling

Late events are handled by:

- Reprocessing the impacted date partitions
- Overwriting only specific days
- Keeping the table append-only at partition level

This avoids full reloads while ensuring correctness.

---

## Data Quality Guarantees

Implemented:

- Deduplication by purchase_id
- Type casting
- Null filtering on release_date

Suggested improvements:

- Schema validation
- Range checks (negative GMV)
- Referential integrity checks
- Anomaly detection

---

## Monitoring Strategy (Proposed)

- Row count per partition
- Daily GMV delta comparison
- Freshness timestamp
- Pipeline duration

Alerting:

- Slack / Teams webhook
- Email notifications

---

## Daily Execution (Production Scenario)

In a real environment:

- Orchestrated via Airflow / Dagster / Prefect
- Triggered daily (D-1)
- Backfills supported via parameters

---

## Cloud Migration Example (AWS)

Bronze → S3 raw bucket  
Silver → S3 curated bucket  
Gold → S3 analytics bucket  
Query → Athena / Redshift Spectrum  

Spark → EMR / Glue / Databricks

---

## Why This Architecture

- Scalable
- Auditable
- Easy to maintain
- Analyst-friendly
- Production-aligned

---

Author: Jaqueline Araujo Xavier  
Role: Data Engineer
