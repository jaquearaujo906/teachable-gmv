# Teachable – Data Engineer II Technical Case
## Daily GMV by Subsidiary

---

## Goal

Build an ETL pipeline and a final analytical table that provides Daily GMV by subsidiary, easy to query by analysts and business users without complex joins or knowledge of the raw event structure.

This project implements a production-oriented data pipeline using a medallion architecture (Bronze → Silver → Gold).

---

## Data Understanding

The dataset is composed of three event tables:

- purchase
- product_item
- product_extra_info (or purchase_extra_info)

All tables can be joined using:

purchase_id

---

## GMV Business Rule

Only paid / released purchases are considered:

release_date IS NOT NULL

GMV formula per item:

gmv_value = purchase_value * item_quantity

Daily GMV is calculated by aggregating by:

transaction_date + subsidiary

---

## Final Table Design (Gold Layer)

### Table grain

One row per:

(transaction_date, subsidiary)

### Columns

transaction_date (DATE)  
subsidiary (STRING)  
gmv (DECIMAL / DOUBLE)

### Partitioning

The table is partitioned by:

transaction_date

---

## Final Table DDL

Data Warehouse example (Redshift / BigQuery / Snowflake):

CREATE TABLE analytics.daily_gmv (
    transaction_date DATE NOT NULL,
    subsidiary VARCHAR(50) NOT NULL,
    gmv NUMERIC(18,2) NOT NULL
);

External table example (Athena / Glue):

CREATE EXTERNAL TABLE analytics.daily_gmv (
    subsidiary STRING,
    gmv DOUBLE
)
PARTITIONED BY (transaction_date DATE)
LOCATION 's3://bucket/gold/daily_gmv/';

---

## ETL Logic Summary

### Bronze → Silver

Raw CSV events are ingested into the Bronze layer.

CDC and duplicate events are resolved using:

row_number() over (partition by purchase_id order by transaction_datetime desc)

Only the latest event per purchase is kept.

---

### Silver → Gold

Steps:

1. Join the three tables on purchase_id
2. Filter only paid purchases (release_date IS NOT NULL)
3. Compute GMV
4. Aggregate by transaction_date and subsidiary
5. Write to Gold partitioned by date

---

## Data Issues Handling

Duplicate events: resolved by keeping the latest event per purchase_id using window functions.

Late-arriving data: handled via configurable reprocessing window.

Append-only table: Gold table is partitioned by date and only affected partitions are overwritten.

This guarantees reproducibility and prevents data duplication.

---

## SQL Query – Daily GMV

SELECT
    transaction_date,
    subsidiary,
    gmv
FROM analytics.daily_gmv
ORDER BY transaction_date, subsidiary;

---

## Example Output

transaction_date | subsidiary | gmv  
2023-02-08 | internacional | 554.50  
2023-02-18 | nacional | 778.55  
2023-02-23 | internacional | 505.50  

---

## How to Run

Install dependencies:

pip install -r requirements.txt

Run ETL (automatic window detection):

python src/etl_gmv.py

Run with custom window:

python src/etl_gmv.py --start-date 2023-02-01 --end-date 2023-02-28

---

## Project Structure

src/  
data_lake/  
  bronze/  
  silver/  
  gold/  
README.md  
ARCHITECTURE.md  
requirements.txt  

---

## Deliverables

Final analytical table (Gold layer)  
ETL script (etl_gmv.py)  
SQL query  
Table DDL  
Architecture documentation (ARCHITECTURE.md)

---

## Architecture

Detailed architecture description is available in:

ARCHITECTURE.md

---

## Optional Improvements

Data quality checks (null values, negative values, schema validation)  
Monitoring metrics (row counts, GMV deltas)  
Freshness checks  
Alerting (Slack / Teams)

---

Author: Jaqueline Araujo Xavier  
Role: Data Engineer

