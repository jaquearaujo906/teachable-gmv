# Architecture – Daily GMV Data Pipeline

This document describes the architecture and design decisions of the Daily GMV data pipeline implemented for the Teachable Data Engineer case.

The solution follows a modern analytical data architecture pattern focused on scalability, reliability, reproducibility, and ease of consumption by business users.

---

## Overview

The pipeline processes event-based data from three source tables to generate a business-ready analytical table containing Daily GMV by subsidiary.

The architecture is based on the Medallion pattern:

Bronze → Silver → Gold

This separation ensures clear responsibilities for each data layer and allows safe reprocessing, auditing, and incremental evolution of the data model.

---

## Technology Stack

The project uses:

Python  
Apache Spark (PySpark)  
SQL  
Local filesystem as a Data Lake simulation (S3-compatible design)

This stack was intentionally chosen to be easily portable to cloud environments such as:

AWS (S3, Glue, Athena, EMR, Redshift)  
Databricks  
BigQuery  
Snowflake  

---

## Data Layers

### Bronze Layer – Raw Events

Purpose:

Store raw incoming events exactly as received from the source systems, without transformations.

Characteristics:

Append-only  
Schema preserved  
No deduplication  
Full historical traceability  

Data stored:

purchase.csv  
product_item.csv  
purchase_extra_info.csv  

Location:

data_lake/bronze/

---

### Silver Layer – Cleaned and Deduplicated

Purpose:

Transform raw events into a clean and consistent representation.

Operations:

Type casting  
Schema normalization  
CDC resolution  
Deduplication by purchase_id  

Deduplication strategy:

For each purchase_id, only the latest event is kept using:

row_number() over (partition by purchase_id order by transaction_datetime desc)

Location:

data_lake/silver/

---

### Gold Layer – Analytical Table

Purpose:

Provide a business-ready dataset optimized for analytical queries.

Table:

daily_gmv

Columns:

transaction_date  
subsidiary  
gmv  

Grain:

One row per day per subsidiary.

Partition:

transaction_date

Location:

data_lake/gold/daily_gmv/

---

## Pipeline Flow

1. Read raw CSV files from the Bronze layer  
2. Normalize schema and cast data types  
3. Apply CDC logic and deduplicate records  
4. Write cleaned datasets to the Silver layer  
5. Join Silver tables using purchase_id  
6. Filter only paid purchases (release_date not null)  
7. Calculate GMV per record  
8. Aggregate by transaction_date and subsidiary  
9. Write partitioned output to the Gold layer  

---

## Incremental Processing Strategy

The pipeline supports incremental execution through date parameters:

--start-date  
--end-date  

Only the partitions within this window are recomputed.

This allows:

Backfills  
Late data correction  
Safe reprocessing  
Cost-efficient execution  

The final table remains append-only at partition level, overwriting only the affected dates when necessary.

---

## Late Arriving Data Handling

Late-arriving events are handled by:

Re-running the pipeline for the impacted dates  
Rebuilding only the affected partitions  
Preserving existing partitions outside the reprocessing window  

This ensures data correctness without full table reloads.

---

## Data Quality Strategy

Implemented:

Deduplication by purchase_id  
Type validation through casting  
Filtering invalid (unpaid) purchases  

Suggested future improvements:

Null checks on critical fields  
Negative value detection for GMV  
Referential integrity validation  
Schema enforcement  
Anomaly detection on daily aggregates  

---

## Monitoring Strategy (Proposed)

In a production environment, the following metrics would be monitored:

Row count per partition  
Daily GMV deltas  
Pipeline execution duration  
Data freshness timestamps  

Alerts could be sent via:

Slack  
Microsoft Teams  
Email  

---

## Orchestration (Production Scenario)

In a real-world deployment, the pipeline would be orchestrated by:

Airflow  
Dagster  
Prefect  

Execution frequency:

Daily (D-1)

The pipeline would support:

Retries  
Backfills  
SLA monitoring  

---

## Cloud Migration Example (AWS)

Bronze → S3 raw bucket  
Silver → S3 curated bucket  
Gold → S3 analytics bucket  

Spark → EMR or AWS Glue  
Query engine → Athena or Redshift Spectrum  

---

## Design Principles

Separation of concerns  
Idempotent processing  
Incremental computation  
Business-oriented data modeling  
Ease of maintenance  
Scalability  
Auditability  

---

## Conclusion

This architecture provides a solid foundation for analytical workloads and demonstrates how event-based transactional data can be transformed into a clean, reliable, and easy-to-consume analytical dataset using modern data engineering practices.

---

Author:  
Jaqueline Araujo Xavier  
Data Engineer
