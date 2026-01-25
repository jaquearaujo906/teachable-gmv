# Daily GMV Data Pipeline – Teachable Data Engineer Case

This project implements an end-to-end ETL pipeline to calculate the Daily GMV (Gross Merchandise Value) by subsidiary from three event-based source tables.

The solution was designed with production best practices in mind, using a Medallion Architecture (Bronze / Silver / Gold), supporting incremental processing, deduplication, late-arriving data handling, and partitioned analytical output.

The final result is an analytical table that is easy to query by analysts and business users, without requiring complex joins or knowledge of the raw event structure.

---

## Project Structure

teachable-gmv/
├── src/
│   └── etl_gmv.py  
├── data_lake/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│       └── daily_gmv/  
├── sql/
│   ├── daily_gmv_ddl.sql
│   └── daily_gmv_query.sql
├── README.md  
├── ARCHITECTURE.md  
├── requirements.txt  
└── .gitignore  

---

## Data Understanding

The pipeline processes three source tables:

purchase  
Contains purchase lifecycle events, including purchase_id, transaction_datetime, order_date, release_date (payment confirmation) and subsidiary.

product_item  
Contains product-level purchase information such as purchase_id, product_id, item_quantity and purchase_value.

purchase_extra_info  
Contains additional attributes related to the purchase.

All tables are joined using the field:

purchase_id

---

## GMV Business Logic

Only paid purchases are included in the GMV calculation.

A purchase is considered valid when:

release_date IS NOT NULL

GMV formula:

GMV = purchase_value * item_quantity

---

## Final Analytical Table

Columns:

transaction_date (DATE)  
subsidiary (STRING)  
gmv (DOUBLE)

Grain:

One row per transaction_date and subsidiary.

Partition:

transaction_date

---

## ETL Logic Summary

The pipeline executes the following steps:

1. Read raw CSV files from the Bronze layer  
2. Normalize and cast data types  
3. Resolve CDC events using window functions  
4. Deduplicate by purchase_id keeping the latest event  
5. Write cleaned data to the Silver layer  
6. Join the three Silver tables  
7. Filter only paid purchases  
8. Calculate GMV  
9. Aggregate by day and subsidiary  
10. Write partitioned output to the Gold layer  

---

## Reproducibility and Incremental Processing

The pipeline supports execution using a date window:

--start-date  
--end-date  

Example:

python src/etl_gmv.py --start-date 2023-02-01 --end-date 2023-02-28

Only the affected partitions are recalculated, allowing safe reprocessing without duplicating data.

---

## Handling Data Issues

Duplicates are handled using:

row_number() over (partition by purchase_id order by transaction_datetime desc)

Only the latest event is kept.

Late-arriving data is handled by reprocessing only the impacted date partitions while keeping the table append-only at partition level.

---

## SQL Deliverables

The project includes a sql folder containing:

daily_gmv_ddl.sql – final table definition for Data Warehouses and External Tables  
daily_gmv_query.sql – query to retrieve daily GMV by subsidiary  

---

## Example SQL Query

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

## Architecture

A detailed architecture description is available in ARCHITECTURE.md and includes:

Data layers design  
CDC strategy  
Incremental processing  
Late data handling  
Monitoring strategy  
Cloud migration approach  

---

## How to Run Locally

1. Create a virtual environment  
2. Install dependencies:

pip install -r requirements.txt

3. Run the pipeline:

python src/etl_gmv.py

Or with incremental window:

python src/etl_gmv.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD

---

## Technologies Used

Python  
Apache Spark (PySpark)  
SQL  
Local filesystem (Data Lake simulation)  

---

## Evaluation Criteria Coverage

This solution addresses:

Correct GMV logic  
Proper joins and aggregations  
Analytical data modeling  
Partitioned final table  
Clean and readable code  
Reproducible pipeline  
Duplicate handling  
Late-arriving data support  
SQL DDL and analytical query  
Architecture documentation  

---

## Author

Jaqueline Araujo Xavier  
Data Engineer
