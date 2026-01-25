--Data Warehouse example (Redshift / BigQuery / Snowflake):
CREATE TABLE analytics.daily_gmv (
    transaction_date DATE NOT NULL,
    subsidiary VARCHAR(50) NOT NULL,
    gmv NUMERIC(18,2) NOT NULL
);

--External table example (Athena / Glue):

CREATE EXTERNAL TABLE analytics.daily_gmv (
    subsidiary STRING,
    gmv DOUBLE
)
PARTITIONED BY (transaction_date DATE)
LOCATION 's3://bucket/gold/daily_gmv/';