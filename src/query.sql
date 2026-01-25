-- SQL Query – Daily GMV
SELECT
    transaction_date,
    subsidiary,
    gmv
FROM analytics.daily_gmv
ORDER BY transaction_date, subsidiary;
