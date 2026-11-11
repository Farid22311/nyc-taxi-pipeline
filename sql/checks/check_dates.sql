SELECT DATE_TRUNC('month', tpep_pickup_datetime) AS pickup_month,
       COUNT(*) AS n
FROM staging_yellow_trips
WHERE tpep_pickup_datetime < '2026-08-01'
GROUP BY 1
ORDER BY 1;