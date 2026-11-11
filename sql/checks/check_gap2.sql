SELECT COUNT(*) AS n
FROM staging_yellow_trips
WHERE fare_amount < 0 AND total_amount >= 0;