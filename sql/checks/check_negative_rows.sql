SELECT payment_type, COUNT(*) AS n
FROM staging_yellow_trips
WHERE fare_amount < 0
GROUP BY payment_type
ORDER BY n DESC;