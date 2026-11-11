SELECT payment_type, COUNT(*) AS n,
       MIN(fare_amount) AS min_fare, MAX(fare_amount) AS max_fare,
       MIN(total_amount) AS min_total, MAX(total_amount) AS max_total
FROM staging_yellow_trips
WHERE total_amount < 0 AND fare_amount >= 0
GROUP BY payment_type
ORDER BY n DESC;