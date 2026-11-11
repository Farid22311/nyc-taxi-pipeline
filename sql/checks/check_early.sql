SELECT tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, total_amount
FROM staging_yellow_trips
WHERE tpep_pickup_datetime < '2026-08-01'
ORDER BY tpep_pickup_datetime;