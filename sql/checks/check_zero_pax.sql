SELECT passenger_count = 0 AS zero_passengers,
       COUNT(*) AS n,
       AVG(trip_distance) AS avg_distance,
       AVG(fare_amount) AS avg_fare
FROM staging_yellow_trips
WHERE passenger_count IS NOT NULL
GROUP BY 1;