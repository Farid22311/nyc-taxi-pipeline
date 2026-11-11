SELECT
    COUNT(*) FILTER (WHERE airport_fee < 0
                     AND NOT (fare_amount < 0 OR total_amount < 0)) AS neg_airport_not_reversal,
    COUNT(*) FILTER (WHERE passenger_count = 0)                     AS zero_passengers,
    COUNT(*) FILTER (WHERE tpep_pickup_datetime > tpep_dropoff_datetime) AS pickup_after_dropoff,
    COUNT(*) FILTER (WHERE vendorid IS NULL
                        OR tpep_pickup_datetime IS NULL
                        OR tpep_dropoff_datetime IS NULL
                        OR trip_distance IS NULL
                        OR pulocationid IS NULL
                        OR dolocationid IS NULL
                        OR payment_type IS NULL
                        OR fare_amount IS NULL
                        OR extra IS NULL
                        OR mta_tax IS NULL
                        OR tip_amount IS NULL
                        OR tolls_amount IS NULL
                        OR improvement_surcharge IS NULL
                        OR total_amount IS NULL)                    AS nulls_in_not_null_cols,
    MIN(tpep_pickup_datetime) AS first_pickup,
    MAX(tpep_pickup_datetime) AS last_pickup
FROM staging_yellow_trips;