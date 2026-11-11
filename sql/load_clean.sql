BEGIN;

TRUNCATE clean_yellow_trips RESTART IDENTITY;

INSERT INTO clean_yellow_trips (
    vendorid, tpep_pickup_datetime, tpep_dropoff_datetime,
    passenger_count, trip_distance, ratecodeid, store_and_fwd_flag,
    pulocationid, dolocationid, payment_type,
    fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge,
    airport_fee, cbd_congestion_fee, request_source,
    has_passenger_count, is_reversal
)
SELECT
    vendorid, tpep_pickup_datetime, tpep_dropoff_datetime,
    passenger_count::INTEGER, trip_distance, ratecodeid::INTEGER, store_and_fwd_flag,
    pulocationid, dolocationid, payment_type::INTEGER,
    fare_amount::NUMERIC(10,2), extra::NUMERIC(10,2), mta_tax::NUMERIC(10,2),
    tip_amount::NUMERIC(10,2), tolls_amount::NUMERIC(10,2),
    improvement_surcharge::NUMERIC(10,2), total_amount::NUMERIC(10,2),
    congestion_surcharge::NUMERIC(10,2), airport_fee::NUMERIC(10,2),
    cbd_congestion_fee::NUMERIC(10,2), request_source,
    (passenger_count IS NOT NULL) AS has_passenger_count,
    (fare_amount < 0 OR total_amount < 0) AS is_reversal
FROM staging_yellow_trips
WHERE tpep_pickup_datetime >= '2020-01-01';

COMMIT;