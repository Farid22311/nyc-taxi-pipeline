CREATE TABLE IF NOT EXISTS staging_yellow_trips (
    vendorid INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count BIGINT,
    pulocationid INTEGER,
    dolocationid INTEGER,
    ratecodeid BIGINT,
    payment_type BIGINT,
    store_and_fwd_flag TEXT,
    request_source TEXT,
    distances DOUBLE PRECISION,
    fees DOUBLE PRECISION,
    amounts DOUBLE PRECISION
);