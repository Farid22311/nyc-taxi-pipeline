{{ config(materialized='view') }}

select
    trip_id,
    vendorid                as vendor_id,
    tpep_pickup_datetime    as pickup_at,
    tpep_dropoff_datetime   as dropoff_at,
    passenger_count,
    trip_distance           as trip_distance_miles,
    ratecodeid              as rate_code_id,
    store_and_fwd_flag,
    pulocationid            as pickup_zone_id,
    dolocationid            as dropoff_zone_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    request_source,
    has_passenger_count,
    is_reversal,
    source_month
from {{ source('taxi', 'clean_yellow_trips') }}
