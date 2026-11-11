SELECT
    MIN(fare_amount)           AS min_fare,           MAX(fare_amount)           AS max_fare,
    MIN(extra)                 AS min_extra,          MAX(extra)                 AS max_extra,
    MIN(mta_tax)               AS min_mta_tax,        MAX(mta_tax)               AS max_mta_tax,
    MIN(tip_amount)            AS min_tip,            MAX(tip_amount)            AS max_tip,
    MIN(tolls_amount)          AS min_tolls,          MAX(tolls_amount)          AS max_tolls,
    MIN(improvement_surcharge) AS min_improvement,    MAX(improvement_surcharge) AS max_improvement,
    MIN(total_amount)          AS min_total,          MAX(total_amount)          AS max_total,
    MIN(congestion_surcharge)  AS min_congestion,     MAX(congestion_surcharge)  AS max_congestion,
    MIN(airport_fee)           AS min_airport,        MAX(airport_fee)           AS max_airport,
    MIN(cbd_congestion_fee)    AS min_cbd,            MAX(cbd_congestion_fee)    AS max_cbd
FROM staging_yellow_trips;
