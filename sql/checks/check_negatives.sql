SELECT
    COUNT(*) FILTER (WHERE fare_amount < 0)           AS neg_fare,
    COUNT(*) FILTER (WHERE total_amount < 0)          AS neg_total,
    COUNT(*) FILTER (WHERE tip_amount < 0)            AS neg_tip,
    COUNT(*) FILTER (WHERE tolls_amount < 0)          AS neg_tolls,
    COUNT(*) FILTER (WHERE extra < 0)                 AS neg_extra,
    COUNT(*) FILTER (WHERE mta_tax < 0)               AS neg_mta_tax,
    COUNT(*) FILTER (WHERE improvement_surcharge < 0) AS neg_improvement,
    COUNT(*) FILTER (WHERE congestion_surcharge < 0)  AS neg_congestion,
    COUNT(*) FILTER (WHERE airport_fee < 0)           AS neg_airport,
    COUNT(*) FILTER (WHERE cbd_congestion_fee < 0)    AS neg_cbd
FROM staging_yellow_trips;