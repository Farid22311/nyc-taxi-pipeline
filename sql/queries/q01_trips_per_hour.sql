-- Q1: trips and revenue per pickup hour
-- Reversals: excluded with WHERE NOT is_reversal. A reversal row is a refund or voided charge, not a trip, so counting it would inflate the trip count.
-- Revenue is gross, before refunds: excluding the 14,510 reversals (-$433,024.43) overstates revenue by about 0.43% if their original charges are in the file.

SELECT
    EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour,
    COUNT(*)                                AS trips,
    SUM(total_amount)                       AS revenue
FROM clean_yellow_trips
WHERE NOT is_reversal
GROUP BY 1
ORDER BY 1;