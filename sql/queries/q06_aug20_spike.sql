-- Q6: Aug 19-21 daily trips, revenue and revenue per trip
-- Question: is the Aug 20 spike more trips or higher fares?
-- Reversals: excluded (WHERE NOT is_reversal), a reversal is a refund or voided charge, not a trip

SELECT
    tpep_pickup_datetime::DATE AS trip_date,
    COUNT(*) AS trips,
    SUM(total_amount) AS revenue,
    ROUND((SUM(total_amount) / COUNT(*))::NUMERIC, 2) AS revenue_per_trip
FROM clean_yellow_trips
WHERE NOT is_reversal
  AND tpep_pickup_datetime >= '2026-08-19'
  AND tpep_pickup_datetime <  '2026-08-22'
GROUP BY trip_date
ORDER BY trip_date;
-- Conclusion: Aug 20 is mainly a spike in trips (+30%, then back below the Aug 19 level on Aug 21).
-- Revenue per trip rose 8% and did not revert (29.40 -> 31.79 -> 31.29), so that part is a step up.
-- Trips explain most of the +1,249,617.47 revenue jump.
