-- check_zone79: are the null-passenger rows concentrated in zone 79?
-- Hypothesis: zone 79 has many null-passenger rows (long trips, low fare for the distance)
-- Reversals: excluded (WHERE NOT is_reversal)

SELECT
    pulocationid = 79 AS is_zone79,
    has_passenger_count,
    COUNT(*) AS trips,
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_miles,
    ROUND((SUM(total_amount) / COUNT(*))::NUMERIC, 2) AS revenue_per_trip
FROM clean_yellow_trips
WHERE NOT is_reversal
GROUP BY is_zone79, has_passenger_count
ORDER BY is_zone79, has_passenger_count;
-- Verdict: the hypothesis holds for high average miles (null rows are 41.8% of zone 79 vs 27.3% elsewhere)
-- but not for low fares ($1.82 vs $3.71 per mile). The reason stays untested.
