SELECT is_reversal, COUNT(*) AS trips, SUM(total_amount) AS revenue
FROM clean_yellow_trips
GROUP BY is_reversal;