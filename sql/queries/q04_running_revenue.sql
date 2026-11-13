-- Q4: daily revenue and running total
-- Reversals: excluded with WHERE NOT is_reversal.

WITH daily AS (
    SELECT
        tpep_pickup_datetime::DATE AS pickup_date,
        SUM(total_amount)          AS revenue
    FROM clean_yellow_trips
    WHERE NOT is_reversal
    GROUP BY 1
)
SELECT
    pickup_date,
    revenue,
    SUM(revenue) OVER (ORDER BY pickup_date) AS running_revenue
FROM daily
ORDER BY pickup_date;