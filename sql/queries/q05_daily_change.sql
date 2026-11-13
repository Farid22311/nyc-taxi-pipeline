-- Q5: daily revenue and change from the previous day
-- Reversals: excluded with WHERE NOT is_reversal.

WITH daily AS (
    SELECT
        tpep_pickup_datetime::DATE AS pickup_date,
        SUM(total_amount)          AS revenue
    FROM clean_yellow_trips
    WHERE NOT is_reversal
    GROUP BY 1
),
with_prev AS (
    SELECT
        pickup_date,
        revenue
        -- add: the previous day's revenue, named prev_revenue
        -- the function is LAG(revenue) OVER (ORDER BY ...)
    FROM daily
)
SELECT
    pickup_date,
    revenue,
    prev_revenue,
    revenue - prev_revenue AS change
FROM with_prev
ORDER BY pickup_date;