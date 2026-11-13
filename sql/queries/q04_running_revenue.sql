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
    revenue
    -- add: a running total of revenue, ordered by date, named running_revenue
    -- pattern from Q3: FUNCTION(...) OVER (ORDER BY ...) AS name
    -- the function is SUM(revenue). Mind the comma after revenue.
FROM daily
ORDER BY pickup_date;