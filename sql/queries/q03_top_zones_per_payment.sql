-- Q3: top 3 pickup zones by revenue within each payment type
-- Reversals: excluded with WHERE NOT is_reversal, the same rule as Q1 and Q2.
-- Why two CTEs: a window function can't be used in WHERE, so the rank is computed in one step and filtered in the next.

WITH zone_payment AS (
    SELECT
        payment_type,
        pulocationid,
        COUNT(*)          AS trips,
        SUM(total_amount) AS revenue
    FROM clean_yellow_trips
    WHERE NOT is_reversal
    GROUP BY payment_type, pulocationid
),
ranked AS (
    SELECT
        payment_type, pulocationid, trips, revenue,
        RANK() OVER (PARTITION BY payment_type ORDER BY revenue DESC) AS rnk
    FROM zone_payment
)
SELECT payment_type, rnk, pulocationid, trips, revenue
FROM ranked
WHERE rnk <= 3
ORDER BY payment_type, rnk;