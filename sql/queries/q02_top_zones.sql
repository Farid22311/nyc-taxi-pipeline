-- Q2: top 10 pickup zones by revenue
-- Reversals: excluded with WHERE NOT is_reversal, the same rule as Q1.
-- Why a CTE: it names the per-zone totals as one step, so the final SELECT only ranks them. A later change, such as joining zone names, goes in one place.

WITH zone_revenue AS (
    SELECT
        pulocationid,
        COUNT(*)                                   AS trips,
        SUM(total_amount)                          AS revenue,
        ROUND(SUM(total_amount) / COUNT(*), 2)     AS revenue_per_trip
    FROM clean_yellow_trips
    WHERE NOT is_reversal
    GROUP BY pulocationid
)
SELECT pulocationid, trips, revenue, revenue_per_trip
FROM zone_revenue
ORDER BY revenue DESC
LIMIT 10;