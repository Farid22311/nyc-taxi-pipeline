-- Returns a row (= test fails) if August gross revenue differs from the q04 total.
select total
from (
    select sum(revenue) as total
    from {{ ref('mart_daily_revenue') }}
    where source_month = '2026-08'
) t
where total <> 100779468.23
