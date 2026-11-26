{{ config(materialized='table') }}

select
    source_month,
    extract(hour from pickup_at)::int as pickup_hour,
    count(*)                          as trips,
    sum(total_amount)                 as revenue
from {{ ref('stg_yellow_trips') }}
where not is_reversal
group by 1, 2
