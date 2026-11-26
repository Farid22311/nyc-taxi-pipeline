{{ config(materialized='table') }}

select
    source_month,
    pickup_at::date   as pickup_date,
    count(*)          as trips,
    sum(total_amount) as revenue
from {{ ref('stg_yellow_trips') }}
where not is_reversal
group by 1, 2
