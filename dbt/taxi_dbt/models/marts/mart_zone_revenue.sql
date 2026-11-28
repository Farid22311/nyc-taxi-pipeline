{{ config(materialized='table') }}

select
    t.source_month,
    t.pickup_zone_id,
    z.zone_name,
    z.borough,
    count(*)             as trips,
    sum(t.total_amount)  as revenue
from {{ ref('stg_yellow_trips') }} t
left join {{ ref('stg_taxi_zones') }} z
    on t.pickup_zone_id = z.zone_id
where not t.is_reversal
group by 1, 2, 3, 4
