{{
    config(
        materialized='table'
    )
}}
with trips_data as (
    select * from {{ ref("fact_trips")}}
)
select
-- revenue grouping
pickup_zone as revenue_zone,
concat('Q', extract(quarter from pickup_datetime), '_', extract(year from pickup_datetime)) as quarter_label,
{{ dbt.date_trunc("quarter","pickup_datetime")}} as revenue_quarter,

service_type, 

-- Revenue calculation 
sum(total_amount) as total_revenue_quarterly

from trips_data
group by 1,2,3,4