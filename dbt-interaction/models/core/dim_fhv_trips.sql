{{
    config(
        materialized='table'
    )
}}

with fhv_data as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * 
    from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhv_data.dispatching_base_num,
    fhv_data.sr_flag,
    fhv_data.Affiliated_base_number,
    fhv_data.pulocationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone,
    fhv_data.pickup_datetime,
    EXTRACT(YEAR FROM fhv_data.pickup_datetime) as pickup_year,
    EXTRACT(MONTH FROM fhv_data.pickup_datetime) as pickup_month,
    fhv_data.dolocationid,
    dropoff_zone.borough as dropoff_borough,
    pickup_zone.zone as dropoff_zone,
    fhv_data.dropoff_datetime
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.pulocationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.dolocationid = dropoff_zone.locationid