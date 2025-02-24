{{
    config(
        materialized='table'
    )
}}

with trips_data as (
    select 
        dispatching_base_num,
        sr_flag,
        Affiliated_base_number,
        pickup_zone,
        pulocationid,
        pickup_datetime,
        pickup_year,
        pickup_month,
        dropoff_datetime,
        dolocationid,
        dropoff_zone,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) as trip_duration
    from {{ ref('dim_fhv_trips') }}
),

p90_trip_duration as (
    select 
        pickup_year,
        pickup_month,
        pulocationid,
        dolocationid,
        APPROX_QUANTILES(trip_duration, 100)[OFFSET(90)] as trip_duration_p90
    from trips_data
    group by pickup_year, pickup_month, pulocationid, dolocationid
)

select 
    t.*,
    p.trip_duration_p90
from trips_data t
left join p90_trip_duration p
on t.pickup_year = p.pickup_year
and t.pickup_month = p.pickup_month
and t.pulocationid = p.pulocationid
and t.dolocationid = p.dolocationid
order by t.pickup_year, t.pickup_month, t.trip_duration
