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
        trip_duration,  -- Include trip_duration
        PERCENT_RANK() OVER (
            PARTITION BY pickup_year, pickup_month, pulocationid, dolocationid
            ORDER BY trip_duration
        ) AS trip_duration_p90
    from trips_data  
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
and t.trip_duration = p.trip_duration  -- Ensure trip_duration is also part of the join
order by t.pickup_year, t.pickup_month, t.trip_duration
