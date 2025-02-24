{{
    config(
        materialized='view'
    )
}}

with tripdata as (
    select *,
    from {{ source('staging','fhv_taxi_25_non_partition')}}
    where dispatching_base_num is not null
)

select 
    -- Identifiers
    {{ dbt_utils.generate_surrogate_key(["Affiliated_base_number", "pickup_datetime"]) }} as tripid,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pulocationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dolocationid,
    
    -- Trip info
    dispatching_base_num,
    {{ dbt.safe_cast("SR_Flag,", api.Column.translate_type("integer"))}} as sr_flag,
    Affiliated_base_number,

    -- Timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime
    cast(dropOff_datetime as timestamp) as dropoff_datetime

from trips_data

{% if var('is_test_run', default=true) %}

    limit 100

{% endif %}