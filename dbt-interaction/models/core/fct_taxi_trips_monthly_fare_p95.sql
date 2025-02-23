{{
    config(
        materialized='table'
    )
}}

WITH trips_data AS (
    SELECT 
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS trip_year,
        EXTRACT(MONTH FROM pickup_datetime) AS trip_month,
        fare_amount,
        trip_distance,
        payment_type_description,

        -- Compute the continuous percentile of fare_amount
        PERCENT_RANK() OVER (
            PARTITION BY service_type, EXTRACT(YEAR FROM pickup_datetime), EXTRACT(MONTH FROM pickup_datetime)
            ORDER BY fare_amount
        ) AS fare_percentile

    FROM {{ ref('fact_trips') }}

    -- Filter out invalid data
    WHERE fare_amount > 0
      AND trip_distance > 0
      AND payment_type_description IN ('Cash', 'Credit Card')
)

SELECT *
FROM trips_data
ORDER BY service_type, trip_year, trip_month, fare_percentile