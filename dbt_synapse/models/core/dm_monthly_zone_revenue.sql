{{ config(materialized='table') }}

WITH trips_data AS (
    SELECT * FROM {{ ref('fact_trips') }}
)
SELECT
    pickup_zone AS revenue_zone,
    MONTH(pickup_datetime) AS month,
    service_type,

    sum(fare_amount) AS revenue_monthly_fare,
    sum(extra) AS revenue_monthly_extra,
    sum(mta_tax) AS revenue_monthly_mta_tax,
    sum(tip_amount) AS revenue_monthly_tip_amount,
    sum(tolls_amount) AS revenue_monthly_tolls_amount,
    sum(ehail_fee) AS revenue_monthly_ehail_fee,
    sum(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    sum(total_amount) AS revenue_monthly_total_amount,
    sum(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    count(tripid) AS total_monthly_trips,
    avg(pASsenger_count) AS avg_montly_pASsenger_count,
    avg(trip_distance) AS avg_montly_trip_distance

FROM trips_data
GROUP BY pickup_zone, MONTH(pickup_datetime), service_type

