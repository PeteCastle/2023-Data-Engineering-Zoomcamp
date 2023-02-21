{{ config(materialized='table') }}

WITH fhv_data AS (
    SELECT *, 'FHV' AS service_type
    FROM {{ ref('stg_fhv_tripdata') }}
),
dim_zones AS (
    SELECT * FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
),
fhv_data_joined AS (
    SELECT
        fhv_data.dispatching_base_num,
        fhv_data.pickup_datetime,
        fhv_data.dropoff_datetime,
        
        pickup_zone.borough AS pickup_borough,
        pickup_zone.zone AS pickup_zone,
        dropoff_zone.borough AS dropoff_borough,
        dropoff_zone.zone AS dropoff_zone,

        fhv_data.SR_flag
    FROM fhv_data
    INNER JOIN dim_zones AS pickup_zone
        ON fhv_data.pickup_locationid = pickup_zone.locationid
    INNER JOIN dim_zones AS dropoff_zone
        ON fhv_data.dropoff_locationid = dropoff_zone.locationid
)
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone,
    SR_flag
FROM fhv_data_joined
WHERE pickup_borough IS NOT NULL AND dropoff_borough IS NOT NULL;
