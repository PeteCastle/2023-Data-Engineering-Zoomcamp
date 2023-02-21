{{ config(materialized='table') }}


SELECT 
    locationid,
    borough,
    zone,
    replace(service_zone, 'Boro Zone', 'Green') as service_zone

FROM {{ ref('taxi_zone_lookup') }}