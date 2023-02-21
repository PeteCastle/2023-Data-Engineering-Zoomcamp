{{ config(materialized='view') }}

SELECT 
    {% if var('is_test_run', false) %}
    TOP(100)
    {% endif %} 

    cast(Dispatching_base_num as nvarchar(10)) as dispatching_base_num,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    Pickup_datetime,
    DropOff_datetime,
 
    cast(SR_flag As integer) as SR_flag

FROM {{ source('staging','fhv_tripdata_2019_local') }}


