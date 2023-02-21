{{ config(materialized='view') }}

WITH tripdata AS
(
    SELECT *,
        row_number() OVER(partition by vendorid, lpep_pickup_Datetime ORDER BY lpep_pickup_Datetime) as rn
    -- RN is done to deal with duplicate records
    FROM {{ source('staging','green_tripdata_combined') }}
    WHERE vendorid IS NOT NULL 
)
SELECT 
    {% if var('is_test_run', false) %}
    TOP(100)
    {% endif %} 

    -- TO HASTEN THE LOAD PROCESS, SEVERAL COLUMNS WERE omitted FROM THE SELECT STATEMENT
    {{ dbt_utils.generate_surrogate_key(['vendorid','lpep_pickup_Datetime']) }} as tripid,
    cast(vendorid as integer) as vendorid,
    -- cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    DATEADD(S, 
            CASE 
                WHEN CAST(LEFT(lpep_pickup_Datetime, 10) AS BIGINT) > 2147483647 THEN 0 
                ELSE CAST(LEFT(lpep_pickup_Datetime, 10) AS INT) 
            END, 
            '1970-01-01') as pickup_datetime,
    DATEADD(S, 
            CASE 
                WHEN CAST(LEFT(lpep_dropoff_datetime, 10) AS BIGINT) > 2147483647 THEN 0 
                ELSE CAST(LEFT(lpep_dropoff_datetime, 10) AS INT) 
            END, 
            '1970-01-01') AS dropoff_datetime,

    -- store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    -- cast(trip_type as integer) as trip_type,

    -- cast(fare_amount as numeric) as fare_amount,
    -- cast(extra as numeric) as extra,
    -- cast(mta_tax as numeric) as mta_tax,
    -- cast(tip_amount as numeric) as tip_amount,
    -- cast(tolls_amount as numeric) as tolls_amount,
    -- cast(ehail_fee as numeric) as ehail_fee,
    -- cast(improvement_surcharge as numeric) as improvement_surcharge,
    -- cast(total_amount as numeric) as total_amount,
    -- cast(payment_type as integer) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description
    -- cast(congestion_surcharge as numeric) as congestion_surcharge

FROM tripdata
WHERE rn=1


