{{ config(
    materialized='view',
    partition_by={
        "field": "pickup_datetime",
        "data_type": "timestamp",
        "granularity": "day"
    }
) }}
 
-- with tripdata as 
-- (
--   select *,
--     row_number() over(partition by vendorid, tpep_pickup_datetime) as rn
--   from {{ source('staging','yellow_taxi_trips') }}
--   where vendorid is not null 
-- )
select
   -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as tripid,
    cast(vendorid as string) as vendorid,
    cast(ratecodeid as string) as ratecodeid,
    cast(pulocationid as string) as  pickup_locationid,
    cast(dolocationid as string) as dropoff_locationid,
    
    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    -- yellow cabs are always street-hail
    '1' as trip_type,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    -- only present in green, but must add here to join with green
    cast(0 as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as string) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description, 
    cast(congestion_surcharge as numeric) as congestion_surcharge
-- from tripdata
-- partition must be done in config, at top of .sql, not here
-- partition by TIMESTAMP_TRUNC(pickup_datetime, DAY)
from {{ source('staging','yellow_taxi_trips') }}
-- where rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

