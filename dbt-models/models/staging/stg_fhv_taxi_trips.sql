{{ config(materialized="view") }}

-- select *
-- from {{ source('staging', 'fhv_taxi_trips') }}
-- limit 100
-- with
-- tripdata as (
-- select *, row_number() over (partition by pickup_datetime) as row_num
-- from {{ source("staging", "fhv_taxi_trips") }}
-- where pickup_datetime is not null
-- )
select
    -- identifiers
    {{ dbt_utils.surrogate_key(["PUlocationID", "pickup_datetime"]) }} as tripid,
    cast(pulocationid as string) as  pickup_locationid,
    cast(dolocationid as string) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    cast(sr_flag as string) as sr_flag,

    -- base number info
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(affiliated_base_number as string) as affiliated_base_num
-- from tripdata
from {{ source("staging", "fhv_taxi_trips") }}
-- where row_num = 1
-- dbt build --m model.sql --var 'is_test_run:false'
{% if var("is_test_run", default=True) %} limit 100 {% endif %}
