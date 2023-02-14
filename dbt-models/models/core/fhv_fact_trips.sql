{{ config(materialized='table') }}

with fhv_taxi_trips as (
    select *, 'fhv' as service_type
    from
        {{ ref('stg_fhv_taxi_trips') }}
),
-- filter the lookup.csv before joining
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    *
from fhv_taxi_trips
inner join dim_zones as pickup_zone
on fhv_taxi_trips.pickup_locationid = cast(pickup_zone.locationid as string)
inner join dim_zones as dropoff_zone
on fhv_taxi_trips.dropoff_locationid = cast(dropoff_zone.locationid as string)