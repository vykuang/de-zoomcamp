{{ config(materialized='table') }}

select
    locationid,
    borough,
    zone,
    -- use single quote for literal strings
    replace(service_zone, 'Boro', 'Green') as service_zone
from
    {{ ref('taxi_zone_lookup') }}