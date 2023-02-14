# Week 4 - Analytics Engineering - Homework

1. num rows in `fact_trips`: 115,716,952
  - need to deduplicate. Should be around 63M
1. distribution between service types (green vs yellow) between 2019 and 2020?
  - use google data studio (now known as looker) or metabase
1. num rows in `stg_fhv_tripdata`: 43261276
  - Actually, there are a lot of duplicates for `tripid` even when I include `dropoff_datetime` as part of the surrogate key
  - `affiliated_base_num` is the culprit
  - for the exact same trip, there are many duplicates with different `affiliated_base_num`
  - may be an idiosyncrasy of their record keeping, but I think they're the same trip?
  - one example I saw had 39 identical pu/do ID, and pu/do datetimes
  - use the `row_number() over partition` trick?
  - dropped to 14633830 after deduplication
1. Create a core model for the stg_fhv_tripdata joining with dim_zones. 
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries 
for pickup and dropoff locations
  - try `row_number() over partition by (...) as row_num ... where row_num = 1`
  - expect fewer number of rows, removing all nulls in PU and DOLocationID
1. What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table.
Create a dashboard with some tiles that you find interesting to explore the data. 
One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.