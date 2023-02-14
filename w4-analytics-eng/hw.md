# Week 4 - Analytics Engineering - Homework

1. num rows in `fact_trips`: 115,716,952
1. distribution between service types (green vs yellow) between 2019 and 2020?
  - use google data studio or metabase
1. num rows in `stg_fhv_tripdata`: 43261276
1. Create a core model for the stg_fhv_tripdata joining with dim_zones. 
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries 
for pickup and dropoff locations
  - try `row_number() over partition by (...) as row_num ... where row_num = 1`
  - expect fewer number of rows, removing all nulls in PU and DOLocationID
1. What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table.
Create a dashboard with some tiles that you find interesting to explore the data. 
One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.