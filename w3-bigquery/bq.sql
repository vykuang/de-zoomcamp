-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `de-zoom-83.trips_data_all.fhv_taxi_trips`
OPTIONS (
  format = 'parquet',
  uris = ['gs://<my-bucket>/data/fhv/019-*.parquet']
);

--Count rows
SELECT COUNT(1)
FROM trips_data_all.fhv_taxi_trips

-- Query distinct num of affiliated_base_number
SELECT DISTINCT Affiliated_base_number
FROM `de-zoom-83.trips_data_all.fhv_taxi_data_partition` 
WHERE 
  CAST(pickup_datetime AS DATE) <= "2019-03-31" AND
  CAST(pickup_datetime AS DATE) >= "2019-03-01"

-- How many records with nulls in both PU and DO location ID
SELECT COUNT(1)
FROM trips_data_all.fhv_taxi_trips
WHERE 
  PUlocationID IS NULL AND
  DOlocationID IS NULL

-- create partitioned and clustered table
LOAD DATA OVERWRITE
  trips_data_all.fhv_taxi_trips_partition
PARTITION BY
  TIMESTAMP_TRUNC(pickup_datetime, MONTH)
CLUSTER BY
  Affiliated_base_number
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://<my-bucket>/data/fhv/2019-*.parquet']
)
