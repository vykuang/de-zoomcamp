-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `de-zoom-83.trips_data_all.fhv_taxi_trips`
OPTIONS (
  format = 'CSV',
  uris = ['https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-*.csv.gz']
);