# W3 - BigQuery and data warehouse

## Setup

Load `fhv-2019` data into a bigquery table

1. Load from https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv onto `gcs`
  - `gs://<de_zoom_datalake>/data/fhv/<filename>_2019-*.parquet`
1. Load from `gcs` to bq via console or statement

  ```sql
  CREATE OR REPLACE EXTERNAL TABLE `de-zoom-83.trips_data_all.fhv_taxi_trips`
    OPTIONS (
    format = 'parquet',
    uris = ['gs://<de_zoom_datalake>/data/fhv/<filename>_2019-*.parquet']
    );
  ```
  
  