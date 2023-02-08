# W3 - BigQuery and data warehouse

## Setup

Load `fhv-2019` data into a bigquery table

1. Load from https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv onto `gcs`
  - [original source from nyc.gov](https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-01.parquet)
  - `gs://<de_zoom_datalake>/data/fhv/<filename>_2019-*.parquet`
1. Load from `gcs` to bq via console or statement

  ```sql
  CREATE OR REPLACE EXTERNAL TABLE `de-zoom-83.trips_data_all.fhv_taxi_trips`
    OPTIONS (
    format = 'parquet',
    uris = ['gs://<de_zoom_datalake>/data/fhv/<filename>_2019-*.parquet']
    );
  ```

### Load fhv

Running into a casting datatype issue with `pd.read_parquet` and `fhv-2019-02.parquet`:

```py
pyarrow.lib.ArrowInvalid: Casting from timestamp[us] to timestamp[ns] would result in out of bounds timestamp: 33106123800000000
```

Refactor the code to retrieve first, and optionally load into df if we want to transform it