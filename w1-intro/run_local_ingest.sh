#! /usr/bin/env sh
# coding: utf-8
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
DATA_DIR="../data/taxi_ingest_data/"
python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL} \
  --data_dir=${DATA_DIR}