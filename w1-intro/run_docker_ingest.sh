#! /usr/bin/env sh
# coding: utf-8
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
DATA_DIR="/data"
docker run -it \
  --network=w1-intro_default \
  -v /home/kohada/de-zoomcamp/data/taxi_ingest_data:/data \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=w1-intro-pg_db-1 \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL} \
    --data_dir=${DATA_DIR}