#! /usr/bin/env sh
# coding: utf-8
# URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"
DATA_DIR="/data"
TABLE_NAME="green_taxi_trips"
docker run -it \
  --network=pg-network \
  -v /home/klang/de-zoomcamp/data/taxi_ingest_data:/data \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=${TABLE_NAME} \
    --url=${URL} \
    --data_dir=${DATA_DIR}