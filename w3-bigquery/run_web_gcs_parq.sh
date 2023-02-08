#! /usr/bin/env sh
# coding: utf-8
docker run -it \
  -v /home/klang/de-zoomcamp/data/taxi_ingest_data:/data \
  -e PREFECT_API_URL=${PREFECT_API_URL} \
  -e PREFECT_API_KEY=${PREFECT_API_KEY} \
  vykuang/de-zoom:taxi-ingest-v002 \
    --taxi-type=fhv \
    --year=2019 \
    --months=1-12 \
    --data-dir="/data"