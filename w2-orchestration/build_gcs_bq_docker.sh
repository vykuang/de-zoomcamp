#!/usr/bin/env bash
# coding: utf-8

# requires "gcsfs" library to use gcs storage block
prefect deployment build etl_gcs_bq.py:gcs_to_bq \
  -n taxi-gcs-bq \
  -q de-zoom-taxi \
  --infra-block docker-container/ingest-taxi-container \
  --output web-gcs-bq \
  --storage-block gcs/flow-storage-gcs/ny-taxi-bq/ \
  --params='{"color": "yellow", "year": 2019, "months":[2,3], "gcs_block":"ny-taxi-gcs"}' \
  --apply

# prefect deployment run \
#     --params='{"color": "yellow", "year": 2019, "months":[2,3], "gcs_block":"ny-taxi-gcs"}' \
#     taxi-gcs-bq/taxi-gcs-bq 

# prefect agent start \
#   -q de-zoom-taxi \
#   --hide-welcome \
#   --run-once