#!/usr/bin/env bash
# coding: utf-8

# requires "gcsfs" library to use gcs storage block
prefect deployment build etl_web_gcs.py:etl_parent_flow \
  -n taxi-upload-gcs \
  -q de-zoom-taxi \
  --infra process \
  --output web-gcs-cloud \
  --storage-block gcs/flow-storage-gcs/ny-taxi \
  --params='{"color": "green", "months":[5], "year": 2019, "block_name":"ny-taxi-gcs"}'\
  --apply