#!/usr/bin/env bash
# coding: utf-8

prefect deployment build etl_web_gcs.py:etl_parent_flow \
  -n taxi-upload-gcs \
  -q de-zoom-taxi \
  --infra process \
  --output web-gcs-deployment \
  --params='{"color": "green", "months":[4], "year": 2019, "block_name":"ny-taxi-gcs"}'\
  --apply