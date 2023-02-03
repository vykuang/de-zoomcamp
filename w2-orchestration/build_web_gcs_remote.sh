#!/usr/bin/env bash
# coding: utf-8

# still requires "gcsfs" library to use gcs 
# within remotefilesystem storage block
prefect deployment build etl_web_gcs.py:etl_parent_flow \
  -n taxi-upload-gcs \
  -q de-zoom-taxi \
  --infra process \
  --output web-gcs-remote \
  --storage-block remote-file-system/flow-storage-remote/ny-taxi \
  --params='{"color": "green", "months":[6], "year": 2019, "block_name":"ny-taxi-gcs"}'\
  --apply