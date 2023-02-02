#!/usr/bin/env bash
# coding: utf-8

prefect deployment build w2-orchestration/etl_web_gcs.py:etl_parent_flow \
  -n taxi-gcs-gh \
  -q taxi \
  -sb github/de-zoom-gh \
#   -ib docker-container/google-trends \
  -o web-gcs-gh-deployment \
  --apply