#! /usr/env/bin bash
# export DTC_DATA_LAKE first
TAXI_YEAR=2020
PQ_YELLOW="gs://$DTC_DATA_LAKE/data/raw/yellow/yellow_tripdata_$TAXI_YEAR*"
PQ_GREEN="gs://$DTC_DATA_LAKE/data/raw/green/green_tripdata_$TAXI_YEAR*"
BQ_REPORT="trips_data_all.report_$TAXI_YEAR"
SPARK_BQ_JAR="gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.29.0.jar"
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=us-west1 \
    --jars $SPARK_BQ_JAR \
    gs://$DTC_DATA_LAKE/code/dataproc_bq_sql.py \
    -- \
        --input_yellow=$PQ_YELLOW \
        --input_green=$PQ_GREEN \
        --output=$BQ_REPORT
