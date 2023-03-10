PQ_YELLOW="gs://$DTC_DATA_LAKE/data/raw/yellow/*"
PQ_GREEN="gs://$DTC_DATA_LAKE/data/raw/green/*"
PQ_REPORT="gs://$DTC_DATA_LAKE/data/report/yg_monthly/"
echo $SPARK_MASTER_HOST
echo $SPARK_GCS_JAR
spark-submit \
    --master $SPARK_MASTER_HOST \
    --jars $SPARK_GCS_JAR \
    spark_sql.py \
    -y $PQ_YELLOW \
    -g $PQ_GREEN \
    -O $PQ_REPORT
    