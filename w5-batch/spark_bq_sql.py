"""
Upload to GCS and submit to dataproc
"""
from pathlib import Path
import os

# from dotenv import load_dotenv
from argparse import ArgumentParser
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# from pyspark.conf import SparkConf
# from pyspark.context import SparkContext

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
# loading .env only for local testing
# otherwise configure with spark-submit args
# load_dotenv()

# # retrieve host URL from .env if testing; otherwise set in exec env
# SPARK_MASTER_HOST = os.getenv("SPARK_MASTER_HOST")
# SPARK_GCS_JAR = os.getenv("SPARK_GCS_JAR")
# # config for connecting to GCS
# conf = (
#     SparkConf().setAppName("test_standalone")
#     # .set("spark.jars", SPARK_GCS_JAR)
# )

# sc = SparkContext(conf=conf)

# hadoop_conf = sc._jsc.hadoopConfiguration()

# hadoop_conf.set(
#     "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
# )
# hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")

# if __name__ == "__main__":
parser = ArgumentParser()
# returns the func to opt
opt = parser.add_argument
# call the func
opt(
    "-y",
    "--input_yellow",
    type=str,
    required=True,
    help="Path for yellow taxi",
)
opt(
    "-g",
    "--input_green",
    type=str,
    required=True,
    help="Path for green taxi",
)
opt("-O", "--output", type=str, required=True, help="output dir for monthly report")
opt(
    "--loglevel",
    "-l",
    default="info",
    type=str.upper,
    help="log level: {DEBUG, INFO, WARNING, ERROR, CRITICAL}",
)
args = parser.parse_args()

loglevel = args.loglevel
loglevel_num = getattr(logging, loglevel)
logger.setLevel(loglevel_num)
ch.setLevel(loglevel_num)
logger.addHandler(ch)

logger.info(f"Instantiating spark session")
spark = SparkSession.builder.getOrCreate()

# the spark-bigquery connector requires a temporary bucket
# use an earlier one created by dataproc
bucket = "dataproc-temp-us-west1-861494848006-o2e0uj5a"
spark.conf.set("temporaryGcsBucket", bucket)

logger.info(f"Reading green parquets from {args.input_green}")
df_green = spark.read.parquet(args.input_green)

logger.info(f"Reading yellow parquets from {args.input_yellow}")
df_yellow = spark.read.parquet(args.input_yellow)

df_green = df_green.withColumnRenamed(
    "lpep_pickup_datetime", "pickup_datetime"
).withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

df_yellow = df_yellow.withColumnRenamed(
    "tpep_pickup_datetime", "pickup_datetime"
).withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

col_yellow = set(df_yellow.columns)
common_cols = [col for col in df_green.columns if col in col_yellow]

df_green_sel = df_green.select(common_cols).withColumn("service_type", F.lit("green"))

df_yellow_sel = df_yellow.select(common_cols).withColumn(
    "service_type", F.lit("yellow")
)

# same as SQL's union all
df_trips_data = df_green_sel.unionAll(df_yellow_sel)
logger.info(f"{df_trips_data.count()} rows in unioned dataset")
df_trips_data.createOrReplaceTempView("view_trips_data")

df_monthly = spark.sql(
    """
select
    -- Reveneue grouping 
    PULocationID as revenue_zone,
    -- revert back to postgres style syntax, from bigquery
    date_trunc('month', pickup_datetime) as revenue_month, 
    service_type, 

    -- Revenue calculation
    sum(fare_amount) as revenue_monthly_fare,
    sum(extra) as revenue_monthly_extra,
    sum(mta_tax) as revenue_monthly_mta_tax,
    sum(tip_amount) as revenue_monthly_tip_amount,
    sum(tolls_amount) as revenue_monthly_tolls_amount,
    -- sum(ehail_fee) as revenue_monthly_ehail_fee,
    sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
    sum(total_amount) as revenue_monthly_total_amount,
    sum(congestion_surcharge) as revenue_monthly_congestion_surcharge,

    -- Additional calculations
    count(1) as total_monthly_trips,
    avg(passenger_count) as avg_monthly_passenger_count,
    avg(trip_distance) as avg_monthly_trip_distance

from view_trips_data
group by revenue_zone, revenue_month, service_type
"""
)
df_monthly.write.format("bigquery").option("table", args.output).save()
logger.info(f"{df_monthly.count()} rows in report table")
