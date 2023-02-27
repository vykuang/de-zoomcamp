from pathlib import Path
import os
from dotenv import load_dotenv
from argparse import ArgumentParser
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
load_dotenv()

SPARK_MASTER_HOST = os.getenv("SPARK_MASTER_HOST")

# config for connecting to GCS
conf = SparkConf() \
    .setMaster(SPARK_MASTER_HOST) \
    .setAppName('test_standalone') \
    .set('spark.jars', '../data/lib/gcs-connector-hadoop3-latest.jar')

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")

if __name__ == "__main__":
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
    opt("-y", "--year", type=int, default=2020, help="year for the dataset")
    opt("-m", "--months", type=str, default="1-12", help="month: {1-12}, or month_A-month_B, e.g. 1-12")
    opt(
        "--loglevel",
        "-l",
        default="info",
        type=str.upper,
        help="log level: {DEBUG, INFO, WARNING, ERROR, CRITICAL}"
    )
    args = parser.parse_args()
    if "-" in args.months:
        mth_start, mth_end = list(map(int, args.months.split("-")))
    else:
        mth_start = mth_end = int(args.months)
    months = list(range(mth_start, mth_end + 1))
    
    loglevel = args.loglevel
    loglevel_num = getattr(logging, loglevel)
    logger.setLevel(loglevel_num)
    ch.setLevel(loglevel_num)
    logger.addHandler(ch)

    logger.info(f"Instantiating spark session")
    spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()
    logger.info(f"Reading green parquets from {args.input_green}")
    df_green = spark.read.parquet(args.input_green)
    logger.info(f"Reading yellow parquets from {args.input_yellow}")
    df_yellow = spark.read.parquet(args.input_yellow)

    df_green = df_green \
        .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
        .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

    df_yellow = df_yellow \
        .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
        .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
    
    col_yellow = set(df_yellow.columns)
    common_cols = [col for col in df_green.columns if col in col_yellow]

    df_green_sel = df_green \
        .select(common_cols) \
        .withColumn('service_type', F.lit('green'))

    df_yellow_sel = df_yellow \
        .select(common_cols) \
        .withColumn('service_type', F.lit('yellow'))
    
    # same as SQL's union all
    df_trips_data = df_green_sel.unionAll(df_yellow_sel)
    df_trips_data.createOrReplaceTempView('view_trips_data')

    df_monthly = spark.sql("""
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
    """)
    df_monthly.coalesce(4).write.parquet(args.output)
