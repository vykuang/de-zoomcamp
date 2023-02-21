#! /usr/bin/env python3
from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql import types
from argparse import ArgumentParser
from pyspark.sql import functions as F


def get_col_types(df) -> dict:
    """
    Returns dict of types containing column names
    Types detected:
        int
        string
        float
        timestamp
    """
    dtypes = ["int", "string", "float", "timestamp"]
    col_all = df.columns
    col_ints = [
        col
        for col in col_all
        if "_time" in col or "ID" in col or "_type" in col or "_count" in col
    ]
    col_str = [col for col in col_all if "_num" in col or "_flag" in col]
    col_timestamp = [col for col in col_all if "_datetime" in col]
    col_flt = [
        col
        for col in col_all
        if col not in col_ints and col not in col_str and col not in col_timestamp
    ]
    return dict(zip(dtypes, [col_ints, col_str, col_flt, col_timestamp]))


def cast_schema(spark_client, raw_path: Path, parts_dir: Path, num_parts: int = 4):
    """
    Reads parquet from raw_path, casts the input schema onto it,
    then partitions into output folder
    """
    # read as-is
    df = spark_client.read.option("header", "true").parquet(str(raw_path))
    # get schema
    schema = get_col_types(df)
    # cast schema
    for dtype in schema:
        match dtype:
            case "int":
                spark_type = types.IntegerType()
            case "string":
                spark_type = types.StringType()
            case "float":
                spark_type = types.FloatType()
            case "timestamp":
                spark_type = types.TimestampType()
        col_map = {col: F.col(col).cast(spark_type) for col in schema[dtype]}
        # withColumns() returns a new dataframe
        df = df.withColumns(col_map)
    # repartition and write
    df.repartition(num_parts).write.parquet(
        str(parts_dir), mode="overwrite", compression="gzip"
    )
    return df


if __name__ == "__main__":
    parser = ArgumentParser()
    # returns the func to opt
    opt = parser.add_argument
    # call the func
    opt(
        "-t",
        "--taxi_type",
        type=str,
        help="{fhv, fhvhv, yellow, green}",
    )
    opt("-y", "--year", type=int, help="year for the dataset")
    opt("-n", "--num_partitions", default=24, type=int, help="Number of partitions")
    args = parser.parse_args()
    for month in range(1, 13):        
        raw_path = Path(f"../data/taxi_ingest_data/raw/{args.taxi_type}/{args.taxi_type}_tripdata_{args.year}-{month:02d}.parquet")
        if raw_path.exists():
            out_dir = Path(f"../data/taxi_ingest_data/parts/{args.taxi_type}/{args.year}/{month:02d}/")
            print(f"{raw_path} -> {out_dir} in {args.num_partitions} parts")
            spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
            cast_schema(spark, raw_path, out_dir, args.num_partitions)
        else:
            raise FileNotFoundError(f"{raw_path} not found")
