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
    match raw_path.suffixes:
        case [".parquet"] | [_, ".parquet"]:
            df = spark_client.read.option("header", "true").parquet(str(raw_path))
        case [".csv"] | [".csv", _]:
            df = spark_client.read.option("header", "true").csv(str(raw_path))
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
    df.repartition(num_parts).write.parquet(str(parts_dir), mode="overwrite")
    return df


if __name__ == "__main__":
    parser = ArgumentParser()
    # returns the func to opt
    opt = parser.add_argument
    # call the func
    opt(
        "-t",
        "--taxi_type",
        required=True,
        type=str,
        help="{fhv, fhvhv, yellow, green}",
    )
    opt("-y", "--year", required=True, type=int, help="year for the dataset")
    opt(
        "-m",
        "--month",
        default=0,
        type=int,
        help="month for dataset; if specified, only this month will be read and partitioned",
    )
    opt("-n", "--num_partitions", default=24, type=int, help="Number of partitions")
    opt(
        "-d",
        "--data_dir",
        default="../data/taxi_ingest_data/raw",
        type=Path,
        help="dir to look for <taxi_type>/*.datasets",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    if args.month >= 1 and args.month <= 12:
        p = f"*{args.year}-{args.month:02d}*"
        fpaths = list((args.data_dir / args.taxi_type).glob(p))
        if fpaths and (num_files := len(fpaths)) > 1:
            raise ValueError(f"{num_files} with specified dates found")
        else:
            out_dir = Path(
                f"../data/taxi_ingest_data/parts/{args.taxi_type}/{args.year}/{args.month:02d}/"
            )
            print(f"{fpaths[0]} -> {out_dir} in {args.num_partitions} parts")
            cast_schema(spark, fpaths[0], out_dir, args.num_partitions)
    else:
        for month in range(1, 13):
            raw_path = Path(
                f"{args.data_dir}/{args.taxi_type}/{args.taxi_type}_tripdata_{args.year}-{month:02d}.parquet"
            )
            if raw_path.exists():
                out_dir = Path(
                    f"../data/taxi_ingest_data/parts/{args.taxi_type}/{args.year}/{month:02d}/"
                )
                print(f"{raw_path} -> {out_dir} in {args.num_partitions} parts")
                cast_schema(spark, raw_path, out_dir, args.num_partitions)
            else:
                raise FileNotFoundError(f"{raw_path} not found")
