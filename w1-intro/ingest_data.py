#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from pathlib import Path
from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    data_dir = params.data_dir
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file

    csv_name = Path(data_dir) / Path(url).name
    if not csv_name.exists():
        os.system(f"wget {url} -O {csv_name}")
    else:
        print(f"{csv_name} already exists; no download required")

    # sqlalchemy
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # specify n=0 to get only column names;
    # Writing only column names to sql basically creates the table with
    # the inferred schema for us automatically
    df_top = pd.read_csv(csv_name, nrows=100)
    if "green" in str(csv_name):
        pickup_dt = "lpep_pickup_datetime"
        dropoff_dt = "lpep_dropoff_datetime"
    elif "yellow" in str(csv_name):
        pickup_dt = "tpep_pickup_datetime"
        dropoff_dt = "tpep_dropoff_datetime"
    else:
        pickup_dt = ""
        dropoff_dt = ""

    df_top[pickup_dt] = pd.to_datetime(df_top[pickup_dt])
    df_top[dropoff_dt] = pd.to_datetime(df_top[dropoff_dt])
    df_top.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    # specifying iterator and chunksize returns ... an iterator in those chunks
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    while True:

        try:
            t_start = time()
            # get the next chunk
            df = next(df_iter)

            df[pickup_dt] = pd.to_datetime(df[pickup_dt])
            df[dropoff_dt] = pd.to_datetime(df[dropoff_dt])

            df.to_sql(name=table_name, con=engine, if_exists="append")

            t_end = time()

            print("inserted another chunk, took %.3f second" % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument("--user", required=True, help="user name for postgres")
    parser.add_argument("--password", required=True, help="password for postgres")
    parser.add_argument("--host", required=True, help="host for postgres")
    parser.add_argument("--port", required=True, help="port for postgres")
    parser.add_argument("--db", required=True, help="database name for postgres")
    parser.add_argument(
        "--table_name",
        required=True,
        help="name of the table where we will write the results to",
    )
    parser.add_argument("--url", required=True, help="url of the csv file")
    parser.add_argument(
        "--data_dir", required=True, help="output directory of downloaded file"
    )

    args = parser.parse_args()

    main(args)
