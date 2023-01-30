#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from pathlib import Path
from time import time

import pandas as pd
from sqlalchemy import create_engine

from prefect import flow, task
from prefect.tasks import task_input_hash

from datetime import timedelta

@task(log_prints=True, tags=["extract"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract(csv_uri: str, data_dir: str, chunksize: int = 1000000) -> pd.DataFrame:
    """
    Extracts the CSV and yield a dataframe in chunks
    """
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    csv_name = Path(data_dir) / Path(csv_uri).name
    if not csv_name.exists():
        os.system(f"wget {csv_uri} -O {csv_name}")
    else:
        print(f"{csv_name} already exists; no download required")

    # specifying iterator and chunksize returns ... an iterator in those chunks
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=chunksize)
    for df in df_iter:
        yield df

@task(log_prints=True)
def transform(df_taxi: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the taxi data
    e.g. look for taxi trips with zero passengers and remove them
    """

@task(log_prints=True, retries=3)
def load(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    

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

@flow(name="Ingest Taxi Data Flow")
def main_flow():
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

    raw_data = extract()
    data = transform(raw_data)

    ingest_data(args)

if __name__ == "__main__":
    main_flow()    
