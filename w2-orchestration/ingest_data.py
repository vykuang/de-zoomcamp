#!/usr/bin/env python
# coding: utf-8

import os
from pathlib import Path
from time import time

import pandas as pd
from sqlalchemy import create_engine

from prefect import flow, task
from prefect.tasks import task_input_hash

from datetime import timedelta

@task(log_prints=True, tags=["extract"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract(csv_uri: str, data_dir: str, chunksize: int = 100000) -> pd.DataFrame:
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

@task(log_prints=True, tags=["transform"])
def transform(df_taxi: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the taxi data
    e.g. look for taxi trips with zero passengers and remove them,
    transform str to datetimes
    """
    datetimes = [col for col in df_taxi.columns if "datetime" in col]
    for col in datetimes:
        df_taxi[col] = pd.to_datetime(df_taxi[col])

    # remove zero pax rides
    df_rm_empty = df_taxi[df_taxi["passenger_count"] > 0]
    print(f"{len(df_taxi) - len(df_rm_empty)} rides with zero pax removed")

    # remove zero distance
    df_rm_zero = df_rm_empty[df_rm_empty["trip_distance"] > 0]
    print(f"{len(df_rm_empty) - len(df_rm_zero)} rides with zero distance removed ")
    return df_rm_zero

@task(log_prints=True, tags=["load"], retries=3)
def load(df_taxi: pd.DataFrame, engine, table_name: str):
    """
    Loads the transformed data into postgres using the SQLAlchemy Engine
    """   
    t_start = time()
    num_insert = df_taxi.to_sql(name=table_name, con=engine, if_exists="append", index=False, chunksize=60000)
    t_end = time()
    print(f"{num_insert} rows inserted, took {t_end - t_start:.3f} second")

@flow(name="Ingest Taxi Data Flow")
def main_flow():
    csv_uri = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    data_dir = "../data/taxi_ingest_data"
    user = "root"
    password = "root"
    host = "localhost"
    port = 5432
    db = "ny_taxi"
    table_name = "yellow_taxi_trips"

    # sqlalchemy
    con_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(con_str)

    raw_data = extract(csv_uri=csv_uri, data_dir=data_dir)
    for raw_chunk in raw_data:
        chunk = transform(raw_chunk)
        load(chunk, engine=engine, table_name=table_name)
    print("Finished ingesting data into the postgres database")

if __name__ == "__main__":
    main_flow()    
