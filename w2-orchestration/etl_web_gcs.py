#!/usr/bin/env python
# coding: utf-8

from pathlib import Path

import pandas as pd


from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect import get_run_logger


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web and return as dataframe

    Set retries=3 to get around web traffic jitters
    """
    logger = get_run_logger()
    df = pd.read_csv(dataset_url)
    logger.info(f"{len(df)} rows loaded from url")
    return df


@task()
def clean(df_taxi: pd.DataFrame) -> pd.DataFrame:
    """
    Fix dtypes, e.g. datetimes
    """
    logger = get_run_logger()
    datetimes = [col for col in df_taxi.columns if "datetime" in col]
    for col in datetimes:
        df_taxi[col] = pd.to_datetime(df_taxi[col])
    logger.info(f"table dtypes:\n{df_taxi.dtypes}")
    # # remove zero pax rides
    # df_rm_empty = df_taxi[df_taxi["passenger_count"] > 0]
    # logger.info(f"{len(df_taxi) - len(df_rm_empty)} rides with zero pax removed")

    # # remove zero distance
    # df_rm_zero = df_rm_empty[df_rm_empty["trip_distance"] > 0]
    # logger.info(f"{len(df_rm_empty) - len(df_rm_zero)} rides with zero distance removed")
    # logger.info(f"{len(df_rm_zero)} rows saved")
    return df_taxi


@task()
def write_local(df: pd.DataFrame, fpath: Path) -> Path:
    """Write the retrieved dataframe locally as parquet"""
    logger = get_run_logger()
    if not fpath.parent.exists():
        logger.info(f"creating data directory {fpath.parent.resolve()} first")
        fpath.parent.mkdir(parents=True)
    df.to_parquet(fpath, compression="gzip")
    return fpath


@task()
def upload_gcs(block_name: str, fpath: Path) -> None:
    """Upload the local parquet file to GCS"""
    gcs_block = GcsBucket.load(block_name)
    # this will return <color>/<filename>.parquet
    gcs_path = Path(fpath.parts[-2]) / fpath.parts[-1]
    gcs_block.upload_from_path(from_path=fpath, to_path=gcs_path)
    return


@flow()
def etl_web_gcs(
    color: str, year: int, month: int, block_name: str, data_dir: str = "../data/cache"
) -> None:
    """Main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    fpath = Path(f"{data_dir}/{color}/{dataset_file}.parquet")
    if not fpath.exists():
        df = fetch(dataset_url)
        df_clean = clean(df)
        fpath = write_local(df_clean, fpath)
    upload_gcs(block_name, fpath)


@flow()
def etl_parent_flow(
    color: str,
    year: int,
    months: list[int],
    block_name: str,
    data_dir: str = "../data/cache",
) -> None:
    """Wrapper function to fetch multiple months"""
    for month in months:
        etl_web_gcs(color, year, month, block_name)


if __name__ == "__main__":
    # "parametrization"
    color = "yellow"
    year = 2021
    month = list(range(1, 4))
    block_name = "ny-taxi-gcs"
    etl_parent_flow(color, year, month, block_name)
