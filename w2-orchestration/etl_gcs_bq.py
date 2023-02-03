#!/usr/bin/env python
# coding: utf-8

from pathlib import Path

import pandas as pd


from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect import get_run_logger


@task(retries=3)
def extract_from_gcs(
    color: str,
    year: int,
    month: int,
    gcs_block: str = "ny-taxi-gcs",
    cache_dir: str = "../data/bq_cache",
):
    """Loads the bucket block and retrieves file paths for the specified
    taxi types and times
    """
    logger = get_run_logger()
    gcs_path = f"{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load(gcs_block)
    # .get_directory() copies the whole folder content to local path
    # docs: https://prefecthq.github.io/prefect-gcp/cloud_storage/#prefect_gcp.cloud_storage.GcsBucket.get_directory
    # perhaps download_object is more appropriate
    local_path = Path(cache_dir) / gcs_path
    if not local_path.exists():
        local_path.parent.mkdir(parents=True, exist_ok=True)
        gcs_block.download_object_to_path(from_path=gcs_path, to_path=local_path)
    else:
        logger.info(f"{local_path} already exists; no download")
    return local_path


@task()
def transform(fpath: Path) -> pd.DataFrame:
    """Data cleaning"""
    logger = get_run_logger()
    df = pd.read_parquet(fpath)
    logger.info(f"{len(df)} rows read from {fpath}")
    # assume missing passenger count means a count of zero
    df_clean = df.copy()
    logger.info(
        f"{df['passenger_count'].isna().sum()} missing pax counts; fill with zero"
    )
    df_clean["passenger_count"] = df["passenger_count"].fillna(0)
    return df_clean


@task()
def upload_bq(df: pd.DataFrame, project_id: str = "de-zoom-83"):
    """Upload dataframe to bigquery dataset"""
    # gcp_cred_block = GcpCredentials.load("")

    df.to_gbq(
        destination_table="trips_data_all.yellow_taxi_trips",  # "<dataset>.<table>" format
        project_id=project_id,
        # credentials=gcp_cred_block.get_credentials_from_service_account(), # use VM cred
        chunksize=50000,
        if_exists="append",
    )


@flow(name="taxi-gcs-bq")
def gcs_to_bq(
    year: int = 2021,
    color: str = "yellow",
    months: list[int] = [1],
    gcs_block: str = "ny-taxi-gcs",
):
    """Main flow to load data from cloud storage to BigQuery"""
    logger = get_run_logger()
    fpaths = [
        extract_from_gcs(color, year, month, gcs_block=gcs_block) for month in months
    ]
    logger.info(f"paths collected:\n{fpaths}")
    for fpath in fpaths:
        df = transform(fpath)
        upload_bq(df)
    # path =


if __name__ == "__main__":
    # "parametrization"
    color = "yellow"
    year = 2021
    month = [1]
    gcs_block = "ny-taxi-gcs"
    gcs_to_bq(color, year, month, gcs_block)
