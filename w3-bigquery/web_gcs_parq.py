#!/usr/bin/env python
# coding: utf-8

from pathlib import Path
import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect import get_run_logger


@task(retries=3)
def retrieve(dataset_url: Path, data_dir: Path, taxi_type: str) -> Path:
    """Download taxi data from web

    Set retries=3 to get around web traffic jitters

    Returns local storage path
    """
    logger = get_run_logger()
    fpath = Path(data_dir) / taxi_type / Path(dataset_url).name
    if not fpath.exists():
        fpath.parent.mkdir(parents=True, exist_ok=True)
        os.system(f"wget {dataset_url} -O {fpath}")
    else:
        logger.info(f"{fpath} already exists; no download required")
    return fpath


@task()
def read_parquet(local_path: str) -> pa.Table:
    """
    Reads parquet into pyarrow table for transformation

    Originally tried to read as df, but ran into a 
    timestamp out of bounds error
    """
    logger = get_run_logger()
    tb = pq.read_table(local_path)
    logger.info(f"{len(tb)} rows loaded from {local_path}")
    return tb


@task()
def clean(tb_taxi: pa.Table) -> pd.DataFrame:
    """
    Fix dtypes, e.g. datetimes, nulls

    Returns
    --------
    df_clean: pd.DataFrame
    """
    logger = get_run_logger()
    datetimes = [col for col in tb_taxi.column_names if "datetime" in col]
    logger.info(f"table dtypes:\n{tb_taxi.schema}")

    df_dts = pd.DataFrame()
    for col in datetimes:
        # handles OutOfBounds timestamps
        df_dts[col] = pd.to_datetime(tb_taxi.column(col), errors='coerce')
    
    # convert to pandas normally for other cols
    non_dts = [col for col in tb_taxi.column_names if col not in datetimes]
    df_taxi = tb_taxi.select(non_dts).to_pandas()
    # combine
    df_taxi = pd.concat([df_taxi, df_dts], axis=1)
    # cast to appropriate types
    # Capital 'I' in Int16 to use pandas integer type; allows NaN
    df_taxi['SR_Flag'] = df_taxi['SR_Flag'].astype('Int16', errors='ignore') 
    id_cols = [col for col in df_taxi.columns if "locationID" in col]
    df_taxi[id_cols] = df_taxi[id_cols].astype('Int32', errors='ignore')
    logger.info(f"df casted to:\n{df.dtypes}")
    # # remove zero pax rides
    # df_rm_empty = df_taxi[df_taxi["passenger_count"] > 0]
    # logger.info(f"{len(df_taxi) - len(df_rm_empty)} rides with zero pax removed")

    # # remove zero distance
    # df_rm_zero = df_rm_empty[df_rm_empty["trip_distance"] > 0]
    # logger.info(
    #     f"{len(df_rm_empty) - len(df_rm_zero)} rides with zero distance removed"
    # )
    # logger.info(f"{len(df_rm_zero)} rows saved")
    return df_taxi


@task()
def write_local(df: pd.DataFrame, fpath: Path) -> Path:
    """Write the retrieved dataframe locally as parquet"""
    logger = get_run_logger()
    if not fpath.parent.exists():
        logger.info(f"creating data directory {fpath.parent.resolve()} first")
        fpath.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(fpath, compression="gzip")
    return fpath


@task(retries=3)
def upload_gcs(block_name: str, fpath: Path, replace: bool = True) -> None:
    """Upload the local parquet file to GCS"""
    logger = get_run_logger()
    gcs_block = GcsBucket.load(block_name)
    gcs_dir = Path(fpath.parts[-2])
    fname = fpath.parts[-1]
    # this will return <color>/<filename>.parquet
    gcs_path = gcs_dir / fname
    # check if already exists:
    blobs = list(map(str, gcs_block.list_blobs(gcs_dir)))
    contents = [[fn for fn in blob.split() if "/" in fn][0] for blob in blobs]
    logger.info(f"{gcs_dir} contents:\n{blobs}")
    logger.info(f"{fname} in above?")
    present = [fname in name for name in contents]
    if any(present) and not replace:
        logger.info(f"{fname} already exists; no upload required")
    else:
        gcs_block.upload_from_path(from_path=fpath, to_path=gcs_path)
        logger.info(f"{gcs_path} uploaded")
    return


@flow()
def _web_gcs_parq(
    taxi_type: str,
    year: int,
    month: int,
    block_name: str,
    data_dir: str = "../data/cache",
) -> None:
    """Subflow to retrieve individual dataset parquet"""
    dataset_file = f"{taxi_type}_tripdata_{year}-{month:02}"
    dataset_url = (
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"
    )
    raw_path = retrieve(dataset_url=dataset_url, data_dir=data_dir, taxi_type=taxi_type)
    tb = read_parquet(raw_path)
    fpath = Path(f"{data_dir}/staging/{taxi_type}/{year}-{month:02}.parquet")
    # if not fpath.exists():
    #     df 
    df_clean = clean(tb)
    fpath = write_local(df_clean, fpath)
    upload_gcs(block_name, fpath)


@flow()
def web_gcs_parq(
    taxi_type: str,
    year: int,
    months: list[int],
    block_name: str,
    data_dir: str = "../data/cache",
) -> None:
    """Wrapper function to fetch multiple months"""
    for month in months:
        _web_gcs_parq(taxi_type, year, month, block_name, data_dir=data_dir)


if __name__ == "__main__":
    # "parametrization"
    parser = argparse.ArgumentParser(description="Load NYC taxi parquets to GCS")

    parser.add_argument(
        "--taxi-type", "-t", required=True, type=str, help="{'yellow', 'green', 'fhv'}"
    )
    parser.add_argument("--year", "-y", required=True, type=int, help="yyyy")
    parser.add_argument(
        "--months",
        "-m",
        required=True,
        type=str,
        help="m_start-m_end, e.g. 1-12, or just 12 for a single month",
    )
    parser.add_argument(
        "--data-dir",
        "-d",
        default="../data/taxi_ingest_data",
        type=str,
        help="local path for intermediate storage",
    )
    parser.add_argument(
        "--block_name",
        "-b",
        default="ny-taxi-gcs",
        type=str,
        help="name of the prefect GCS storage block",
    )
    args = parser.parse_args()
    taxi_type = args.taxi_type
    year = args.year
    months = args.months
    data_dir = args.data_dir
    if "-" in months:
        mth_start, mth_end = list(map(int, months.split("-")))
    else:
        mth_start = mth_end = int(months)
    month = list(range(mth_start, mth_end + 1))
    block_name = args.block_name
    web_gcs_parq(taxi_type, year, month, block_name, data_dir)
