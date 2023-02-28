#!/usr/bin/env python
# coding: utf-8

from pathlib import Path
import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import logging
from google.cloud import storage

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()


def retrieve(dataset_url: Path, data_dir: Path, taxi_type: str) -> Path:
    """Download taxi data from web

    requests.get(..., stream=True), and
    fstream.iter_content(chunk_size=...) streams the download files in chunks

    Returns local storage path
    """
    raw_path = Path(data_dir) / taxi_type / Path(dataset_url).name
    logger.info(f"streaming {dataset_url} to {raw_path}")
    if not raw_path.exists():
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        # os.system(f"wget {dataset_url} -O {raw_path}")
        with requests.get(dataset_url, stream=True) as fstream:
            fstream.raise_for_status()  # raises HTTPerror, if it occurs
            with open(raw_path, "wb") as f:
                for chunk in fstream.iter_content(chunk_size=10 * 1024):
                    if chunk:
                        f.write(chunk)
    else:
        logger.info(f"{raw_path} already exists; no download required")
    return raw_path


def read_parquet(local_path: str) -> pa.Table:
    """
    Reads parquet into pyarrow table for transformation

    Originally tried to read as df, but ran into a
    timestamp out of bounds error
    """
    tb = pq.read_table(local_path)
    logger.info(f"{len(tb)} rows loaded from {local_path}")
    return tb


def clean(tb_taxi: pa.Table, taxi_type: str = "fhv") -> pd.DataFrame:
    """
    Fix dtypes, e.g. datetimes, nulls

    Returns
    --------
    df_clean: pd.DataFrame
    """
    datetimes = [col for col in tb_taxi.column_names if "datetime" in col]
    logger.info(f"table dtypes:\n{tb_taxi.schema}")

    df_dts = pd.DataFrame()
    for col in datetimes:
        # handles OutOfBounds timestamps
        df_dts[col] = pd.to_datetime(tb_taxi.column(col), errors="coerce")

    # convert to pandas normally for other cols
    non_dts = [col for col in tb_taxi.column_names if col not in datetimes]
    df_taxi = tb_taxi.select(non_dts).to_pandas()

    # combine
    df_taxi = pd.concat([df_taxi, df_dts], axis=1)

    # cast to appropriate types
    fee_cols = [col for col in df_taxi.columns if "fee" in col.lower()]
    # Capital "F" for nullable float; pandas feature
    df_taxi[fee_cols] = df_taxi[fee_cols].astype("Float64", errors="ignore")

    if taxi_type != "fhv":
        # float, otherwise, due to nulls present
        df_taxi["passenger_count"] = df_taxi["passenger_count"].astype(
            "Int8", errors="ignore"
        )
    # flags, types and IDs are categorical; cast from numeric to string
    id_cols = [
        col
        for col in df_taxi.columns
        if "ID" in col or "type" in col.lower() or "flag" in col.lower()
    ]
    df_taxi[id_cols] = df_taxi[id_cols].astype("string", errors="ignore")
    logger.info(f"df casted to:\n{df_taxi.dtypes}")
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


def write_local(df: pd.DataFrame, fpath: Path):
    """Write the retrieved dataframe locally as parquet"""
    if not fpath.parent.exists():
        logger.info(f"creating data directory {fpath.parent.resolve()} first")
        fpath.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(fpath, compression="gzip")
    logger.info(f"Wrote recasted dataframe to {fpath}")


def upload_gcs(bucket_name: str, src_file: Path, dst_file: str, replace: bool = True):
    """
    Upload the local parquet file to GCS
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python

    Authentication woes:
    https://cloud.google.com/docs/authentication/client-libraries
    MUST SET UP APPLICATION DEFAULT CREDENTIALS FOR CLIENT LIBRARIES
    DOES NOT USE SAME CREDS AS GCLOUD AUTH
    USE gcloud auth application-default login

    To use service accounts (instead of user accounts),
    env var GOOGLE_APPLICATION_CREDENTIALS='/path/to/key.json'
    especially relevant for docker images, if they have fine-grain
    controlled permissions

    not required if you're on a credentialled GCE
    """
    logger.info(f"{bucket_name}: storage bucket\n{dst_file}: destination file")
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(dst_file)

    # set to zero to avoid overwrite
    try:
        blob.upload_from_filename(
            src_file,
            # if_generation_match=int(replace),
            timeout=90,
        )
    except Exception as e:
        logger.error(f"Error raised:\n{e}")
    logger.info(f"{src_file} uploaded to {dst_file}")


def web_gcs_parq(
    taxi_type: str,
    year: int,
    month: int,
    bucket_name: str,
    data_dir: str = "../data/cache",
) -> None:
    """Subflow to retrieve individual dataset parquet"""
    dataset_file = f"{taxi_type}_tripdata_{year}-{month:02}"
    dataset_url = (
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"
    )
    raw_path = retrieve(dataset_url=dataset_url, data_dir=data_dir, taxi_type=taxi_type)
    tb = read_parquet(raw_path)
    df_clean = clean(tb, taxi_type=taxi_type)
    fpath = Path(f"{data_dir}/staging/{taxi_type}/{year}-{month:02}.parquet")
    write_local(df_clean, fpath)
    # this is the relative path inside our bucket
    dst_file = f"data/{taxi_type}/{fpath.name}"
    upload_gcs(bucket_name, fpath, dst_file=dst_file)


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
        "--bucket-name",
        "-b",
        required=True,
        type=str,
        help="name of the GCS bucket",
    )
    parser.add_argument(
        "--loglevel",
        "-l",
        default="info",
        type=str.upper,
        help="log level: {DEBUG, INFO, WARNING, ERROR, CRITICAL}",
    )
    args = parser.parse_args()
    taxi_type = args.taxi_type
    year = args.year
    months = args.months
    data_dir = args.data_dir
    loglevel = args.loglevel
    if "-" in months:
        mth_start, mth_end = list(map(int, months.split("-")))
    else:
        mth_start = mth_end = int(months)
    months = list(range(mth_start, mth_end + 1))
    bucket_name = args.bucket_name

    loglevel_num = getattr(logging, loglevel)
    logger.setLevel(loglevel_num)
    ch.setLevel(loglevel_num)
    logger.addHandler(ch)

    for month in months:
        web_gcs_parq(taxi_type, year, month, bucket_name, data_dir)
