#!/usr/bin/env python
# coding: utf-8
"""
Create a GCS bucket block from code
Note that these blocks need to be already registered in the CLI via

prefect register -m prefect_gcp

"""
from prefect_gcp.cloud_storage import GcsBucket

# from prefect_gcp import GcpCredentials

# alternative to creating GCP blocks in the UI
# insert your own service_account_file path or service_account_info dictionary from the json file
# IMPORTANT - do not store credentials in a publicly available repository!

# credentials_block = GcpCredentials(
#     service_account_info={}  # enter your credentials info or use the file method.
# )
# credentials_block.save("de-zoom-gcs-creds", overwrite=True)

bucket_block = GcsBucket(
    # gcp_credentials=GcpCredentials.load("de-zoom-gcs-creds"),
    bucket="dtc_data_lake_de-zoom-83",  # insert your  GCS bucket name
    bucket_folder="data",
)

bucket_block.save("ny-taxi-gcs", overwrite=True)
