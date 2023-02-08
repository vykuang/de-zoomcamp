FROM prefecthq/prefect:2.7.10-python3.10

RUN  apt-get update && apt-get install -y wget

RUN pip --disable-pip-version-check install gcsfs pandas pyarrow prefect_gcp

WORKDIR /app
COPY web_gcs_parq.py web_gcs_parq.py 

ENTRYPOINT [ "python", "web_gcs_parq.py" ]