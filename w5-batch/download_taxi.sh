#! /usr/bin/env sh

# if any wget exits with nonzero code, interrupt the script
set -e
TAXI_TYPE=$1
YEAR=$2

LOCAL_PREFIX="~/de-zoomcamp/data/taxi_ingest_data/${TAXI_TYPE}/${YEAR}"
echo "Making directory ${LOCAL_PREFIX}"
# mkdir -p $LOCAL_PREFIX
# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"
# downloads the entire year given taxi type and year
for MONTH in {1..12}; do
    # format it to 01, 02, ...
    # BACKTICK, NOT SINGLE QUOTE
    FMONTH=`printf "%02d" ${MONTH}`
    # echo $FMONTH
    URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"
    # echo $URL
    LOCAL_FILE="${TAXI_TYPE}-${YEAR}-${FMONTH}.parquet"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"
    echo "Downloading ${URL} to ${LOCAL_PATH}"
    # echo wget ${URL} -o ${LOCAL_PATH}

    echo "Compressing ${LOCAL_PATH}"
    # use zcat command to preview gzipped files
    # gzip ${LOCAL_PATH}
done