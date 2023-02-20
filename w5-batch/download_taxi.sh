#! /usr/bin/env sh

# if any wget exits with nonzero code, interrupt the script
set -e
TAXI_TYPE=$1
YEAR=$2

LOCAL_PREFIX="${HOME}/de-zoomcamp/data/taxi_ingest_data/raw/${TAXI_TYPE}"
echo "Making directory ${LOCAL_PREFIX}"
mkdir -p $LOCAL_PREFIX
# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"
# downloads the entire year given taxi type and year
for MONTH in {1..2}; do
    # format it to 01, 02, ...
    # BACKTICK, NOT SINGLE QUOTE
    FMONTH=`printf "%02d" ${MONTH}`
    # echo $FMONTH
    URL_SUFFIX="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"
    URL="${URL_PREFIX}/${URL_SUFFIX}"
    # echo $URL
    # LOCAL_FILE="${TAXI_TYPE}-${YEAR}-${FMONTH}.parquet"
    LOCAL_PATH="${LOCAL_PREFIX}/${URL_SUFFIX}"
    # if we're downloading parquets, don't need to compress anymore
    # LOCAL_GZIP="${LOCAL_PATH}.gz"
    # check if compressed file already exists
    # echo "$LOCAL_GZIP here?"
    # if test -e "$LOCAL_GZIP"; then
    #     echo "$LOCAL_GZIP already exists"
    # else
    #     echo "$LOCAL_GZIP not here. $LOCAL_PATH here?"
    # check if uncompressed LOCAL_PATH exists:
    if test -e "$LOCAL_PATH"; then
        echo "$LOCAL_PATH already exists"
    else
        echo "Downloading ${URL} to ${LOCAL_PATH}"
        wget ${URL} -O ${LOCAL_PATH}
    fi

        # echo "Compressing ${LOCAL_PATH}"
        # use zcat command to preview gzipped files
        # gzip is preferred b/c pandas can read it as is, without unpacking
        # gzip -9 ${LOCAL_PATH}
    # fi
done