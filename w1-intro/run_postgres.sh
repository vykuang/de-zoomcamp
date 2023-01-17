#! /usr/bin/env sh
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v /home/kohada/de-zoomcamp/data:/var/lib/postgresql/taxi_data \
    -p 5432:5432 \
    postgres:10