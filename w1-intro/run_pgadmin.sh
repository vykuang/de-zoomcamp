#! /usr/bin/env sh
# coding: utf-8
docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    -v /home/kohada/de-zoomcamp/data/pgadmin_config:/var/lib/pgadmin \
    --network pg-network \
    --name pgadmin \
    dpage/pgadmin4