# Week 1 - Setup for GCP, Docker, Terraform, Postgres

## Ingest csv to postgres

Set the whole `data/` directory as a mount for my `postgres` container; how it's got really strict permissions that prevents `black` and `git` commands.

- `sudo chmod -R 770 data/` to restore read/write permission

pandas has a tool that can read the header row of a csv and create the table schema in SQL

### Connect pgAdmin to Postgres

- `docker network create pg-network`
- when `docker run`ing the docker images for pgAdmin and postgres, specify `--network=pg-network` and `--name=<host_name>` for easier connection later on, instead of finding out the explicit IPs and typing in a bunch of numbers
- when connecting pgAdmin to a server, 
  - name could be anything
  - Under `Connection` is where the important stuff is
  - Host name is what we specified in `--name` for the postgres container
  - username and password is also the same as what we set when we `docker run` postgres

### run locally

Before running in a python container, run locally on venv python

```bash
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL}
```

### dockerize

- `RUN apt-get install wget` was not possible in `3.9-slim`; only default `3.9` allowed it.
- build as `taxi_ingest:v001`

How to mount the data file so that it doesn't need to redownload...? Add a `--data_dir` parameter to our `ingest_data.py` script, and check for the file name there

When running as a script, the `argparse` parameters simply follow the image name; no need to use `CMD` in the dockerfile:

```bash
docker run -it \
  --network=pg-network \
  -v /home/kohada/de-zoomcamp/data/taxi_ingest_data:/data \
  taxi_ingest:v001 \
    --user=root \ # argparse params start here
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL} \
    --data_dir=${DATA_DIR}
```

when assigning `pgadmin` its own volume, provide permission for the pgadmin container to read and write from the mount directory. From [docs](https://www.pgadmin.org/docs/pgadmin4/6.15/container_deployment.html#mapped-files-and-directories):

> Warning: pgAdmin runs as the pgadmin user (UID: 5050) in the pgadmin group (GID: 5050) in the container. You must ensure that all files are readable, and where necessary (e.g. the working/session directory) writeable for this user on the host machine. For example: `sudo chown -R 5050:5050 <host_directory>`

Otherwise pgadmin won't load, and browser can't connect to the client

### docker compose

- Use the network and names that have already been specified in the earlier `docker run` commands.
  - to use a pre-existing network from `docker network create ...`, set `name` and `external: true`
  - otherwise compose will create an app specific network called `[projectname]_default`, which is what we need to specify in our `run_docker_digest.sh`
- set `attachable: true` so that our dockerized ingestion script and connect to it and use our specified network alias from docker compose

## Homework

1. `--iidfile string` writes image ID to file
1. `python-3.9` has 3 packages installed
1. in sql:

    ```sql
    SELECT
	COUNT(1)
    FROM green_taxi_data
    WHERE
	lpep_pickup_datetime >= '2019-01-15 00:00:00' AND lpep_dropoff_datetime <= '2019-01-15 23:59:59'
    ```

    Datetime requires `HH:MM:SS` specification, not just the date
    ans: 20530
1. Find max trip distance using GROUP BY and ORDER BY:

    ```sql
    SELECT
 	MAX(trip_distance) AS "trip_distance",
	CAST(lpep_dropoff_datetime AS DATE) AS "day"
    FROM green_taxi_data g
    GROUP BY CAST(lpep_dropoff_datetime AS DATE)
    ORDER BY MAX(trip_distance) DESC
    ```

    ans: 1-15
1. num pax = 2, and 3, on 01-01:

    ```sql
    SELECT
        COUNT(1)
    -- 	CAST(lpep_dropoff_datetime AS DATE) AS "dropoff",
    -- 	CAST(lpep_pickup_datetime AS DATE) AS "pickup"
    FROM green_taxi_data g
    WHERE
        <!-- CAST(lpep_dropoff_datetime AS DATE) = '2019-01-01' AND -->
        CAST(lpep_pickup_datetime AS DATE) = '2019-01-01' AND
        (passenger_count = 3)
    ```

    2: 12682  3: 254
1. largest tip where pick up was in zone 'Astoria':

    ```sql
    SELECT
        MAX(tip_amount),
        zdo."Zone"
    FROM 
        green_taxi_data g 
        JOIN zones zpu
            ON g."PULocationID" = zpu."LocationID"
        JOIN zones zdo
            ON g."DOLocationID" = zdo."LocationID"
    WHERE zpu."Zone" = 'Astoria'
    GROUP BY zdo."Zone"
    ORDER BY MAX(tip_amount) DESC
    ```

    Always use single quote for string literals. Double quotes refer to column names

## Terraform and GCP

1. Get GCP project ID
1. git clone my repo
1. run the terraform files on the gcloud terminal