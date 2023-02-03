# Week 1 - Setup for GCP, Docker, Terraform, Postgres

## Ingest csv to postgres

Set the whole `data/` directory as a mount for my `postgres` container; now it's got really strict permissions that prevents `black` and `git` commands.

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

    2: 1268  3: 254
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

## GCP Setup

1. Get GCP project ID - `de-zoom-376014` / `1075001006785` (project number)
    1. switch to project
    1. create `service account` in that project in IAM
        - service account includes permissions for all the components in that service
        - components may include cloud storage, big query, VM access, e tc.
        - can pick and choose permissions based on the components it needs
    1. Download the `.json` key for that service account
    1. `export GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json"`
        - so that's not for `gcloud`; `terraform` uses it to authenticate with GCP
        - if we're already running on a VM with the correct service account permissions, then it's not necessary
    1. `gcloud auth application-default login`
        - encountered this prompt:

        > The environment variable [GOOGLE_APPLICATION_CREDENTIALS] is set to:
        [/g/documents/google-cloud/de-zoom-376014-73df77142a5f.json]
        Credentials will still be generated to the default location:
        [/home/kohada/.config/gcloud/application_default_credentials.json]
        To use these credentials, unset this environment variable before
        running your application.
        Do you want to continue (Y/n)?

    - according to docs, `auth application-default` *manages your active app default credentials*
    - logging in without setting the env var above will lead to a browser login to ask for your *user creds*
    - [The vid also covers this](https://youtu.be/Hajwnmj0xfQ?t=773)
    - It's asking whether you want the newly set env var to replace the existing creds. Say yes
    - Authenticates your user ID via web flow
1. I think it's more straightforward to do the following:
    - `gcloud auth activate-service-account ...`
    - `gcloud init`
    - set a new config with the new project-id and the newly auth'd service account
    - no need to webflow auth
1. service account `de-zoom@de-zoom-376014.iam.gserviceaccount.com` will have the following roles:
    - storage admin - for buckets
    - storage object admin - for things inside buckets
    - bigquery admin - big query stuff
1. Add SSH key to compute engine > metadata
    - `ssh-keygen -t rsa -f gcp_de2 -C klang -b 2048`

### Setup VM

1. docker
1. terraform
1. pyenv 
    - `curl https://pyenv.run | bash`
    - edit `bashrc` and `profile`
    - [prepare build environment](https://github.com/pyenv/pyenv/wiki#suggested-build-environment)
    - install python-3.10.6
1. poetry
    - `poetry install` only recognized 3.10.6 after I uninstalled and re-installed
    - Ran into this thing:
    - `Current Python version is not allowed by the project`
    - When I ran `poetry shell` it created a venv but with the system defined 3.8.10, so when it saw the 3.10 in `.toml` it freaked
1. github CLI and github token

### New Trial

Start new trial to use the GCP VM instead

- Project ID - de-zoom-83
- `admin-110@de-zoom-83.iam.gserviceaccount.com`

### `sftp`

SSH File Transfer Protocol (sftp) allows transmission of files between cloud VM and local PC, e.g. our `credentials.json` so our VM has the permissions it needs

1. In local env, `cd my/local/dir`
1. `sftp <remote_ssh_host>`
1. Now you're on the remote host shell; `mkdir` and `cd` to whichever dir
1. `put <file_from_mylocaldir>` will send the file from local to remote host

[`sftp` has these commands](https://phoenixnap.com/kb/sftp-commands); most important is `put`

## Terraform

Infrastructure as code. Now we can version control the state of our cloud infra.

Only `main.tf` is required; rest are optional:

- variables
- resources
- output
- `.tfstate`

### `main.tf`

Four top level declarations: terraform, provider, resource, resource. Resource is doubled since we're defining storage bucket and bigquery dataset

1. `terraform`
    - backend is the storage choice for the infrastructure states
        - local vs s3 vs gcp storage
        - local will save a `.tfstate` file; could contain sensitive data in plain text
    - required_providers is optional, if we already have another top level declaration in `provider`
        - similar to `import ...` in python
1. `provider` - set some defaults for this infra
    - project
    - region
1. `resource` - meat of the `main.tf`. Define the stuff we want

The `var.<name>` denotes some variable defined in a separate file, in `variables.tf`, and so the `main.tf` can be configured by only changing the variables file, akin to `.env`.

Here we're creating two resources:

1. GCS bucket named `dtc_data_lake_<project_id>`
1. BQ dataset (their name for database) named `trips_data_all`

### `variables.tf`

Sets the env vars for our infra, e.g. project-id, region, bucket name, etc.

Only has `locals {...}` and `variable "var_name" { ... }` as top levels

### commands

- `terraform init` - initialize and install
- `terraform plan` - compares changes to previous state
    - `-out=path/to/tfplan` - saves the plan to a file, to be used by `apply`
- `terraform apply` - implements the changes on the cloud
- `terraform destroy` - teardown the infra

### State

TF records state of the managed infra and config in a `.tfstate` file to keep track of the metadata. It maps the named resources to an actual remote object, in light of the fact that not every provider supports tags.

Other metadata include *resource dependencies*. If we ask TF to delete a resource, TF must know the order to know how to delete it.

`terraform plan` queries the provider to get real-time status of state and update the `.tfstate` file to show what the real changes will be.

If `backend` set to `local`, creates a local tfstate; otherwise could get set `backend` to any cloud storage, e.g. `gcs` to store in a bucket.

Any changes made are first compared to the existing `.tfstate` 

#### State locking

If remote backend supports locking, terraform will lock the state for all ops that can write state, preventing *other terraform processes* from acquiring the lock, and corrupting the state. Only one process can hold this lock.

When I `terraform init` on a backend with a 6M retention policy, it threw a state lock error back:

```shell
Error loading state: <nil>
Additionally, unlocking the state file on Google Cloud Storage failed:

Error message: "googleapi: Error 403: Object 'tf-states-de-zoom/terraform/state/default.tflock' is subject to bucket's retention policy and cannot be deleted, overwritten or archived until 2023-08-03T22:52:52.086111-07:00, retentionPolicyNotMet
Lock Info:
ID:        1675057972084200
Path:      gs://tf-states-de-zoom/terraform/state/default.tflock
  Operation: init
  Who:       klang@de-zoom
  Version:   1.3.7
  Created:   2023-01-30 05:52:51.991151375 +0000 UTC
  Info:      
"
"
Lock ID (gen): 1675057972084200
Lock file URL: gs://tf-states-de-zoom/terraform/state/default.tflock

You may have to force-unlock this state in order to use it again.
The GCloud backend acquires a lock during initialization to ensure
the initial state file is created.
"
```

- Since I'm sure that no one else will be modifying the state, I can resolve with `terraform force-unlock <lock_ID>`
- It's also possible there is a hanging terraform process that actually does have the lock; `ps aus | grep terraform` and `sudo kill -9 <PID>` to remove the hanging process and free the lock

### Terraform on GCP

- The service account on our VM needs to have the correct permissions
- Otherwise set `$GOOGLE_APPLICATION_CREDENTIALS=path/to/key.json` env var for terraform to use

