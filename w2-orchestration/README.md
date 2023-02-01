# Workflow Orchestration

## Data lake

- Ingests both structured *and unstructured data*
- secure
- scales to handle the sheer size
- catalogs and indexes for analyses without data movement???
- connects data with analytics and ML tools

### Lake vs Warehouse

- unstructured, semi-structured, structured vs cleaned, refined, pre-processed data
  - logs
  - sensor data
- Petabyte vs Terabytes
  - stored indefinitely
  - transformed only when in use
- undefined use-cases for data in lakes; WH store only relational data

### Gotchas

- swampy due to sheer size and unwieldiness
  - hard to be useful
  - why? no versioning
  - incompatible schemas and formats
- no metadata will instantly make all data useless
- no `JOIN`s available, if there is no compatible foreign key

### ETL vs ELT

- ETL meant for smaller amount of data; ELT for larger
- ELT provides data lake support (schema on read)
  - write to the lake first, *then* determine the schema

### Cloud Providers

- GCP cloud storage
- AWS S3
- Azure blob

## Intro to Workflow Orchestration

Three main components:

1. Workflow - the main framework; what needs to be done
  - Buy items
  - Deliver to address
1. Configuration
  - Order in which to buy items; all at once, or sequential?
  - type of packaging - docker or k8
  - type of delivery - single/concurrent thread?
1. Orchestration
  - scheduling
  - what happens if deliveries fail?
  - conditions for restarts?
  - monitoring and metadata - how long did delivery take? when will it complete? how many times has this task failed?

*scalable and available*

## Env Setup

- add the list of requirements to `pyproject.toml`
- this will become out of sync with the lock. `poetry lock --no-update` to sync the lock file again
  - otherwise it'll throw a non-sensical error that it doesn't recognize the version spec of the last dependency
- `poetry install --no-root`

Packages:

```
prefect = "~2.7.7"
prefect-sqlalchemy = "~0.2.2"
protobuf = "~4.21.11"
pyarrow = "~10.0.1"
pandas-gbq = "^0.18.1"
prefect-gcp[cloud_storage] = "^0.2.4"
```

## Prefect

### Turning our `ingest_data.py` into a flow

- `from prefect import flow, task`
- add `@flow` and `@task` decorator to our main flow and subtasks
- `from prefect.tasks import task_input_hash` - allows tasks with heavy computation to cache results and improve performance. Use in task decorator.
  - applicable most in our `extract`, if the source file hasn't changed
  - set `cache_expiration=datetime.timedelta(days=1)` to set retention policy
- break `ingest_data` into three smaller, more focused tasks:
    - convert `extract` into generator
    - `transform` will convert datetimes and remove trips with zero passengers
    - `load` will `UPSERT` the data into postgres
    - the ETL will repeat until `extract` runs out

### Create Prefect GCP blocks

Blocks are how prefect store configurations, as a way to quickly interface with external systems. They can store

- cloud credentials
- cloud storage config, e.g. GCS, S3
- database connections

Saving these in `blocks` allow other flows to make use of them, if they also have access to your prefect API. Connection info and other credentials no longer need to be inside our scripts

Blocks could be *nested*. GCS block could make use of GCP credential block to obtain the necessary permissions for their API

after installing `prefect-gcp[cloud_storage]`, some pre-made blocks are ready for registering. Register with `prefect block register -m prefect_gcp`. This makes the block templates available for creation.

Create and edit these blocks in the Orion UI. Access UI by `prefect orion start`. Need to forward port 4200. Can do so in VS Code if already SSH'd

1. Create `SQLAlchemyConnector` block named `pg-connector` - this replaces our manual engine creation inside the flow, and instead grabs the saved configuration from the prefect server

    ```py
    from prefect_sqlalchemy import SqlAlchemyConnector

    with SqlAlchemyConnector.load("pg-connector") as database_block:
        engine = database_block.get_engine() # use same way as create_engine()
    ```

1. Create GCP credentials block `de-zoom-key`
  - ideally create a new service account with only the permissions it needs
    - bigquery
    - cloud storage
  - video copied the entire JSON content into the `service account info` box, as a dict
  - provide path to the service account json
  - Can also do it via code, but doesn't that much easier/better
  - use this credential block in our flow:
  ```py
  from prefect_gcp import GcpCredentials
  gcp_credentials_block = GcpCredentials.load("de-zoom-key")
  ```
  - since the service account on my VM already includes those permission, I can get by with having no credential in the GCS block
1. Create GCS block
  - name it; this is local to prefect
  - map it to an existing bucket
  - optional - put default directory within the bucket, for whomever references this bucket block
    - use `/data`

### Upload from GCS to BigQuery

[Docs for prefect_gcp blocks](https://prefecthq.github.io/prefect-gcp/)

- `gcp_block.download_object_to_path` or `.get_directory` to download whole folder
- `gcp_cred_block.get_credentials_from_service_account()` to access the credentials
- `from prefect import get_run_logger` to log without `print`
- `df.to_gbq()` uploads straight to bigquery `table`. That means we first create the `dataset.table`; dataset in bq is akin to a `database` in postgres, or any other DB
  - setting `credentials=` will override default creds, e.g. if your VM has its own service acccount creds

### Flow Parametrization and Deployment

Flow can be parametrized at deployment

Deployment can be built two ways: via python script or via CLI into `yaml`. I distinctly remember the latter being much simpler, so let's do that.

Given our flow `etl_web_gcs.py`, use 

```bash
prefect deployment build ./etl_web_gcs.py:etl_parent_flow \
    -n "etl-web-gcs" \
    -q default \
    --output web-gcs-deployment.yaml
```

All it does is create a `.yaml`. This yaml specs our deployment with:

- name: parametrized ny-taxi ETL
- work queue: default - directs it to the "default" work queue

We can edit the `yaml` to include the parameters we want to specify, i.e. `color`, `year`, etc: `{"color": "yellow", "months" :[1, 2, 3], "year": 2021, "block_name": "ny-taxi-gcs"}`. Other specs:

- schedule
- infra

Then, `prefect deployment apply web-gcs-deployment.yaml` registers the deployment on prefect server, terminal will show that the `default` work queue now has this deployment, and an agent subscribed to that work queue will be able to pull it and execute.

Start a deployment run manually either through UI or `prefect deployment run "etl-parent-flow/parametrized ny-taxi ETL"`, then try locally with `prefect agent start -q 'default'`

### Work Queues and Agents

Deployments are sent to work queues, and agents subscribe to the specified queues to run those jobs

Agent infra needs to fulfill the requirements of the jobs in the queues. Containerize these agents.

### Notifications

From UI, notifications can be triggered absed on flow run states

webhooks exist for

- teams
- slack
- twilio
- opsgenie

`collections` module allows notification to be coded as well

### Scheduling

### Prefect Cloud

Let's set up shop here instead.

## Homework

1. green-jan-2020: 447,770 rows
1. cron: 0 5 1 * *
1. load to bq, without any transformations, yellow taxi data for feb-mar 2019. total rows = 
1. github storage block to store flow code for `etl_web_gcs`, and process taxi data for `green-nov-2020`. total rows = 
1. host prefect cloud and set up notification. run `etl_web_gcs` code from above for `green-apr-2019`. Send slack when flow is `completed`. 
  - cloud account still intact
  - webhook: https://hooks.slack.com/services/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
  - try `testing-notifications` channel
  - total rows = 
1. secret block - how many `*` shown in UI?