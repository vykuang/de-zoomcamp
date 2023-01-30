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