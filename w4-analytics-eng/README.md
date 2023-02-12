# Week 4 - Analytics Engineering

## AE Basics

### Intro

AE is the intersection between DE and Data Analysts

- DEs have the software eng background
  - preparing the infrastructure and pipeline
  - lack business perspective
- DAs have the analytics background
  - data users to solve business problems
  - lack SWE background
- AE tries to fill the gap in the data team

### Modeling concepts

ETL:

- sources -> transform -> warehouse
- more stable, data compliant

ELT:

- sources -> load to lake -> transform
- enabled by lower storage and compute costs
- faster turnaround

### Dimensional modeling

Fact tables

- measurements
- process
- *verbs*

Dimenion tables

- provide contexts
- *nouns*

Architecture:

- Stage
  - contains raw data
  - limited exposure
- processing
  - from raw data to model
  - emphasis on efficiency and ensuring standards
- presentation
  - final presentation of data
  - exposure to stakeholders

## dbt

**Data Build Tool** is a SQL based transformation tool that leverages SWE best practices

- modularity
- portability
- version control
- CI/CD
- documentation

### How it works

Adds a modeling layer apart from DWH

- dbt model is a \*.sql file, a la `SELECT ...`
- not DDL/DML, e.g. `CREATE` or `LOAD`
- models the data from DWH
- separate from DWH
- persists the model back into the DWH
  - the query result from the model `.sql`

### How to use

dbt Core

- open source
- builds and runs dbt project
  - comprised of `.sql` and `yml`
- incl. SQL compilation logic, macros (functions), and db adapters
- CLI to run dbt cmds locally

dbt Cloud

- SaaS
- web-based IDE
- orchestration
  - logging and alerts
- integrated docs
- free for one dev

### Integration with Bigquery

dbt Cloud will integrate with Bigquery; no local dbt core req'd

- alternatively, can use dbt core locally and connect to local postgres instance
- run via CLI
 
## Create a dbt project

[following steps from this doc](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_4_analytics_engineering/dbt_cloud_setup.md)

### Connect bigquery to dbt cloud

- create service account for dbt-cloud
  - add bq admin
  - alternatively, add these smaller roles:
    - viewer (for all GCP resources)
    - bq data editor
    - bq job user
    - bq user
- create the .json key
- (optional) sftp to vm
- initialize cloud dbt project and connect to bigquery
    - upload the .json key
    - link to our repo
        - provide the ssh key
        - dbt will return with a deploy key
        - add this as a deploy key in our github repo > settings > deploy keys
            - give read/write access
            - add key
        - provide a subdirectory so that when we run `dbt init` it doesn't crowd the root
        - `dbt_models`
    - in the dbt cloud IDE, press `initialize project`
    - some folders and a `dbt_project.yml` will be created
    - name our project - `ny_taxi_trips` - in the `.yml`

### dbt cloud project

Use the starter project

Under `models/examples/` there are two sample `model.sql` files, which in addition to regular SQL statements have a *materialization strategies*:

```sql
{{ config(materialized='table') }}
```

- table
  - faster to query
  - may take longer to rebuild
  - new records in source are not automatically added
  - use for BI tools - faster end-user experience
  - and if transformations are complex
- view
  - no additional data stored
  - may be slow to query against, if transformations are complex (anything outside of renaming/recasting)
  - start with `views`; change to others if performance becomes an issue
- incremental
  - essentially a table
  - allows model to run incrementally
  - e.g. if data doesn't change often, our model would update only if data changes, and perform transformations only on new records
  - needs more configs
- ephemeral
  - reduce DWH clutter; does not produce views or tables
  - creates common table expression (CTE) instead
  - cannot be queried directly
  - use in downstream model

This refers to how the model is *materialized* in the DWH

### dbt model - `FROM`

How does dbt source datasets to use in `FROM` clauses?

##