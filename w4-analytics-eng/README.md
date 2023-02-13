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

## dbt basics

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

### File Structure

[docs here](https://docs.getdbt.com/guides/best-practices/how-we-structure/1-guide-overview)

1. `models/` should have three primary layers:
    1. *staging* - creating initial modular blocks from source
    1. *intermediate* - stacking layers of logic with specific purposes to join staging models into what we needs
    1. *mart* - what end-users seed
1. `data/`, or `seeds/`
    - static data files
1. `README.md` - for humans to read
1. `dbt_project.yml` - project configs
1. `packages.yml` - dependencies of this project
1. `macros/` - user defined SQL functions
1. `tests/` - data validation
    - tests for nulls, uniques, accepted values, or some other custom specs
    - typically use open source pkgs like `dbt-utils` or `dbt-expectations`

### dbt cloud project

[docs on project components](https://docs.getdbt.com/docs/build/projects)

Use the starter project with `dbt init`.

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

This refers to how the model is *materialized* in the DWH.

> Materializations abstract away DDL and DML.

[docs: best practices](https://docs.getdbt.com/guides/best-practices/materializations/guides/best-practices/materializations/1-overview)

### dbt model - `FROM`

[dbt: using sources](https://docs.getdbt.com/docs/build/sources)

How does dbt source datasets to use in `FROM` clauses?

- `sources`: defined top level in `models/schema.yml`
  - use in `my_model.sql` `FROM` statements:
  - `FROM {{ source('source_name', 'table_name') }}`
  - the `{{ source(...) }}` invokes a source macro which resolves the name to the correct schema (i.e. table)
  - creates dependency between the source table and the model (derived from the source)
- `seeds`: located in `seeds/`
  - [docs here](https://docs.getdbt.com/docs/build/seeds)
  - csv files
  - for static dim tables which do not change often
  - `dbt seed -s file_name` loads them into the DWH (i.e. bigquery)
  - `taxi_zone_lookup.csv`
  - other examples:
    - country codes: country names
    - employee account IDs: names
  - do not use for raw or sensitive data
- `ref`: references `seed`'d tables downstream in models
  - `FROM {{ ref('table_name`) }}` will reference `table_name` that was seeded
  - abstracts the referencing of table names so that it can be resolved properly in different environments

### Building the first model

1. define `sources` in `schema.yml`
  1. refer to an existing table in our BQ DWH
1. in `stg_fhv_taxi_trips.sql`, in the same dir as the schema:
  1. configure the materialization as `view`
  1. `select * from {{ source('staging', 'fhv_taxi_trips`) }}

`schema.yml`

```yml
version: 2

sources:
    - name: staging
      # For bigquery:
      database: de-zoom-83

      schema: trips_data_all

      tables:
        - name: fhv_taxi_trips
```

The `.sql` model

```sql
{{ config(materialized='view') }}

select *
from {{ source('staging', 'fhv_taxi_trips') }}
limit 100
```

Execute via `dbt run --select stg_fhv_taxi_trips`

A `stg_fhv_taxi_trips` view should now populate the `dbt_ny_taxi` dataset in our BQ DWH; that dataset was previously specified when first creating this project in dbt cloud

Note that being a view, it will show `0 bytes; 0 rows` in `details` tab, but it can be queried against.

The actual SQL used to materialize the model will also be stored in `targets/compiled/proj-name/models/`

#### row_number() over partition

This snippet:

```sql
select 
    *,
    row_number() over (
        partition by vendorid, lpep_pickup_datetime
        ) as row_num
from {{ source('staging','green_tripdata') }}
where vendorid is not null 
```

is a common pattern which produces a new column of `row_num` to use as index.

- `row_number() over` is a window function that assigns sequential `int`s to each row within a partition
- `partition by` defines our partitions
- so when it gets to a new `vendorid`, `row_num` alos re-initializes back to 1

What's curious here is that the partitions are set so granular, at the timestamp level. You would expect that our `row_num` would re-initialize at every row, and it does. Mostly. 
The rationale is that we are also using those two columns, `vendorid` and `lpep_pickup_datetime`, as our table key. We need that combination to be unique for our table to function the way we expect.
If we add `where row_num = 1` as a filter, we are guaranteeing that every `vendorid` and `lpep_pickup_datetime` combination is unique; if for whatever reason there were duplicates,
the `row_num = 1` clause would filter it out, and ensure that our `{{ dbt_utils.surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as tripid` remains unique

### Macro

Defined in `macros/some_name.yml`, they are reuseable SQL snippets that act like functions.

- allows control structures like `if` and `for loops`
- set environment vars for production
- written as `.sql` inside `macros/`
- the `.yml` references that `some_macro.sql` and contextualizes it with metadata

### Packages

Analogous to libraries

- standalone dbt projects, incl. models and macros that target specific problems
- when added to a project, that package's models and macros are also added to your project
- define the package dependencies in a `packages.yml` in project root, i.e. `dbt-models/`
- `dbt_utils` has a `.surrogate_key(['list_of_cols'])` which produces an ad-hoc unique ID based on that `list_of_cols`

```yml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: 0.8.0
```

Reference with

```sql
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as tripid,
    ...
```

Before usage, must run `dbt deps` to install them onto the project

- creates `dbt_packages/dbt_utils/` that houses the package models/macros`

### Variables

- defines values used across the project
    - define in `dbt_project.yml`
    - define with `{{ var('name', args=...) }}`
    - or, specify in CLI with `dbt run --var 'name:value'`
        - `dbt build` can also set var
- combine with macro to provide dynamic data at runtime

### Seeds

`.csv`s inside `seeds/`, which should be excluded from version control. Add to the DWH via `dbt seed`.
- `dbt seed` will insert `taxi_zone_lookup` table in our `dbt_ny_taxi` dataset
    - `--full-refresh` to `DROP` the original and `CREATE` a new one.

Configure via `dbt_project.yml`, and set the properties via `seeds/properties.yml`. The subdirectory `.yml` will take precedence.

Upload by pushing to the remote repository, so that it shows up in dbt's cloud IDE.

- if it's small enough, can also create the file and copy-paste the contents in

## Testing models

[testing docs](https://docs.getdbt.com/docs/build/tests)

We can impose tests on our models to see if our data meets our expectations. In dbt they function as `select` queries, written such that if they return any rows, it could indicate a problem

- defined on a per-column basis in `schema.yml`, under `column: tests: `
- these built-in checks include
    - `unique`: checks that all values are unique, e.g. for the table's primary key
    - `not_null`: checks for nulls
    - `accepted_value`: checks that all values are expected
    - `relationships`: foreign key to another table
        - `to: ref('taxi_zone_lookup')`
        - `field: CAST(locationid as string)`: all locationIDs exist in taxi_zone_lookup's `locationid`
        - `severity: warn`; could also be error?
- custom test queries are defined the same way as `macros/` but in `tests/`
    - `select * from ... where tripid is null`

Test results are output during `dbt build`, or `dbt run`

### payment_types

There's about 1e6 rows in yellow taxi trips where `payment_types = 0`, failing the test that we set, which assumes that all payment types must have value between 1 and 6. Should the value be shifted, or should `0` be changed to `6`?

## Documenting models

dbt provides a way to automatically generate docs for our project, and render as website

[docs for docs here](https://docs.getdbt.com/docs/collaborate/documentation)

The docs are generated from the column descriptions in our `schema.yml`, under top level `models: -> columns: `

For each column, set:

- name:
- description:
- tests:

CLI commands:

- `dbt docs generate` generates the `.JSON` documentation.
- `dbt docs serve` takes those JSONs and populates a local website

## Deploying a dbt project

### Environments

Deploying implies we're pushing to production. While we develop in `dev` environment, deployment requires `deployment` environment, so create one in `Deploy -> Environments`

- name
- env type: `{'dev', 'deploy'}`
- dbt version
- env vars

A `deployment` type env is required to run jobs

### Jobs

- Projects are deployed via `jobs`.
- jobs are scheduled, or triggered manually
- jobs can include multiple `dbt ...` commands
- jobs can be made to generate documentation

Create in `Deploy -> Jobs`

### Continuous Integration

> Practice of regularly merging dev branches into a central repo, after which automated builds and tests are run

- pull requests can trigger CI
- enabled via webhooks from github
- when pull requests (PRs) are ready to merge, the webhook from github can invoke a new run of a specified job
- that CI job is against a temporary schema
- PR will only go through if that CI job completes successfully



## Visualization
