# BigQuery and DWH

## Intro concepts

### OLTP vs OLAP

On-line *Transactional* Processing v. On-line *Analytical* Processing
|| OLTP | OLAP |
|--------------|-----------|------------|
| Purpose | Realtime business ops | Solve problems, find insights |
| Updates | small, frequent| batch, long running
| design | normalized for efficiency | denormalized for analysis
| space req | small, if archived | large due to historical aggregates|
| backup/recv | regular backups to ensure continuous operation and meet legal/governance reqs | can be reloaded from OLTP|
| productivity | boosts end-users | analysts, managers, executives |
| view | day-to-day transactions | multi-dim view of enterprise level data|
| examples | customer-facing staff, shoppers|analysts, execs|

- OLAP may feed to *data marts* (departmentalized) for analysts
- DS may prefer the multidimensionality of the warehouse

### Bigquery

- serverless DWH
  - no hardware to manage
  - no db software to install
- scalability
- availability
- built-in features for
  - ML
  - geospatial
  - BI
- separates compute and storage
- *external table*
  - metadata is stored on bigquery itself
  - actual data is still sourced externally, and not loaded onto bq storage

### Partitioning

Improves querying performance by segmenting via some granular column, e.g. `modified_datetime` or `ingestion_time`, that's commonly used

- even though datetime can be down to seconds resolution, we can still partition by `day`
- drastically reduce the data required to be scanned and processed
  - highlight the SQL command in bigquery editor to preview how much data will be processed, before processing
- limit of 4000 partitions

### Clustering

- Columns are specified to *cluster* related data
- Order of the clustering columns is important
  - determines sort order
- improves filter, and aggregate queries
- up to 4 clustering columns
  - these are columns commonly filtered or aggregated against
  - high granularity
- cost-benefits not known upfront
- to maintain benefit of clustering, tables must be periodically *re-clustered* to maintain the sort property

### BQ Best practices

- cost reduction
  - avoid `SELECT *` - use only data we need
  - price queries before running (by highlight)
  - cluster/partition
  - query in stages
- performance
  - filter on partitioned columns (which we set beforehand)
  - denormalize
  - reduce data size before `JOIN`
  - start with largest row-size table in SQL statements, to better distribute load on its serverless infra

### BQ Internals

- `Colossus` - storage
- `Jupiter` - network: TB/sec
  - what actually enables the compute/storage separation
- `Dremel` - query execcution
  - modifies the query to distribute the load
  - User query -> root server -> mixer -> leaf nodes -> Colossues
- BQ uses column oriented data
  - improved aggregate performance
  - OLAP generally only uses a few columns
  - drastically reduces query size

## BQ Machine learning

[offical bq ML tutorials](https://cloud.google.com/bigquery-ml/docs/tutorials)

- Freetier
  - 10 GB/mth
  - 1 TB/mth processing

### Basic syntax

[Reference docs](https://cloud.google.com/bigquery-ml/docs/reference/standard-sql/bigqueryml-syntax-e2e-journey)

Create the model:

```sql
-- linear reg model to predict tips
CREATE OR REPLACE MODEL `trips_data_all.green_tip_model`
OPTIONS(
  model_type='linear_reg',
  input_label_cols=['tip_amount'],
  DATA_SPLIT_METHOD='AUTO_SPLIT') AS
SELECT
  *
FROM
  `trips_data_all.green_taxi_trips_ml`
WHERE
  tip_amount IS NOT NULL;
```

Creates a model `green_tip_model` under `dataset.Models` in our project

Reference it with:

```sql
SELECT *
FROM ML.<method>( -- FEATURE_INFO, EVALUATE, PREDICT, etc.
    MODEL 'dataset.model', 
    (
        -- dataset used by model
        SELECT * 
        FROM data 
        WHERE ...
    )
);
```

#### Feature stats

```sql
-- CHECK FEATURES
SELECT * FROM ML.FEATURE_INFO(MODEL `trips_data_all.green_tip_model`);
```

#### Evaluate

```sql
-- EVALUATE THE MODEL
SELECT
  *
FROM
  ML.EVALUATE(
    MODEL `trips_data_all.green_tip_model`,
    (
      SELECT
        *
      FROM
        `trips_data_all.green_taxi_trips_ml`
      WHERE
        tip_amount IS NOT NULL
    )
  );
```

Output:

```json
[{
  "mean_absolute_error": "0.79690319429957723",
  "mean_squared_error": "2.4932030995937571",
  "mean_squared_log_error": "0.39596225536150648",
  "median_absolute_error": "0.46554008681451031",
  "r2_score": "0.36729953572866236",
  "explained_variance": "0.36729953576607466"
}]
```

#### Predict

```sql
-- PREDICT THE MODEL
SELECT
  *
FROM
  ML.PREDICT(
    MODEL `trips_data_all.green_tip_model`,
    (
      SELECT
        *
      FROM
        `trips_data_all.green_taxi_trips_ml`
      WHERE
        tip_amount IS NOT NULL
    )
  );
```

Returns prediction for each row

#### predict and *explain*

```sql
-- PREDICT AND EXPLAIN
SELECT
  *
FROM
  ML.EXPLAIN_PREDICT(
    MODEL `trips_data_all.green_tip_model`,
    (
      SELECT *
      FROM `trips_data_all.green_taxi_trips_ml`
      WHERE tip_amount IS NOT NULL
    ), 
    -- for each prediction, list 3 of the most impactful features
    -- as well as their numerical attribution
    STRUCT(3 as top_k_features)
  );
```

#### Hyperparam tuning

Tests for a range of regularization parameters; processes 1.4GB

```sql
-- HYPER PARAM TUNNING
CREATE OR REPLACE MODEL `trips_data_all.green_tip_model_hyperparam`
OPTIONS(
    model_type='linear_reg',
    input_label_cols=['tip_amount'],
    DATA_SPLIT_METHOD='AUTO_SPLIT',
    num_trials=5,
    max_parallel_trials=2,
    l1_reg=hparam_range(0, 20),
    l2_reg=hparam_candidates([0, 0.1, 1, 10])
    ) AS
SELECT *
FROM `trips_data_all.green_taxi_trips_ml`
WHERE tip_amount IS NOT NULL;
```

### Deployment

The trained model in bigquery can be extracted and served as a dockerized microservice, where we can `curl <feature_data>` against it and receive a prediction back.

1. extract the model and store in `gsc`:

    `bq --project_id taxi-rides-ny extract -m nytaxi.tip_model gs://taxi_ml_model/tip_model`
1. download to local directory:

    ```bash
    mkdir /tmp/model
    gsutil cp -r gs://taxi_ml_model/tip_model /tmp/model
    mkdir -p serving_dir/tip_model/1
    cp -r /tmp/model/tip_model/* serving_dir/tip_model/1
    ```

1. `docker pull tensorflow/serving` - [this one's pretty magical](https://github.com/tensorflow/serving/blob/master/tensorflow_serving/g3doc/docker.md)
1. Run the image and mount the model on the container. The container acts as the model's REST API, exposing port 8501 by default. Env var `MODEL_NAME` is the subfolder inside `/models/` which contains our model.

    ```bash
    docker run \
        -p 8501:8501 \
        --mount type=bind,source=pwd/serving_dir/tip_model,target= /models/tip_model \
        -e MODEL_NAME=tip_model \
        -t tensorflow/serving &
    ```

1. POST the json including the necessary features and get a prediction back:

    ```bash
    curl -d \
    '{"instances": [{"passenger_count":1, "trip_distance":12.2, "PULocationID":"193", "DOLocationID":"264", "payment_type":"2","fare_amount":20.4,"tolls_amount":0.0}]}' \
    -X POST http://localhost:8501/v1/models/tip_model:predict
    ```

1. Receive: `{ "predictions": [2.5, 3.0, 4.5] }`
