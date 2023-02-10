-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `de-zoom-83.trips_data_all.fhv_taxi_trips`
OPTIONS (
  format = 'parquet',
  uris = ['gs://<my-bucket>/data/fhv/019-*.parquet']
);

--Count rows
SELECT COUNT(1)
FROM trips_data_all.fhv_taxi_trips

-- Query distinct num of affiliated_base_number
SELECT DISTINCT Affiliated_base_number
FROM `de-zoom-83.trips_data_all.fhv_taxi_data_partition` 
WHERE 
  CAST(pickup_datetime AS DATE) <= "2019-03-31" AND
  CAST(pickup_datetime AS DATE) >= "2019-03-01"

-- How many records with nulls in both PU and DO location ID
SELECT COUNT(1)
FROM trips_data_all.fhv_taxi_trips
WHERE 
  PUlocationID IS NULL AND
  DOlocationID IS NULL

-- create partitioned and clustered table
LOAD DATA OVERWRITE
  trips_data_all.fhv_taxi_trips_partition
PARTITION BY
  TIMESTAMP_TRUNC(pickup_datetime, MONTH)
CLUSTER BY
  Affiliated_base_number
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://<my-bucket>/data/fhv/2019-*.parquet']
)

-- green taxi partition
LOAD DATA OVERWRITE
  trips_data_all.green_taxi_trips_partition
PARTITION BY
  TIMESTAMP_TRUNC(lpep_pickup_datetime, MONTH)
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://<my-bucket>/data/green/2019-*.parquet']
)

-- green taxi ML
-- recast locationID and payment_type as STRING, as they're categoricals
CREATE OR REPLACE TABLE
  trips_data_all.green_taxi_trips_ml (
  `passenger_count` FLOAT64,
  `trip_distance` FLOAT64,
  `PULocationID` STRING,
  `DOLocationID` STRING,
  `payment_type` STRING,
  `fare_amount` FLOAT64,
  `tolls_amount` FLOAT64,
  `tip_amount` FLOAT64
  ) AS (
SELECT passenger_count, trip_distance, cast(PULocationID AS STRING), CAST(DOLocationID AS STRING),
CAST(payment_type AS STRING), fare_amount, tolls_amount, tip_amount
FROM `trips_data_all.green_taxi_trips_partition` WHERE fare_amount > 0
);

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

-- CHECK FEATURES
SELECT * FROM ML.FEATURE_INFO(MODEL `trips_data_all.green_tip_model`);

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
    STRUCT(3 as top_k_features)
  );

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