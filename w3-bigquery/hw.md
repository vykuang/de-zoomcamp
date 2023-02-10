# W3 - BigQuery and data warehouse

## Setup

Load `fhv-2019` data into a bigquery table

1. Load from https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv onto `gcs`
  - [original source from nyc.gov](https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-01.parquet)
  - `gs://<de_zoom_datalake>/data/fhv/<filename>_2019-*.parquet`

1. Load from `gcs` to bq via console or statement

  ```sql
  CREATE OR REPLACE EXTERNAL TABLE `de-zoom-83.trips_data_all.fhv_taxi_trips`
    OPTIONS (
    format = 'parquet',
    uris = ['gs://<de_zoom_datalake>/data/fhv/<filename>_2019-*.parquet']
    );
  ```

### Load fhv

- used `web_gcs_parq.py` to do transfer from `nyc.gov` to `gcs`
- originally used week 2's `etl_web_gcs.py`, which used `df.read_parquet` straight from `cloudfront.net` URI
- ran into issue in week 2:

Running into a casting datatype issue with `pd.read_parquet` and `fhv-2019-02.parquet`:

```py
pyarrow.lib.ArrowInvalid: Casting from timestamp[us] to timestamp[ns] would result in out of bounds timestamp: 33106123800000000
```

Since pandas uses nanosecond datetime (thanks finance), int64 limits it to a 584 year span:

```py
In [54]: pd.Timestamp.min
Out[54]: Timestamp('1677-09-22 00:12:43.145225')

In [55]: pd.Timestamp.max
Out[55]: Timestamp('2262-04-11 23:47:16.854775807')
```

The errant row had a year of 3019, hence the out of bounds error. pyarrow uses `us`, which gives it a much more reasonable timespan and does not care about 3019.

`pd.to_datetime` has an optional arg `errors='coerce'` which automatically coerces out of bounds to either lower or upper bound.

```py
feb = pq.read_table(path_feb)
dt = pd.to_datetime(feb.column('dropOff_datetime'), errors='coerce')
```

Refactor the code to retrieve first, and optionally load into df if we want to transform it

- use `pc.filter` to remove all timestamps out of bounds
  - but this removes data
- *replace* the timestamps out of bounds with null?
  - replace both pickup and dropoff as null
- replace locationIDs? no, leave as null

Coerce the guilty timestamp via `pd.to_datetime`; use `Table.to_pandas()` on the other columns

```py
feb.column_names
dt_cols = [col for col in feb.column_names if "datetime" in col]
nondt_cols = [col for col in feb.column_names if col not in dt_cols]
print(dt_cols, nondt_cols)
df_feb_dts = pd.DataFrame()
for dt_col in dt_cols:
    feb_dt = feb.column(dt_col)
    df_feb_dts[dt_col] = pd.to_datetime(feb_dt, errors='coerce')

# print(feb_dt)
# dts = pd.to_datetime(feb_dt, errors='coerce')
df_feb = feb.select(nondt_cols).to_pandas()
df_feb = pd.concat([df_feb, df_feb_dts], axis=1)
df_feb.head()
```

### Coercing with `.astype()` with `NaN` present

Since `NaN` is of type `float`, we cannot cast an array to any type besides `float` or `object` if `NaN` is present; it must be dealt with in some way, e.g. `.fillna(0)`

Alternatively, `pandas` has a built-in integer type that does allow for `NaN`; simply capitalize the `I` when specifying `Int32`, or int whichever. Question now is how will parquet or Bigquery interface with this schema?

`bigquery` correctly recognizes the recasted pandas dtypes.

### Create BQ table

In addition to creating external table, make one in BQ as well

To load parquet from GCS to bq table:

```sql
LOAD DATA OVERWRITE mydataset.mytable
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://bucket/path/file.parquet']);
```

- replace `OVERWRITE` with `INTO` for append
## Questions

1. fhv record count - 43 million
1. count distinct num of `affiliated_base_number`
  - bq table: 318 MB
  - ext table: 0 B
  - external table processing quantity cannot be estimated, hence the zero
1. how many records have both PU and DO ID as null?
  - 717K
1. how to optimize if query always filters by pickup_datetime and order by affiliated?
  - partition by pickup_dt
  - cluster by affiliated
1. Create the partitioned and clustered table

  ```sql
  CREATE TABLE 
    project-name.my-dataset.table-name (
        column_name DATA_TYPE
        ...
    )
  PARTITION BY
    partition_col
  CLUSTER BY
    cluster_col1, cluster_col2
  AS (
    SELECT * FROM my-dataset.unpartitioned_unclustered_table
  )
  ```

  alternatively, load from parquet again:

  ```sql
    LOAD DATA INTO mydataset.mytable
    PARTITION BY TIMESTAMP_TRUNC(transaction_date, MONTH)
    CLUSTER BY customer_id
    FROM FILES(
      format = 'AVRO',
      uris = ['gs://bucket/path/file.avro']);
  ```
  
    - Unpartitioned and unclustered table processes 648 MB
    - partitioned and clustered table processes 24 MB
    - my partitioning statement only worked after I removed the `OPTIONS` clause; produced empty table otherwise, when I specified things like `partition_expiry_date`
1. External table stores data in original source. [docs here](https://cloud.google.com/bigquery/docs/external-tables?hl=en)
1. Is it always best practice to cluster? Not exactly because it depends on if dataset has obvious clustering columns, and if the clustering columns have the cardinality required. It also depends if the dataset size is large enough to see the benefits, otherwise the overhead involved with maintaining a clustered table (via regular sorting) may not outweigh the costs

   In practice, the clustered column sorts the storage blocks, and this sorting should improve query performance and reduce processing costs. Best use cases for clustering:
    
    - queries commonly filter on those columns; if they're already sorted, querying wilil be faster
    - those columns have high granularity (or cardinality); they give the search better resolution
    - cost estimates are not required prior to running the query
      - clustered table cannot provide cost estimate because the number of clustered storage blocks are not known prior to execution
    - [see docs here](https://cloud.google.com/bigquery/docs/clustered-tables)
 _
