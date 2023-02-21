# Week 5 - Batch Processing with Spark

Installing local spark seems to be a huge hassle. Will work locally with my existing 3.2.1 spark installation and move on to spark container when working in GCP compute

Most notes are in the jupyter notebook.

Concepts covered

- reading files
- schema enforcement
- repartition to optimize cluster workload
- functions and UDF

## Casting schemas

If source is already in parquets, it seems much harder to enforce schema on read. Instead I read the parquet as is, and recasted each column according to their column names, e.g. IDs are ints, datetimes are timestamps, fees and charges are floats

`DataFrame.withColumns(cols_map)` was used; returns a new dataframe.