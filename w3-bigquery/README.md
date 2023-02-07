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

### BQ Machine learning

- Freetier
  - 10 GB/mth
  - 1 TB/mth processing
- `CREATE MODEL...`