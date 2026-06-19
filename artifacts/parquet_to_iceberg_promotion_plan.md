# Parquet Lake To Iceberg Promotion Plan

Purpose: define how to promote the v1 S3 Parquet medallion lake into an Iceberg
lakehouse without losing the raw replay story or rewriting the project from
scratch.

Preferred approach:

```text
Create new Iceberg tables under a separate Iceberg root.
Backfill them from v1 Parquet or raw NDJSON.
Validate row counts, grains, and metrics.
Switch dbt sources from Parquet paths to Iceberg catalog relations.
Keep v1 Parquet paths until the Iceberg path is trusted.
```

This is safer than trying to register existing Parquet files in place.

---

## Difficulty Read

Expected difficulty if v1 is shaped well:

```text
Backfill new Iceberg tables from existing Parquet: moderate
Register existing Parquet files in place: moderate to high
Make dbt write native Iceberg tables directly: moderate to high
Production-grade Iceberg maintenance: high enough to deserve its own phase
```

The cleanest path is a rewrite/backfill into new Iceberg table locations. It
costs more S3 storage during migration, but it is easier to validate and easier
to roll back.

---

## Promotion Prerequisites

Before starting:

- Raw NDJSON exists for every interval that should be recoverable.
- Bronze Parquet has typed columns and standard metadata columns.
- dbt models do not hard-code physical S3 paths.
- dbt sources abstract the current Parquet locations.
- Key grains are tested.
- Row counts and core metrics are reproducible.
- No Snowflake-only SQL remains in the active DuckDB/dbt path.

Recommended readiness checks:

```text
count raw records by dataset/window
count bronze rows by dataset/window
count silver current rows by natural key
count gold rows by declared grain
compare dashboard metrics before and after promotion
```

---

## Phase 0: Freeze V1 Contracts

Document each table:

```text
logical table name
physical Parquet path
column list and types
grain
partition columns
known uniqueness tests
freshness column
source raw path relationship
```

This contract becomes the Iceberg table DDL input.

Example:

```text
bronze.permits
  path: s3://bucket/lake/parquet/bronze/permits/
  grain: one DataSF record occurrence
  partition: loaded_date
  freshness: _loaded_at

silver.permits_current
  path: s3://bucket/lake/parquet/silver/permits_current/
  grain: permit_number
  partition: loaded_date or none
  freshness: _loaded_at
```

---

## Phase 1: Choose Catalog And Writer

Pick one catalog and one authoritative writer for the first Iceberg pass.

Catalog candidates:

```text
AWS Glue or SageMaker Lakehouse catalog
Amazon S3 Tables
Lakekeeper or Nessie REST catalog for local/dev
```

Writer candidates:

```text
DuckDB Iceberg extension through an attached REST catalog
Spark with Iceberg libraries
AWS Glue or EMR Spark
PyIceberg for supported write workflows
```

Recommendation:

- For local learning, spike DuckDB Iceberg writes against a REST catalog.
- For AWS portfolio value, also validate reads from Athena.
- Keep only one writer active during the first migration.

Decision record to create:

```text
catalog selected
writer selected
table format version
S3 table root
IAM model
maintenance owner
rollback strategy
```

---

## Phase 2: Create Iceberg Namespaces And Tables

Create namespaces:

```text
bronze
silver
gold
metadata
```

Create Iceberg tables with the same logical names as v1.

Example shape:

```sql
create table bronze.permits (
    permit_number string,
    permit_type_code string,
    permit_type string,
    status string,
    filed_at timestamp,
    issued_at timestamp,
    status_at timestamp,
    data_loaded_at timestamp,
    _loaded_at timestamp,
    _ingest_run_id string,
    _raw_s3_path string,
    _window_start timestamp,
    _window_end timestamp,
    _effective_start timestamp,
    _extracted_at timestamp,
    _source_dataset_id string,
    _record_hash string,
    _raw_payload string
)
partitioned by (day(_loaded_at));
```

Use Iceberg partition transforms rather than copying the v1 folder scheme too
literally.

---

## Phase 3: Backfill Bronze

Preferred source:

```text
v1 bronze Parquet
```

Fallback source:

```text
raw NDJSON replay
```

Backfilling from Parquet should be straightforward if v1 bronze is typed:

```sql
insert into iceberg_catalog.bronze.permits
select
    permit_number,
    permit_type_code,
    permit_type,
    status,
    filed_at,
    issued_at,
    status_at,
    data_loaded_at,
    _loaded_at,
    _ingest_run_id,
    _raw_s3_path,
    _window_start,
    _window_end,
    _effective_start,
    _extracted_at,
    _source_dataset_id,
    _record_hash,
    _raw_payload
from read_parquet('s3://bucket/lake/parquet/bronze/permits/**/*.parquet');
```

Validation:

```text
row count by dataset
row count by window_start/window_end
min/max _loaded_at
distinct ingest_run_id count
sample record hash comparisons
null checks on required columns
```

---

## Phase 4: Rebuild Silver And Gold

Two reasonable options:

### Option A: Rebuild From Iceberg Bronze

Run the existing dbt logic against Iceberg bronze sources and write silver/gold
Iceberg tables.

This is the cleanest end state, but may require custom dbt materialization
support depending on the chosen engine.

### Option B: Backfill From V1 Silver/Gold Parquet

Create Iceberg silver/gold tables and insert from existing v1 Parquet outputs.

This is faster for the initial promotion:

```sql
insert into iceberg_catalog.gold.mart_housing_production
select *
from read_parquet('s3://bucket/lake/parquet/gold/mart_housing_production/**/*.parquet');
```

After the initial promotion, move transformation writes to Iceberg-native tables
as a separate step.

Recommendation:

```text
Use Option B for initial migration.
Use Option A as the follow-up operating model.
```

---

## Phase 5: Migrate Metadata Tables

Create:

```text
metadata.ingest_runs
metadata.file_manifest
metadata.dbt_runs, optional
```

Backfill from DuckDB metadata tables or Parquet exports.

Validation:

```text
every bronze _ingest_run_id has an ingest_runs row
every raw_s3_path in bronze appears in metadata
no successful metadata row is missing window_start/window_end
no failed run emits an ingest asset
```

---

## Phase 6: Dual-Read Validation

For a period, keep both read paths:

```text
v1 Parquet sources
v2 Iceberg sources
```

Compare:

```text
bronze row counts by dataset and loaded_date
silver row counts by entity
gold mart row counts by grain
dashboard headline metrics
dbt test results
freshness status
sample records by natural key
```

Suggested acceptance rule:

```text
No unexpected row-count differences.
No unexpected metric differences.
No failing uniqueness or not-null tests.
Dashboard renders the same core numbers from Iceberg gold.
```

---

## Phase 7: Switch dbt Sources

Change source configuration only after validation passes.

Before:

```yaml
source bronze.permits -> read_parquet('s3://bucket/lake/parquet/bronze/permits/**/*.parquet')
```

After:

```yaml
source bronze.permits -> iceberg_catalog.bronze.permits
```

Model SQL should stay almost unchanged if v1 source abstraction was done well.

---

## Phase 8: Update Airflow Writes

Once reads are stable, change the active write path:

```text
extract interval to raw NDJSON
append interval rows to Iceberg bronze
record ingest run in Iceberg metadata
run dbt transformations into Iceberg silver/gold
emit assets
```

For the first Iceberg write implementation:

- Keep raw NDJSON unchanged.
- Keep the same Airflow interval semantics.
- Keep no-record ingest metadata.
- Avoid multiple concurrent writers per Iceberg table.
- Add retry-safe checks around snapshot commits.

---

## Phase 9: Add Iceberg Maintenance

Iceberg adds table maintenance work that plain Parquet does not have.

Add scheduled maintenance for:

```text
small file compaction
snapshot expiration
orphan file cleanup
metadata file cleanup
table statistics, if supported by the chosen engine
```

Maintenance should be explicit and visible in Airflow or Make targets. This is
part of the portfolio value.

---

## Rollback Strategy

Rollback should be simple because v1 paths are left intact:

```text
switch dbt sources back to Parquet
pause Iceberg writer tasks
continue writing raw NDJSON
rebuild Iceberg from raw or v1 Parquet after fixing issue
```

Do not delete v1 Parquet paths until Iceberg has been the trusted path for a
meaningful period.

---

## In-Place Registration Alternative

Some Iceberg engines support registering or adding existing Parquet files to an
Iceberg table. This can avoid rewriting data.

Use this only if:

- The existing Parquet schema exactly matches the target Iceberg schema.
- Partition folders map cleanly to the target partition spec.
- You understand which files the Iceberg table now owns.
- You have a rollback plan for partially registered data.

For this project, in-place registration is less attractive than clean
backfill because the datasets are small and raw replay is available.

---

## Final Acceptance Criteria

Promotion is complete when:

- Iceberg bronze, silver, gold, and metadata tables exist.
- Backfilled Iceberg tables match v1 Parquet row counts and key metrics.
- dbt builds against Iceberg sources.
- Dashboard reads Iceberg gold tables or Iceberg-backed outputs.
- Airflow writes new intervals into Iceberg bronze and metadata.
- Raw NDJSON remains the durable replay layer.
- Maintenance jobs are documented and runnable.
- The README explains both the v1 Parquet lake and v2 Iceberg promotion.

---

## Useful References

- DuckDB Iceberg overview: https://duckdb.org/docs/current/core_extensions/iceberg/overview
- DuckDB Iceberg writes: https://duckdb.org/docs/current/core_extensions/iceberg/writing
- Apache Iceberg Spark procedures: https://iceberg.apache.org/docs/latest/spark-procedures/
- Amazon S3 Tables: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html
