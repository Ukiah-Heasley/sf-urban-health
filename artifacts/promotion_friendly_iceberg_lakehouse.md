# Promotion-Friendly Iceberg Lakehouse Shape

Purpose: define the v1 S3 Parquet lake so it can later become an Iceberg
lakehouse without conceptual churn.

The v1 system should be useful on its own:

```text
S3 raw NDJSON
S3 Parquet medallion layers
DuckDB transformation engine
dbt model management
```

The v2 promotion should add:

```text
Iceberg table metadata
catalog integration
snapshots and rollback
schema and partition evolution
table maintenance
multi-engine table access
```

---

## Design Principle

Keep v1 Parquet files physically simple, but make the logical model look like
future Iceberg tables.

That means:

- Stable table names now.
- Typed schemas now.
- Metadata columns now.
- Explicit partition columns now.
- No downstream dependency on physical S3 folder names.
- Raw NDJSON stays immutable and replayable.

---

## Recommended Physical Layout

Keep raw, v1 Parquet, and future Iceberg roots separate:

```text
s3://{bucket}/raw/{dataset}/...
s3://{bucket}/lake/parquet/{layer}/{table}/...
s3://{bucket}/lake/iceberg/{namespace}/{table}/...
```

This avoids collision between plain Parquet files and future Iceberg table
metadata. It also gives a clean rollback path during promotion.

Example v1 paths:

```text
s3://{bucket}/raw/permits/window_start=20260618T060000/window_end=20260619T060000/run_id=scheduled__2026-06-18/permits.ndjson

s3://{bucket}/lake/parquet/bronze/permits/loaded_date=2026-06-18/run_id=scheduled__2026-06-18/part-000.parquet
s3://{bucket}/lake/parquet/silver/permits_current/build_date=2026-06-18/part-000.parquet
s3://{bucket}/lake/parquet/gold/mart_housing_production/build_date=2026-06-18/part-000.parquet
```

Example future Iceberg locations:

```text
s3://{bucket}/lake/iceberg/bronze/permits/
s3://{bucket}/lake/iceberg/silver/permits_current/
s3://{bucket}/lake/iceberg/gold/mart_housing_production/
s3://{bucket}/lake/iceberg/metadata/ingest_runs/
```

---

## Logical Namespaces

Use logical names that can survive the promotion:

```text
bronze.permits
bronze.evictions
bronze.incidents

silver.permits_current
silver.evictions_current
silver.incidents_current

gold.mart_housing_production
gold.mart_evictions
gold.mart_public_safety
gold.mart_permit_pipeline

metadata.ingest_runs
metadata.file_manifest
```

In v1, dbt sources map these names to Parquet paths. In v2, the same names map
to Iceberg catalog relations.

---

## Bronze Table Contract

Bronze should be append-only and source-faithful.

Required metadata columns:

```text
_ingest_run_id           TEXT
_raw_s3_path             TEXT
_window_start            TIMESTAMP
_window_end              TIMESTAMP
_effective_start         TIMESTAMP
_loaded_at               TIMESTAMP
_extracted_at            TIMESTAMP
_source_dataset_id       TEXT
```

Recommended metadata columns:

```text
_record_hash             TEXT
_raw_payload             TEXT or JSON
```

Guidance:

- Do not dedupe bronze.
- Do not apply reporting filters in bronze.
- Preserve enough raw context to debug future schema changes.
- Parse common fields into typed columns as early as possible.
- Keep source timestamps and business timestamps distinct.

Example `bronze.permits` columns:

```text
permit_number
permit_type_code
permit_type
status
filed_at
issued_at
status_at
approved_at
last_activity_at
existing_units
proposed_units
zipcode
supervisor_district
neighborhood
data_loaded_at
_loaded_at
_ingest_run_id
_raw_s3_path
_window_start
_window_end
_effective_start
_extracted_at
_raw_payload
```

`data_loaded_at` can preserve the source field name. `_loaded_at` can be the
standard cross-dataset ingestion timestamp used for dbt freshness and ordering.

---

## Silver Table Contract

Silver is where source data becomes analytics-ready.

Responsibilities:

- Deduplicate current entity state.
- Normalize types and names.
- Apply reusable data-quality rules.
- Keep business semantics out of staging when they belong in marts.

Examples:

```text
silver.permits_current
  grain: permit_number

silver.evictions_current
  grain: eviction_id or best available natural key

silver.incidents_current
  grain: incident_id or best available natural key
```

Promotion-friendly dedupe pattern:

```sql
row_number() over (
  partition by natural_key
  order by _loaded_at desc, _ingest_run_id desc
) = 1
```

For records without a reliable natural key, create a documented hash key and
test it.

---

## Gold Table Contract

Gold tables preserve the current mart semantics.

Known grains:

```text
gold.mart_housing_production
  grain: filed_month, neighborhood, supervisor_district, use_transition

gold.mart_public_safety
  grain: incident_month, neighborhood, incident_category or local equivalent

gold.mart_evictions
  grain: filing_month, neighborhood, eviction_reason or local equivalent
```

For future Iceberg promotion, every gold mart should have:

- A declared grain.
- dbt tests for the grain.
- A stable set of partition candidates.
- A small set of documented metric definitions.

---

## Partition Strategy

For plain Parquet v1, partition folders are real physical layout. For Iceberg
v2, partitioning can become hidden table metadata. Choose columns now that make
sense in both worlds.

Bronze:

```text
loaded_date = date(_loaded_at)
```

Silver:

```text
loaded_date for current-state tables, if rebuilt by load date
business_month for historical analytical tables
```

Gold:

```text
filed_month for housing production
incident_month for public safety
filing_month for evictions
```

Avoid over-partitioning. These datasets are small enough that too many tiny
folders will make the lake feel more complex without making it faster.

---

## Metadata Tables

Treat metadata as first-class lake data, even in v1.

`metadata.ingest_runs`:

```text
dataset_name
window_start
window_end
effective_start
status
records_fetched
candidate_max_loaded_at
raw_s3_path
bronze_s3_path
airflow_dag_id
airflow_run_id
started_at
finished_at
```

`metadata.file_manifest`:

```text
layer
table_name
dataset_name
s3_path
row_count
bytes_written
source_path
created_by
created_at
```

These can be DuckDB tables in v1. The Iceberg promotion should migrate them to
Iceberg tables too.

---

## dbt Boundary

Use dbt as the logical abstraction boundary:

- v1 sources point at Parquet paths.
- v2 sources point at Iceberg catalog tables.
- Model SQL should not need to know the physical path layout.

For v1, hide S3 locations behind source definitions and macros:

```yaml
external_location: "{{ lake_source_location('bronze', name) }}"
```

For v2, the same source can become:

```yaml
database: lakehouse
schema: bronze
name: permits
```

---

## Iceberg Promotion Hooks To Prepare Now

Prepare these items before Iceberg exists:

- A schema contract per bronze table.
- A grain contract per silver/gold table.
- Row-count validation between layers.
- Natural key tests for deduped silver tables.
- Source freshness based on `_loaded_at`.
- No hard-coded Snowflake JSON syntax in models.
- No hard-coded S3 paths in model SQL.
- Rebuild instructions from raw NDJSON.

---

## Future Iceberg Catalog Options

Reasonable promotion targets:

```text
AWS Glue / SageMaker Lakehouse catalog
Amazon S3 Tables
Lakekeeper or Nessie as a local REST catalog
Spark or DuckDB Iceberg writer through a REST catalog
```

For a portfolio project, a compelling path is:

```text
local/dev: DuckDB + Parquet, optional local REST catalog
cloud/v2: S3 + Iceberg + Glue or S3 Tables
query: DuckDB locally, Athena for cloud validation
```

---

## README Story

The eventual portfolio story can be:

```text
V1: Built a medallion data lake on S3 using immutable NDJSON raw extracts,
typed Parquet analytical layers, dbt-duckdb transformations, and Airflow
interval orchestration.

V2: Promoted the Parquet lake to Iceberg tables for snapshots, schema
evolution, rollback, catalog integration, and multi-engine query access.
```
