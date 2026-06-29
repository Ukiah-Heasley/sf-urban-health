# Architecture

The checked-in runtime is a raw S3 ingestion path, a lakehouse bronze path, and
retained dashboard/report consumer shells.

## Raw ingestion

```text
DataSF SODA API
    │  paginated HTTPS; half-open Airflow data interval
    ▼
airflow/include/scripts/soda_ingest.py
    │  streaming compact NDJSON
    ▼
S3 raw/{dataset}/data_interval_start=.../data_interval_end=.../records.ndjson
    │
    ▼
record_{dataset}_extract_metadata
    │  current ingest metadata event + attempt audit event
    ▼
Airflow ingest-complete asset
```

The three generated ingest DAGs are:

| DAG | Schedule | Tasks |
| --- | --- | --- |
| `ingest_permits` | `0 6 * * *` | `extract_permits_to_raw -> record_permits_extract_metadata -> ingest_complete` |
| `ingest_evictions` | `0 6 * * *` | `extract_evictions_to_raw -> record_evictions_extract_metadata -> ingest_complete` |
| `ingest_incidents` | `0 6 * * *` | `extract_incidents_to_raw -> record_incidents_extract_metadata -> ingest_complete` |

`dag_factory.py` builds all three from `DatasetConfig` and `DagConfig` values.
Each extract task captures `started_at` and `completed_at` inside
`extract_to_raw` and pushes interval metadata through XCom. The metadata task
writes an attempt audit event under
`lake/metadata/events/ingest_run_attempts/`, then overwrites the deterministic
current JSON event under `lake/metadata/events/ingest_runs_current/`. Empty
intervals return null raw paths, still write ingest metadata, and still emit the
ingest asset.

Ingest DAGs do not promote bronze Parquet. A failure in lakehouse promotion
does not block raw capture.

## Lakehouse transformation path

```text
permits ingest asset   ─┐
evictions ingest asset ─┼─> transform_lakehouse
incidents ingest asset ─┘      │
                               ├─> select_lakehouse_interval
                               ├─> branch_on_lakehouse_plan
                               ├─> promote_permits_to_bronze
                               ├─> promote_evictions_to_bronze
                               ├─> promote_incidents_to_bronze
                               ├─> compact_lakehouse_metadata
                               ├─> lakehouse_transform_complete
                               └─> lakehouse_noop
```

`select_lakehouse_interval` reads current JSON ingest events and file-manifest
events from S3 and selects the oldest complete interval missing bronze promotion
(default `pending` mode). Each DAG run promotes at most one interval
(`LAKEHOUSE_PLAN_LIMIT` must be `1`). `transform_lakehouse` sets
`max_active_runs=1` so concurrent runs cannot select the same global interval.
When no interval is selected,
`branch_on_lakehouse_plan` skips promotion, compaction, and lakehouse asset
emission via `lakehouse_noop`. Promotion tasks load the selected current ingest
event for each dataset, stream raw NDJSON into typed bronze Parquet under
`lake/parquet/bronze/`, and write file-manifest metadata events under
`lake/metadata/events/file_manifest/`. Bronze Parquet is written to a local temp
file, row counts are validated against the ingest event, and only then uploaded
to the final bronze key. `compact_lakehouse_metadata` is the only task that may
use DuckDB, and only as an in-memory compaction engine while building
contract-compatible metadata Parquet under `lake/parquet/metadata/`. Each
compaction deletes the full `ingest_runs/` and `file_manifest/` metadata Parquet
prefixes and rebuilds them from the JSON source-of-truth event families. Empty
ingest or file-manifest event families produce no Parquet output.

S3 JSON metadata events are the durable source of truth for promotion control
flow. Current ingest events under `ingest_runs_current/` drive the planner;
attempt audit events do not. Compacted metadata Parquet reflects the current
ingest interval grain, is fully rebuilt from JSON on each compaction, and is a
queryable export, not a planner input.

## dbt and local Spark

`dbt/` is the canonical lakehouse-first project. `make sync-dbt` mirrors dbt and
lakehouse contracts into `airflow/include/` for the Astro Docker build context.

Local development runs MinIO and a repo-built Spark Thrift Server from
`lakehouse/docker-compose.yml`. Spark is configured with Iceberg and S3A so
warehouse data is stored under `s3a://lakehouse/warehouse/` in the local bucket.
`dbt/profiles.yml` connects to Spark Thrift with the `lakehouse` uv dependency
group. Bronze remains Parquet in MinIO.

Local fixture preparation uses `make lakehouse-prepare-fixtures`, which
destructively resets the local MinIO bucket, seeds fixture raw NDJSON for
permits, evictions, and incidents, promotes bronze Parquet through the existing
Python promotion code, and restarts Spark Thrift so catalog namespaces are
rebuilt. dbt reads bronze through ephemeral `bronze_*` models over MinIO Parquet.
Silver `*_current` models and gold analytical models materialize as Iceberg
through Spark/dbt. The `smoke_iceberg` model remains a harmless catalog
connectivity check.

```text
make spark-up
make lakehouse-prepare-fixtures
    → destructive MinIO reset
    → raw fixture NDJSON for permits, evictions, incidents in MinIO
    → bronze Parquet promotion + Spark catalog refresh

make dbt-lakehouse-gold
    → bronze_* (ephemeral) + silver/gold Iceberg tables

make dbt-lakehouse-permits
    → bronze_permits (ephemeral) + permits_current Iceberg table

make dbt-lakehouse-smoke
    → dbt-spark → Thrift → smoke_iceberg Iceberg table
```

## Consumers

- Plotly Dash keeps six pages as a consumer shell. Live warehouse loading is
  disabled; startup cache calls fail closed to empty Polars frames.
- Evidence reads committed local Parquet snapshots with DuckDB during its static
  build. The Pages workflow builds from those snapshots only.

These paths are separate processes; neither dashboard is part of an ingest DAG.
