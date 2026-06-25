# Architecture

The checked-in code has a raw S3 ingestion path, a lakehouse bronze path, and
retained Snowflake-backed transformation and consumer paths.

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

The retained SQL files under `airflow/include/sql/` are not referenced by the
current ingest DAG factory. Raw S3 objects are not copied into Snowflake by
these DAGs.

## Snowflake transformation path

```text
permits ingest asset   ─┐
evictions ingest asset ─┼─> transform_all
incidents ingest asset ─┘      │
                               ├─> dbt deps
Existing Snowflake RAW sources ├─> dbt run --target prod
                               └─> dbt test --target prod
```

The dbt project expects `SF_URBAN_HEALTH.RAW` source tables populated outside
the current raw ingest DAG. It builds:

```text
RAW sources -> staging views -> intermediate views -> mart tables
```

`dbt/` is the canonical project. `make sync-dbt` mirrors it into
`airflow/include/dbt/` for the Astro Docker build.

## Pipeline metadata path

```text
Airflow REST API
    -> ingest_pipeline_metadata (07:00 UTC)
    -> Snowflake METADATA.AIRFLOW_DAG_RUNS / AIRFLOW_TASK_INSTANCES
    -> dbt observability marts
```

The collector enriches extract task instances with selected XCom values before
upserting them. The checked-in collector asks for `max_watermark`, while the
current extract DAG publishes `max_loaded_at`; the maximum timestamp therefore
does not currently populate that Snowflake field.

## Consumers

- Plotly Dash reads Snowflake mart and metadata schemas into Polars frames when
  the process starts.
- Evidence reads local Parquet snapshots with DuckDB during its static build.
  The Pages workflow first attempts a Snowflake-to-Parquet export and otherwise
  uses the checked-in sample snapshot.

These paths are separate processes; neither dashboard is part of an ingest DAG.
