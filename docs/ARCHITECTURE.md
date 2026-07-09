# Architecture

The checked-in runtime is an AWS S3 ingestion path, a Glue-catalog lakehouse
transform path, and an Evidence report consumer. The local MinIO + Hadoop
catalog Compose stack is a development harness, not a separate deployed layer.

Airflow handles the S3 raw-ingestion and bronze-promotion paths. Host CLI
commands support Spark/dbt transforms against AWS Glue; Airflow does not yet
orchestrate that Glue transform path.

For an explorable component and contract map, open the deployed [interactive
PipeFlow architecture whiteboard](https://ukiah-heasley.github.io/sf-urban-health/architecture/sf-urban-health.pipeflow.html).

## Raw ingestion

```text
DataSF SODA API
    │  paginated HTTPS; half-open extraction interval
    ▼
airflow/include/scripts/soda_ingest.py
    │  streaming compact NDJSON
    ▼
S3 raw/{dataset}/data_interval_start=.../data_interval_end=.../records.ndjson
    │
    ▼
record_{dataset}_extract_metadata
    │  current ingest metadata event + attempt audit event, including failed extracts
    ├─ success/empty extract ─> Airflow ingest-complete asset
    └─ failed extract ───────> Airflow failed-ingest-metadata asset
```

The three generated ingest DAGs are:

| DAG | Schedule | Tasks |
| --- | --- | --- |
| `ingest_permits` | `0 6 * * *` | `extract_permits_to_raw -> record_permits_extract_metadata -> ingest_complete`; failed metadata also routes to `mark_permits_extract_failure_metadata` |
| `ingest_evictions` | `0 6 * * *` | `extract_evictions_to_raw -> record_evictions_extract_metadata -> ingest_complete`; failed metadata also routes to `mark_evictions_extract_failure_metadata` |
| `ingest_incidents` | `0 6 * * *` | `extract_incidents_to_raw -> record_incidents_extract_metadata -> ingest_complete`; failed metadata also routes to `mark_incidents_extract_failure_metadata` |

`_shared/dag_factory.py` builds all three from `DatasetConfig` and `DagConfig` values.
Scheduled runs derive the extraction window from Airflow data intervals. Manual
full/backfill triggers pass JSON conf (`load_mode`, `window_start`, `window_end`,
optional `lookback_hours`); `resolve_extract_window` maps those bounds onto the
same `ExtractWindow` shape and raw key layout as scheduled runs. Each extract task
captures `started_at` and `completed_at` inside
`extract_to_raw` and pushes interval metadata through XCom. The metadata task
writes an attempt audit event under
`lake/metadata/events/ingest_run_attempts/`, then overwrites the deterministic
current JSON event under `lake/metadata/events/ingest_runs_current/`. Empty
intervals return null raw paths, still write ingest metadata, and still emit the
ingest asset. Failed extracts recompute the same interval bounds, write
`status="failed"` current and attempt metadata events with null raw paths, and
do not emit the ingest-complete asset.

Ingest DAGs do not promote bronze Parquet or run dbt. A failure in lakehouse
promotion or dbt transforms does not block raw capture.

## Bronze promotion path

```text
permits ingest asset   ─┐
evictions ingest asset ─┼─> promote_raw_to_bronze
incidents ingest asset ─┘      │
                               ├─> select_bronze_interval
                               ├─> branch_on_bronze_plan
                               ├─> promote_permits_to_bronze
                               ├─> promote_evictions_to_bronze
                               ├─> promote_incidents_to_bronze
                               ├─> compact_lakehouse_metadata
                               ├─> bronze_promotion_complete
                               └─> bronze_promotion_noop
```

`select_bronze_interval` reads current JSON ingest events and file-manifest
events from S3 and selects the oldest complete interval missing bronze promotion
(default `pending` mode). Each DAG run promotes at most one interval
(`LAKEHOUSE_PLAN_LIMIT` must be `1`). `promote_raw_to_bronze` sets
`max_active_runs=1` so concurrent runs cannot select the same global interval.
When no interval is selected,
`branch_on_bronze_plan` skips promotion and bronze promotion asset emission via
`bronze_promotion_noop`, but still runs metadata compaction. Promotion tasks
load the selected current ingest event for each dataset, stream raw NDJSON into
typed bronze Parquet under
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
attempt audit events do not. The planner requires the configured datasets as a
subset of the current event set, ignores extra dataset events, and skips any
interval where a required dataset has `status="failed"`. Compacted metadata
Parquet reflects the current ingest interval grain, is fully rebuilt from JSON
on each compaction, and is a queryable export, not a planner input.

## Failure metadata path

```text
failed ingest metadata asset
    → handle_ingest_failure_metadata
        ├─> compact_failed_ingest_metadata
        └─> dbt_build_observability_gold
```

`handle_ingest_failure_metadata` is asset-triggered by failed ingest metadata.
It compacts current metadata JSON events, then runs
`dbt build --select pipeline_health data_trust` inside the Astro Airflow
runtime. This updates operational gold tables for failed extracts without
emitting dataset ingest assets, bronze promotion assets, or the general gold
transform completion asset.

## Local Airflow gold transform path

```text
bronze promotion asset
    → build_lakehouse_gold
        ├─> dbt_debug
        ├─> dbt_build_lakehouse_gold
        └─> lakehouse_gold_complete
    → gold transform completion asset
```

`build_lakehouse_gold` is asset-triggered by `BRONZE_PROMOTION_ASSET`. It runs
`dbt debug` then `dbt build --select tag:lakehouse` inside the Astro Airflow
runtime through `scripts/dbt_lakehouse.py`, using the mirrored dbt project at
`airflow/include/dbt/` (synced by `make sync-dbt`). dbt connects to Spark
Thrift using `dbt/profiles.yml` and `DBT_SPARK_*` environment variables. The
final task emits `GOLD_TRANSFORM_ASSET`. Evidence Parquet export is not part of
this DAG. This asset-triggered path does not orchestrate the AWS Glue transform
proof described below.

## dbt and Spark catalogs

`dbt/` is the canonical lakehouse-first project. `make sync-dbt` mirrors dbt and
lakehouse contracts into `airflow/include/` for the Astro Docker build context.

### AWS Glue catalog

`make spark-up-aws` starts Spark Thrift with the AWS Glue Iceberg catalog and
S3FileIO against AWS S3. It does not start MinIO, Glue crawlers, or Glue ETL
jobs. Bronze and compacted metadata Parquet must already exist in S3.
`dbt/profiles.yml` connects through Spark Thrift using the `lakehouse` uv group;
dbt reads those prefixes through `LAKEHOUSE_BRONZE_BASE_URI` and
`LAKEHOUSE_METADATA_BASE_URI`, then materializes silver and gold as Glue
catalog Iceberg tables. The host CLI commands are the supported Glue proof path.

### Optional local test harness

`make spark-up` starts MinIO and Spark Thrift with a local Hadoop catalog.
`make lakehouse-prepare-fixtures` destructively resets that local bucket, loads
three deterministic fixture datasets, and promotes them to bronze. It exists to
exercise the same promotion and dbt logic without AWS credentials; it is not a
deployment target.

## Consumers

- Evidence reads committed Parquet snapshots with DuckDB during its static
  build. The snapshots are exported from the six lakehouse gold tables:
  `housing_production`, `permit_pipeline`, `evictions`, `public_safety`,
  `pipeline_health`, and `data_trust`. The Pages workflow builds from those
  snapshots only.

This consumer path is separate from the ingest DAGs.
