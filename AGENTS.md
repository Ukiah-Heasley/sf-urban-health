# Repository guidance for coding agents working on SF Urban Health.

## Commands

Use root `Makefile` targets; ad-hoc Airflow CLI targets source `airflow/.env`.

```bash
make ingest              # permits: DataSF -> S3 raw NDJSON
make sync-dbt            # mirror dbt/ and contracts/ into airflow/include/
make airflow-up          # sync Airflow assets, then astro dev start (alias: airflow-up-local)
make airflow-up-local    # copy airflow/.env.local -> airflow/.env, then start Astro
make airflow-up-aws      # copy airflow/.env.aws -> airflow/.env, then start Astro
make airflow-down
make airflow-logs
make spark-up            # local MinIO + Spark Thrift Server
make spark-up-aws        # Spark Thrift in AWS Glue catalog mode (no MinIO)
make spark-down
make dbt-lakehouse-debug
make dbt-lakehouse-smoke
make lakehouse-prepare-fixtures       # destructive local MinIO reset + all dataset fixtures
make lakehouse-prepare-permits-fixture  # alias for lakehouse-prepare-fixtures
make dbt-lakehouse-permits              # build/test permits_current silver Iceberg
make dbt-lakehouse-gold                 # build/test all lakehouse bronze/silver/gold Iceberg
make export-evidence-snapshots            # export Evidence Parquet snapshots from selected lakehouse gold
make lakehouse-status                    # inspect S3 interval diagnostics
make lakehouse-promote                   # trigger a configurable promotion drain
make lakehouse-genesis                   # trigger the matching three-DAG full load
make lint
make yamllint
make pre-commit
make docs-check
make test
make lakehouse-smoke
```

## Current architecture

The checked-in runtime is split:

```text
DataSF SODA API -> soda_ingest.py -> S3 raw interval NDJSON
  -> lakehouse_metadata.py current + attempt ingest events
  -> success ingest assets or failed metadata asset

any ingest asset -> promote_raw_to_bronze -> S3 backlog snapshot + bronze Parquet
  -> compact metadata Parquet -> bronze promotion asset (when receipts exist)

bronze promotion asset -> build_lakehouse_gold -> dbt silver/gold Iceberg
  -> gold transform completion asset

failed metadata asset -> handle_ingest_failure_metadata
  -> compact metadata Parquet -> dbt pipeline_health/data_trust

lakehouse/ Compose -> MinIO + Spark Thrift + Iceberg (local Hadoop catalog)
  -> fixture prep -> bronze Parquet in MinIO -> dbt bronze/silver/gold Iceberg
  -> dbt smoke Iceberg model (local dev only)

`make spark-up-aws` starts Spark Thrift with Iceberg AWS Glue catalog and
S3FileIO against AWS S3 (no MinIO). Bronze Parquet must already exist in S3.
Airflow does not orchestrate the AWS Glue path yet.

Evidence reads committed Parquet snapshots; `make export-evidence-snapshots`
regenerates exact-name snapshots from selected lakehouse gold tables
(`housing_production`, `permit_pipeline`, `evictions`, `public_safety`,
`pipeline_health`, `data_trust`).
```

`promote_raw_to_bronze` snapshots eligible intervals from current S3 JSON ingest
metadata events (default: every pending complete interval, oldest first), then
drains that fixed snapshot. Compacted metadata Parquet is a query/reporting
layer, not promotion control flow; each compaction fully rebuilds it from JSON.
Attempt audit events do not drive the planner. Promotion snapshots live outside
the event prefixes and are not compacted.

Dataset ingest DAGs contain
`extract_<dataset>_to_raw -> record_<dataset>_extract_metadata -> ingest_complete`,
plus a failed-metadata marker task that emits the failure metadata asset only
after a failed extract metadata event is written.
`promote_raw_to_bronze` promotes raw intervals to bronze and compacts metadata.
It reads `LAKEHOUSE_PLAN_MODE`, optional `LAKEHOUSE_PLAN_MAX_INTERVALS`, and
optional start/end bounds, or the equivalent `plan_*` `dag_run.conf` values.
`LAKEHOUSE_PLAN_LIMIT` is retired and fails loudly when set; root Make targets
reject selected Airflow env files that still contain it. `max_active_runs=1`
prevents concurrent runs from selecting the same global backlog. Each drain
revalidates current events and bronze receipts so retries skip completed work.
An explicit max-intervals cap requires a later asset wake-up or another manual
promotion to process remaining work; the default unbounded drain is hands-off.
When no interval is selected, or no snapshot interval reaches complete receipts,
bronze promotion asset emission is skipped but metadata compaction still runs.
Intervals with a failed required dataset are not selected until the failed
current event is overwritten by a successful rerun of the same interval.
`make lakehouse-status` hides receipt-complete intervals unless passed `--all`.
Genesis automatic end selection stops on a failed earliest shared daily
interval. Deterministic failed genesis runs require explicit `--retry-failed`,
which clears and requeues every task in the affected dataset run.

Three ingest DAGs run at 06:00 UTC with `catchup=True` and `max_active_runs=1`.
`promote_raw_to_bronze` is asset-triggered by any ingest asset.
`build_lakehouse_gold` is asset-triggered by `BRONZE_PROMOTION_ASSET`, runs
`dbt debug` then `dbt build --select tag:lakehouse`
inside the Astro Airflow runtime against the mirrored project at
`airflow/include/dbt/`, and emits `GOLD_TRANSFORM_ASSET` when complete.
`handle_ingest_failure_metadata` is asset-triggered by failed ingest metadata,
compacts metadata, and runs `dbt build --select pipeline_health data_trust`. Local
Airflow containers need `DBT_SPARK_HOST=host.docker.internal` (and matching
`DBT_SPARK_PORT`) so dbt can reach the host-published Spark Thrift Server from
`make spark-up`.

## Extraction invariants

- Scheduled ingest runs use Airflow data intervals as the extraction boundary.
  Manual full/backfill runs supply trigger JSON in `dag_run.conf` (see
  `resolve_extract_window` in `_shared/dag_factory.py`):
  `load_mode` (`full` or `backfill`), `window_start`, `window_end`, and optional
  `lookback_hours` (default `0`). The explicit window bounds become
  `ExtractWindow.data_interval_start` and `data_interval_end`; `lookback_hours`
  widens only `effective_start`. Raw keys and ingest metadata use those bounds.
  To promote the same manual window, pass matching `plan_start` / `plan_end`
  in a manual `promote_raw_to_bronze` run; `make lakehouse-promote` does this
  through the Airflow REST API.
- Manual full/backfill runs do not require an Airflow logical date or scheduled
  data interval; the explicit JSON bounds are authoritative.
- `ExtractWindow` queries with a half-open `[effective_start, data_interval_end)`
  predicate.
- Normalize all internal timestamps to timezone-aware UTC.
- Page in configured timestamp order with the dataset key as a stable
  tie-breaker.
- Stream records; do not materialize a complete API response in memory.
- Raw files are NDJSON: one compact JSON object per line, no outer array.
- Raw keys encode both interval bounds and end in `records.ndjson`.
- Empty extracts are successful and upload no raw object.
- Raw records remain source-faithful. Reporting filters belong downstream.
- `max_loaded_at` is descriptive metadata; it does not control the next
  scheduled interval.
- Extract `started_at` and `completed_at` are captured inside `extract_to_raw`.

## dbt contracts

- `dbt/` is canonical; never edit the generated `airflow/include/dbt/` mirror.
- Local lakehouse dev uses the `lakehouse` uv group (`dbt-core`, `dbt-spark`).
- Use medallion dbt folders (`bronze/`, `silver/`, `gold/`). Do not mix
  `staging/`, `intermediate/`, `stg_*`, `int_*`, or `mart_*` in the lakehouse slice.
- `smoke_iceberg` is the harmless Iceberg proof model.
- Bronze dbt models (`bronze_permits`, `bronze_evictions`, `bronze_incidents`) are
  ephemeral read adapters over Python-promoted bronze Parquet (`LAKEHOUSE_BRONZE_BASE_URI`,
  default `s3a://lakehouse/lake/parquet/bronze`).
- Metadata dbt models (`metadata_ingest_runs`, `metadata_file_manifest`) are
  ephemeral read adapters over compacted metadata Parquet
  (`LAKEHOUSE_METADATA_BASE_URI`; when unset, dbt derives it from
  `LAKEHOUSE_BRONZE_BASE_URI` by replacing the trailing `/bronze` with
  `/metadata`).
- Silver Iceberg models: `permits_current`, `evictions_current`, `incidents_current`.
- Gold Iceberg models: `housing_production`, `permit_pipeline`, `evictions`,
  `public_safety`, `pipeline_health`, `data_trust`.
- `data_trust` includes daily interval coverage after a 30-hour grace period.
  Wide genesis intervals are excluded; permits/incidents gaps fail dbt while
  eviction gaps warn.
- `make dbt-lakehouse-gold` fixes that coverage cutoff to the checked-in local
  fixture interval for the Hadoop catalog only; AWS/Glue builds use the rolling
  production cutoff unless `DBT_INTERVAL_COVERAGE_CUTOFF_EPOCH` is set.
- The local Airflow environment's `AWS_ENDPOINT_URL` is container-facing.
  `make lakehouse-status`, `lakehouse-promote`, and `lakehouse-genesis` use
  `LAKEHOUSE_CLI_AWS_ENDPOINT_URL` when set so host-side commands reach MinIO.

## Airflow import boundary

Astro mounts `airflow/include/` at `/usr/local/airflow/include/`, and the
Dockerfile adds that directory to `PYTHONPATH`, so DAGs import project modules
as `from scripts ...`. Airflow parses `airflow/dags/`, so DAG-local helpers are
imported as `from _shared ...`.

## Dependency boundaries

- `pyproject.toml` controls local uv environments.
- `airflow/requirements.txt` controls additional packages in the Astro image,
  including `dbt-core` and `dbt-spark` for `build_lakehouse_gold`.

## Documentation maintenance

Use `$maintain-project-docs` before handing off a change that affects commands,
architecture, configuration, schemas, DAGs, storage, deployment, or
user-visible behavior.

Public documentation must describe checked-in behavior only. Do not publish
roadmaps, target-state diagrams, proposed designs, or private learning notes.
Run `make docs-check` before handoff. If documentation does not need a change,
state the reason in the handoff.
