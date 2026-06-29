# Repository guidance for coding agents working on SF Urban Health.

## Commands

Use root `Makefile` targets; it loads `airflow/.env` when present.

```bash
make ingest              # permits: DataSF -> S3 raw NDJSON
make sync-dbt            # mirror dbt/ and contracts/ into airflow/include/
make airflow-up          # sync Airflow assets, then astro dev start
make airflow-down
make airflow-logs
make spark-up            # local MinIO + Spark Thrift Server
make spark-down
make dbt-lakehouse-debug
make dbt-lakehouse-smoke
make lakehouse-prepare-fixtures       # destructive local MinIO reset + all dataset fixtures
make lakehouse-prepare-permits-fixture  # alias for lakehouse-prepare-fixtures
make dbt-lakehouse-permits              # build/test permits_current silver Iceberg
make dbt-lakehouse-gold                 # build/test all lakehouse bronze/silver/gold Iceberg
make dashboard-dev       # http://localhost:8050
make dashboard-docker
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
  -> lakehouse_metadata.py current + attempt ingest events -> ingest assets

ingest assets -> transform_lakehouse -> bronze Parquet + metadata events
  -> compact metadata Parquet -> lakehouse transform asset (when planned)

lakehouse/ Compose -> MinIO + Spark Thrift + Iceberg
  -> fixture prep -> bronze Parquet in MinIO -> dbt bronze/silver/gold Iceberg
  -> dbt smoke Iceberg model (local dev only)

Plotly Dash and Evidence remain consumer shells over empty frames or committed
sample Parquet until lakehouse consumer wiring lands.
```

`transform_lakehouse` plans intervals from current S3 JSON ingest metadata
events (default: oldest pending complete interval, one interval per DAG run).
Compacted metadata Parquet is a query/reporting layer, not promotion control
flow; each compaction fully rebuilds it from JSON. Attempt audit events do not
drive the planner.

Dataset ingest DAGs contain
`extract_<dataset>_to_raw -> record_<dataset>_extract_metadata -> ingest_complete`.
`transform_lakehouse` promotes raw intervals to bronze and compacts metadata.
It reads `LAKEHOUSE_PLAN_MODE`, `LAKEHOUSE_PLAN_LIMIT` (must be `1`), and
optional start/end bounds. `max_active_runs=1` prevents concurrent runs from
selecting the same global interval. When no interval is selected, promotion and
lakehouse asset emission are skipped.

Three ingest DAGs run at 06:00 UTC. `transform_lakehouse` is asset-triggered by
all three ingest assets.

## Extraction invariants

- Airflow data intervals are the extraction boundary. `ExtractWindow` uses a
  half-open `[effective_start, data_interval_end)` predicate.
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
  ephemeral read adapters over Python-promoted MinIO Parquet.
- Silver Iceberg models: `permits_current`, `evictions_current`, `incidents_current`.
- Gold Iceberg models: `housing_production`, `permit_pipeline`, `evictions`, `public_safety`.

## Airflow import boundary

Astro mounts `airflow/include/` at `/usr/local/airflow/include/`. The Dockerfile
adds that directory to `PYTHONPATH`, so DAGs import project modules as
`from scripts ...` and `from pipeline_assets ...`.

## Dependency boundaries

- `pyproject.toml` controls local uv environments.
- `airflow/requirements.txt` controls additional packages in the Astro image.
- Dashboard imports may be skipped locally with `SKIP_DASHBOARD_TESTS=1` when
  platform wheels cannot load. Linux CI exercises the import.

## Documentation maintenance

Use `$maintain-project-docs` before handing off a change that affects commands,
architecture, configuration, schemas, DAGs, storage, deployment, or
user-visible behavior.

Public documentation must describe checked-in behavior only. Do not publish
roadmaps, target-state diagrams, proposed designs, or private learning notes.
Run `make docs-check` before handoff. If documentation does not need a change,
state the reason in the handoff.
