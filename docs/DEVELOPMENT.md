# Development

## Prerequisites

- Python `>=3.11,<3.12`
- [uv](https://docs.astral.sh/uv/)
- Docker Desktop and Astro CLI for Airflow
- Docker Desktop for the local lakehouse stack
- Node.js 20 for the Evidence site
- AWS credentials for real extraction

## Environment

```bash
uv sync --all-groups
cp airflow/.env.example airflow/.env
cp lakehouse/.env.example lakehouse/.env
```

`airflow/.env` is gitignored and loaded by the root Makefile. The extractor
requires `AWS_S3_BUCKET`; `DATASF_APP_TOKEN` is optional. Set `LAKE_LOCAL_ROOT`
for local lakehouse smoke tests without AWS. Set `AWS_ENDPOINT_URL` or
`AWS_S3_ENDPOINT_URL` to point ingest and promotion code at an S3-compatible
endpoint such as the local MinIO stack. `promote_raw_to_bronze` reads
`LAKEHOUSE_PLAN_MODE`, `LAKEHOUSE_PLAN_LIMIT` (must be `1`), and optional
`LAKEHOUSE_PLAN_START` / `LAKEHOUSE_PLAN_END` to plan intervals from current
S3 JSON ingest metadata events.

`lakehouse/.env` configures local MinIO and Spark Thrift host ports. The root
Makefile loads it for `spark-up`, fixture prep, and lakehouse dbt targets.
`make spark-up` creates the file from `lakehouse/.env.example` when missing.
Spark Thrift always listens on container port `10000`; `SPARK_THRIFT_PORT` selects
the host port mapped by Compose. When `DBT_SPARK_PORT` is unset, the Makefile
exports it from `SPARK_THRIFT_PORT`, or `10000` when that is also unset. dbt also
reads `DBT_SPARK_HOST`,
`DBT_SPARK_USER`, and `DBT_SPARK_SCHEMA` from the environment. The checked-in
profile uses `auth: NOSASL` to match the local Thrift Server configuration.
For dbt from Astro Airflow containers, set `DBT_SPARK_HOST=host.docker.internal`
in `airflow/.env` so tasks in `build_lakehouse_gold` can reach the host-published
Spark Thrift port.

## Commands

| Goal | Command |
| --- | --- |
| Show available targets | `make` |
| Extract permits to raw S3 | `make ingest` |
| Run Python tests | `make test` |
| Promote fixture NDJSON locally | `make lakehouse-smoke` |
| Reset local MinIO and seed all dataset fixtures to bronze | `make lakehouse-prepare-fixtures` (destructive; requires `spark-up`; `lakehouse-prepare-permits-fixture` is an alias) |
| Build/test permits silver Iceberg model | `make dbt-lakehouse-permits` |
| Build/test all lakehouse bronze/silver/gold Iceberg models | `make dbt-lakehouse-gold` |
| Export Evidence Parquet snapshots from local lakehouse gold | `make export-evidence-snapshots` |
| Start local MinIO + Spark Thrift | `make spark-up` |
| Stop local lakehouse stack | `make spark-down` |
| Verify dbt Spark profile | `make dbt-lakehouse-debug` |
| Run dbt smoke Iceberg model | `make dbt-lakehouse-smoke` |
| Lint Python | `make lint` |
| Lint YAML | `make yamllint` |
| Run pre-commit hooks | `make pre-commit` |
| Check documentation | `make docs-check` |
| Mirror dbt and contracts into Airflow | `make sync-dbt` |
| Start local Airflow | `make airflow-up` |
| Stop local Airflow | `make airflow-down` |
| Tail scheduler logs | `make airflow-logs` |
| Run Plotly Dash shell | `make dashboard-dev` |
| Build the Dash image | `make dashboard-docker` |

## Targeted extraction

The standalone dataset scripts accept ISO-8601 interval boundaries:

```bash
uv run airflow/include/scripts/permits.py \
  --window-start 2024-01-01T00:00:00Z \
  --window-end 2024-02-01T00:00:00Z \
  --lookback-hours 0
```

When omitted, the start defaults to the dataset epoch and the end defaults to
the current UTC time.

## Airflow

`make airflow-up` first mirrors `dbt/` and `contracts/` into
`airflow/include/`, then runs `astro dev start`. The local UI is
<http://localhost:8080> with development credentials `admin` / `admin`.

Scripts under `airflow/include/scripts/` are mounted into the containers. DAGs
import them through the Dockerfile's `PYTHONPATH` configuration.

### Local MinIO Airflow smoke

This path exercises ingest and bronze promotion against the lakehouse MinIO
stack without real AWS S3. It does not reset the bucket; avoid
`make lakehouse-prepare-fixtures` unless you explicitly want a destructive
local reseed.

1. Start local MinIO (and Spark Thrift) with `make spark-up`. The
   `minio-init` service creates the `lakehouse` bucket from `lakehouse/.env`.
2. Configure `airflow/.env` for MinIO-compatible S3:
   - `AWS_ACCESS_KEY_ID=minioadmin`
   - `AWS_SECRET_ACCESS_KEY=minioadmin`
   - `AWS_S3_BUCKET=lakehouse` (must match `LAKEHOUSE_BUCKET` in
     `lakehouse/.env`)
   - `AWS_ENDPOINT_URL=http://host.docker.internal:9000` for Astro containers
     reaching the host-published MinIO API port (`MINIO_API_PORT`, default
     `9000`). Use `http://localhost:9000` for host CLI runs such as
     `make ingest`.
3. Start Airflow with `make airflow-up`.
4. In the UI at <http://localhost:8080>, trigger `ingest_permits`,
   `ingest_evictions`, and `ingest_incidents` for the **same** logical date so
   all three share one daily interval. Each DAG runs
   `extract_{dataset}_to_raw -> record_{dataset}_extract_metadata ->
   ingest_complete`.
5. After all three ingest assets update, `promote_raw_to_bronze` runs
   automatically. Confirm it promotes the interval, compacts metadata, and
   emits `bronze_promotion_complete` rather than `bronze_promotion_noop`.
6. Verify objects in MinIO (console at <http://localhost:9001> or `mc` against
   `http://localhost:9000`):
   - raw NDJSON under `raw/{permits,evictions,incidents}/data_interval_start=.../`
   - current ingest metadata JSON under `lake/metadata/events/`
   - bronze Parquet under `lake/parquet/bronze/`
   - file-manifest metadata events under `lake/metadata/events/`
   - compacted metadata Parquet under `lake/parquet/metadata/ingest_runs/` and
     `lake/parquet/metadata/file_manifest/`
7. Trigger `promote_raw_to_bronze` again (or wait for the next asset-driven
   run). With default `LAKEHOUSE_PLAN_MODE=pending` and no remaining pending
   complete interval, the DAG should branch to `bronze_promotion_noop` and skip bronze
   promotion and compaction.
8. Set `DBT_SPARK_HOST=host.docker.internal` and `DBT_SPARK_PORT` in
   `airflow/.env` to match the host port from `make spark-up`. After bronze
   promotion completes, `build_lakehouse_gold` runs automatically. Confirm
   `dbt_debug`, `dbt_build_lakehouse_gold`, and `lakehouse_gold_complete`
   succeed and emit the gold transform completion asset.

## Testing

The default suite mocks HTTP and S3 boundaries. DAG tests skip automatically
when Airflow is not installed. CI installs the Airflow dependency group for a
separate DAG-integrity job.

Set `SKIP_DASHBOARD_TESTS=1` when local Arrow wheels cannot load; Linux CI still
runs the dashboard import probe.

## dbt and local Spark

Edit only top-level `dbt/`. The mirror under `airflow/include/dbt/` is generated,
gitignored, and replaced by `make sync-dbt`.

Local lakehouse development uses the `lakehouse` uv dependency group (`dbt-core`,
`dbt-spark`). Start the Compose stack with `make spark-up`, then run
`make lakehouse-prepare-fixtures` to reset the local MinIO sandbox and promote
fixture bronze Parquet for permits, evictions, and incidents. That target deletes
every object in the local `lakehouse` bucket before reseeding fixture data and
restarting Spark Thrift so catalog namespaces are rebuilt. Run
`make dbt-lakehouse-debug`, `make dbt-lakehouse-permits`, `make dbt-lakehouse-gold`, or
`make dbt-lakehouse-smoke` as needed. Bronze remains Parquet in MinIO. dbt uses
medallion folders (`bronze/`, `silver/`, `gold/`) with ephemeral bronze read
adapters and Iceberg silver/gold tables. The smoke model is tagged `smoke`,
materializes as Iceberg, and stores warehouse data in the local MinIO bucket
through Spark S3A without reading bronze.

### Evidence snapshot export

After the local lakehouse gold models build, regenerate the committed Evidence
snapshots with:

```bash
make spark-up
make lakehouse-prepare-fixtures
make dbt-lakehouse-gold
make export-evidence-snapshots
```

`airflow/include/scripts/evidence_snapshots.py` connects to Spark Thrift,
exports `mart_housing_production.parquet` from the gold Iceberg table
`sf_urban_health.housing_production`, and writes deterministic observability
snapshots for `mart_pipeline_health.parquet` and `mart_data_trust.parquet`.
Lakehouse metadata Parquet is not registered in the Spark catalog, so those two
observability marts remain generated locally in this slice.

`reports/scripts/make_sample_data.py` remains the fallback demo-data generator
when Spark is unavailable. GitHub Pages builds from committed snapshots only.
