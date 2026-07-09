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
cp airflow/.env.local.example airflow/.env.local
cp lakehouse/.env.local.example lakehouse/.env.local
```

Private mode files are gitignored: `airflow/.env.local`, `airflow/.env.aws`,
`lakehouse/.env.local`, and `lakehouse/.env.aws`. Make targets copy or pass them
without commenting shared blocks:

| Mode | Airflow | Lakehouse Compose |
| --- | --- | --- |
| Local MinIO | `make airflow-up-local` copies `.env.local` → `airflow/.env` | `make spark-up` uses `lakehouse/.env.local` |
| AWS | `make airflow-up-aws` copies `.env.aws` → `airflow/.env` | `make spark-up-aws` uses `lakehouse/.env.aws` |

`make airflow-up` is an alias for `make airflow-up-local`. Astro always reads
gitignored `airflow/.env`; ad-hoc Airflow CLI targets source it when they run.
The extractor requires `AWS_S3_BUCKET`; `DATASF_APP_TOKEN` is optional. Set
`LAKE_LOCAL_ROOT` for local lakehouse smoke tests without AWS. Local templates
set `AWS_ENDPOINT_URL` for MinIO; AWS templates use real S3 credentials.
`promote_raw_to_bronze` reads `LAKEHOUSE_PLAN_MODE`, `LAKEHOUSE_PLAN_LIMIT`
(must be `1`), and optional `LAKEHOUSE_PLAN_START` / `LAKEHOUSE_PLAN_END` to
plan intervals from current S3 JSON ingest metadata events. After a manual
full/backfill ingest, set matching plan start/end bounds when you need promotion
to select that same window.

Local dbt and fixture targets source `lakehouse/.env.local` by default. Pass
`LAKEHOUSE_ENV_FILE=lakehouse/.env.aws` for AWS Glue proof dbt runs after
`make spark-up-aws`.

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
| Export Evidence Parquet snapshots from selected lakehouse gold | `make export-evidence-snapshots` |
| Start local MinIO + Spark Thrift | `make spark-up` |
| Start Spark Thrift in AWS Glue catalog mode | `make spark-up-aws` |
| Stop local lakehouse stack | `make spark-down` |
| Verify dbt Spark profile | `make dbt-lakehouse-debug` |
| Run dbt smoke Iceberg model | `make dbt-lakehouse-smoke` |
| Lint Python | `make lint` |
| Lint YAML | `make yamllint` |
| Run pre-commit hooks | `make pre-commit` |
| Check documentation | `make docs-check` |
| Mirror dbt and contracts into Airflow | `make sync-dbt` |
| Start local Airflow (local env) | `make airflow-up-local` or `make airflow-up` |
| Start local Airflow (AWS env) | `make airflow-up-aws` |
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

### Manual full/backfill ingest in Airflow

Scheduled ingest DAGs keep using the Airflow data interval. To run a bounded
historical window, trigger `ingest_permits`, `ingest_evictions`, or
`ingest_incidents` with JSON conf:

```json
{
  "load_mode": "full",
  "window_start": "2018-01-01T00:00:00Z",
  "window_end": "2026-06-29T00:00:00Z",
  "lookback_hours": 0
}
```

`load_mode` may be `"full"` or `"backfill"`. Both require `window_start` and
`window_end` together; `lookback_hours` is optional and defaults to `0`. The
extract task writes raw NDJSON and ingest metadata under the normal interval key
shape using the explicit window bounds. When promotion should target that same
window, set matching `LAKEHOUSE_PLAN_START` and `LAKEHOUSE_PLAN_END` in the
Airflow environment before triggering or waiting for `promote_raw_to_bronze`.

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
   `minio-init` service creates the `lakehouse` bucket from `lakehouse/.env.local`.
2. Configure `airflow/.env.local` for MinIO-compatible S3 (see
   `airflow/.env.local.example`):
   - `AWS_ACCESS_KEY_ID=minioadmin`
   - `AWS_SECRET_ACCESS_KEY=minioadmin`
   - `AWS_S3_BUCKET=lakehouse` (must match `LAKEHOUSE_BUCKET` in
     `lakehouse/.env.local`)
   - `AWS_ENDPOINT_URL=http://host.docker.internal:9000` for Astro containers
     reaching the host-published MinIO API port (`MINIO_API_PORT`, default
     `9000`). Use `http://localhost:9000` for host CLI runs such as
     `make ingest`.
3. Start Airflow with `make airflow-up-local` (or `make airflow-up`).
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
   `airflow/.env.local` to match the host port from `make spark-up`. After bronze
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
`make dbt-lakehouse-smoke` as needed. Bronze remains Parquet in object storage;
dbt reads it through `LAKEHOUSE_BRONZE_BASE_URI` (default
`s3a://lakehouse/lake/parquet/bronze`). dbt reads compacted metadata through
`LAKEHOUSE_METADATA_BASE_URI`; when unset, dbt derives it from
`LAKEHOUSE_BRONZE_BASE_URI` by replacing the trailing `/bronze` with
`/metadata`. dbt uses medallion folders (`bronze/`, `silver/`, `gold/`) with
ephemeral bronze and metadata read adapters plus Iceberg silver/gold tables.
The smoke model is tagged `smoke`, materializes as Iceberg, and stores
warehouse data in the configured catalog warehouse without reading bronze.

### AWS Glue catalog proof

`make spark-up-aws` starts only Spark Thrift with `LAKEHOUSE_CATALOG=glue`. It
does not start MinIO, does not use Glue crawlers or Glue ETL jobs, and is not
orchestrated from Airflow. Bronze and compacted metadata Parquet must already
exist in AWS S3. dbt reads bronze from `LAKEHOUSE_BRONZE_BASE_URI` and reads
metadata from `LAKEHOUSE_METADATA_BASE_URI` when set, otherwise from the
matching `/metadata` path beside the bronze prefix. From the repository root
with AWS credentials in `lakehouse/.env.aws`:

```bash
make spark-up-aws
make dbt-lakehouse-debug LAKEHOUSE_ENV_FILE=lakehouse/.env.aws
make dbt-lakehouse-gold LAKEHOUSE_ENV_FILE=lakehouse/.env.aws
make spark-down
```

Iceberg silver and gold tables register in the AWS Glue Data Catalog under
`DBT_SPARK_SCHEMA` (default `sf_urban_health`). The Iceberg warehouse uses
`s3://` at `LAKEHOUSE_WAREHOUSE_URI`; bronze Parquet reads use `s3a://` at
`LAKEHOUSE_BRONZE_BASE_URI`, and metadata Parquet reads use
`LAKEHOUSE_METADATA_BASE_URI` or the derived sibling metadata path.

### Evidence snapshot export

After lakehouse gold models build, regenerate the committed Evidence snapshots
from the selected Spark/Iceberg catalog. Local export:

```bash
make spark-up
make lakehouse-prepare-fixtures
make dbt-lakehouse-gold
make export-evidence-snapshots
```

AWS Glue export:

```bash
make spark-up-aws
make dbt-lakehouse-gold LAKEHOUSE_ENV_FILE=lakehouse/.env.aws
make export-evidence-snapshots LAKEHOUSE_ENV_FILE=lakehouse/.env.aws
```

`airflow/include/scripts/evidence_snapshots.py` connects to Spark Thrift,
exports exact-name Parquet snapshots for `housing_production`,
`permit_pipeline`, `evictions`, `public_safety`, `pipeline_health`, and
`data_trust` from gold Iceberg tables in `DBT_SPARK_SCHEMA`.

`reports/scripts/make_sample_data.py` remains a fallback demo-data generator
when Spark is unavailable. GitHub Pages builds from committed snapshots only.
