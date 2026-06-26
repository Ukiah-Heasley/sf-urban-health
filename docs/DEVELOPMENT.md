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
for local lakehouse smoke tests without AWS. `transform_lakehouse` reads
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

## Commands

| Goal | Command |
| --- | --- |
| Show available targets | `make` |
| Extract permits to raw S3 | `make ingest` |
| Run Python tests | `make test` |
| Promote fixture NDJSON locally | `make lakehouse-smoke` |
| Reset local MinIO and seed permits fixture to bronze | `make lakehouse-prepare-permits-fixture` (destructive; requires `spark-up`) |
| Build/test permits silver Iceberg model | `make dbt-lakehouse-permits` |
| Start local MinIO + Spark Thrift | `make spark-up` |
| Stop local lakehouse stack | `make spark-down` |
| Verify dbt Spark profile | `make dbt-lakehouse-debug` |
| Run dbt smoke Iceberg model | `make dbt-lakehouse-smoke` |
| Lint Python | `make lint` |
| Lint YAML | `make yamllint` |
| Run pre-commit hooks | `make pre-commit` |
| Check documentation | `make docs-check` |
| Mirror dbt into Airflow | `make sync-dbt` |
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

`make airflow-up` first mirrors `dbt/` into `airflow/include/dbt/`, then runs
`astro dev start`. The local UI is <http://localhost:8080> with development
credentials `admin` / `admin`.

Scripts under `airflow/include/scripts/` are mounted into the containers. DAGs
import them through the Dockerfile's `PYTHONPATH` configuration.

## Testing

The default suite mocks HTTP and S3 boundaries. DAG tests skip automatically
when Airflow is not installed. CI installs the Airflow dependency group for a
separate DAG-integrity job.

Set `SKIP_DASHBOARD_TESTS=1` when local Arrow wheels cannot load; Linux CI still
runs the dashboard import probe.

## dbt and local Spark

Edit only top-level `dbt/`. The mirror under `airflow/include/dbt/` is generated,
gitignored, and replaced by `make sync-dbt`.

Local silver development uses the `lakehouse` uv dependency group (`dbt-core`,
`dbt-spark`). Start the Compose stack with `make spark-up`, then run
`make lakehouse-prepare-permits-fixture` to reset the local MinIO sandbox and
promote fixture permits bronze Parquet. That target deletes every object in the
local `lakehouse` bucket before reseeding fixture data and restarting Spark
Thrift so catalog namespaces are rebuilt. Run
`make dbt-lakehouse-debug`, `make dbt-lakehouse-permits`, or
`make dbt-lakehouse-smoke` as needed. Bronze remains Parquet in MinIO.
`permits_current` materializes as silver Iceberg through Spark/dbt. The smoke
model is tagged `smoke`, materializes as Iceberg, and stores warehouse data in
the local MinIO bucket through Spark S3A without reading bronze.
