# Development

## Prerequisites

- Python `>=3.11,<3.12`
- [uv](https://docs.astral.sh/uv/)
- Docker Desktop and Astro CLI for Airflow
- Node.js 20 for the Evidence site
- AWS credentials for real extraction
- Snowflake credentials for dbt, pipeline metadata, Dash, and real report export

## Environment

```bash
uv sync --all-groups
cp airflow/.env.example airflow/.env
```

`airflow/.env` is gitignored and loaded by the root Makefile. The extractor
requires `AWS_S3_BUCKET`; `DATASF_APP_TOKEN` is optional. Snowflake consumers use
the `SNOWFLAKE_*` variables documented in the example file.

## Commands

| Goal | Command |
| --- | --- |
| Show available targets | `make` |
| Extract permits to raw S3 | `make ingest` |
| Run Python tests | `make test` |
| Lint Python | `make lint` |
| Lint YAML | `make yamllint` |
| Run pre-commit hooks | `make pre-commit` |
| Check documentation | `make docs-check` |
| Install dbt packages | `make dbt-deps` |
| Run dbt dev models | `make dbt-run` |
| Build and test dbt dev models | `make dbt-build` |
| Run dbt tests | `make dbt-test` |
| Mirror dbt into Airflow | `make sync-dbt` |
| Start local Airflow | `make airflow-up` |
| Stop local Airflow | `make airflow-down` |
| Tail scheduler logs | `make airflow-logs` |
| Run Plotly Dash | `make dashboard-dev` |
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

Set `SKIP_DASHBOARD_TESTS=1` when local Arrow/Snowflake wheels cannot load; Linux
CI still runs the dashboard import probe.

## dbt mirror

Edit only top-level `dbt/`. The mirror under `airflow/include/dbt/` is generated,
gitignored, and replaced by `make sync-dbt`.
