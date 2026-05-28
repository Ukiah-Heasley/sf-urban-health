# Developer Setup

## Prerequisites

- **Python 3.11** — pinned in `pyproject.toml`. Use `uv` to manage the venv.
- **uv** — [astral.sh/uv](https://docs.astral.sh/uv/), the package manager.
- **Astro CLI** — for the local Airflow stack ([install](https://www.astronomer.io/docs/astro/cli/install-cli)).
- **Docker Desktop** — Astro spins up Postgres + Airflow components in containers.
- **AWS credentials + S3 bucket** — for the raw data lake.
- **Snowflake account + warehouse** — see Snowflake bootstrap in the repo README.

## Initial setup

```bash
git clone git@github.com:Ukiah-Heasley/sf-urban-health.git
cd sf-urban-health

# create venv with all groups (airflow, dbt, dashboard, dev)
uv sync --all-groups

# fill in secrets
cp airflow/.env.example airflow/.env
$EDITOR airflow/.env
```

## Running things

| Goal | Command |
|---|---|
| Lint Python | `make lint` |
| Lint YAML | `make yamllint` |
| Run all pre-commit hooks | `make pre-commit` |
| Run tests | `make test` |
| Trigger one ingest | `make ingest` |
| Build dbt | `make dbt-build` |
| Test dbt only | `make dbt-test` |
| Start local Airflow | `make airflow-up` (`http://localhost:8080`, admin/admin) |
| Stop local Airflow | `make airflow-down` |
| Tail Airflow logs | `make airflow-logs` |
| Run dashboard | `make dashboard-dev` (`http://localhost:8050`) |

Set `SKIP_DASHBOARD_TESTS=1` if your local venv has wheel-loadability
issues with `pyarrow` / `polars` / `snowflake-connector-python` (e.g.
bleeding-edge macOS arm64) — the test suite will skip the dashboard
import probe.

## Lint stack

- **Ruff** — Python lint + format. Pinned in `[dependency-groups].dev`.
- **SQLFluff** — SQL lint with the **dbt** templater + **Snowflake**
  dialect. Config in `.sqlfluff`. Run `uv run sqlfluff lint dbt/models`
  for an ad-hoc check; pre-commit handles the per-file path.
- **yamllint** — YAML lint. Config in `.yamllint`.
- **pre-commit** — Hook stack in `.pre-commit-config.yaml`. Default
  stage runs ruff + yamllint + sqlfluff-lint + the standard
  pre-commit-hooks set (eof, trailing whitespace, large-file guard).

The `manual` stage holds dbt-checkpoint hooks (model description /
column doc / test coverage). They need a compiled dbt manifest, so
they're not in the default stage. Run them with:

```bash
uv run pre-commit run --hook-stage manual --all-files
```

## CI

Three GitHub Actions workflows:

- `ci.yml` — Ruff + yamllint + pytest (dev group), pre-commit, gitleaks,
  DAG-integrity (separate Airflow-installed job).
- `dbt-ci.yml` — `dbt deps` + `dbt parse` + `dbt compile` against
  Snowflake (no models run, no data written).

## Secrets handling

Keep all credentials in `airflow/.env` — gitignored.
**Never** commit `airflow/.env` or `airflow/airflow_settings.yaml`.
The leak audit on the May 2026 polish pass verified no live values are
in any tracked file or git history. `gitleaks` runs on every PR
to keep it that way.

## Repository layout

```
.
├── airflow/                   # Astro project + dags + ingest scripts
│   ├── dags/                  # 5 DAGs (3 ingests + transform_all + ingest_pipeline_metadata)
│   ├── include/
│   │   ├── scripts/           # SODA ingest engine + Airflow REST client
│   │   ├── sql/               # SQL templates loaded by Airflow operators
│   │   └── dbt/               # rsync mirror of dbt/ — gitignored
│   └── Dockerfile             # Astro Runtime image
├── dbt/                       # dbt project — staging / intermediate / marts / metadata
├── dashboard/                 # Plotly Dash multi-page app + Dockerfile
├── snowflake/                 # one-off bootstrap SQL
├── tests/                     # pytest unit tests
├── wiki/                      # versioned wiki source — published to GitHub Wiki
├── docs/                      # operator-facing docs
├── .github/workflows/         # CI
├── .pre-commit-config.yaml    # lint stack
├── .sqlfluff / .sqlfluffignore / .yamllint
└── pyproject.toml             # uv-managed Python deps
```
