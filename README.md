# SF Urban Health Pipeline — Phase 1

[![CI](https://github.com/Ukiah-Heasley/sf-urban-health/actions/workflows/ci.yml/badge.svg)](https://github.com/Ukiah-Heasley/sf-urban-health/actions/workflows/ci.yml)
[![dbt CI](https://github.com/Ukiah-Heasley/sf-urban-health/actions/workflows/dbt-ci.yml/badge.svg)](https://github.com/Ukiah-Heasley/sf-urban-health/actions/workflows/dbt-ci.yml)
![Python](https://img.shields.io/badge/python-3.11-3776AB?logo=python&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.9-FF694B?logo=dbt&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-warehouse-29B5E8?logo=snowflake&logoColor=white)
![Ruff](https://img.shields.io/badge/lint-ruff-261230?logo=ruff&logoColor=white)
![SQLFluff](https://img.shields.io/badge/sql-sqlfluff-25D366)
[![Live demo](https://img.shields.io/badge/live%20demo-GitHub%20Pages-2ea44f)](https://ukiah-heasley.github.io/sf-urban-health/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-style daily ETL pipeline that ingests SF civic datasets (building permits, eviction notices, police incidents) from the DataSF SODA API, lands raw JSON in S3, loads into Snowflake, and transforms through a dbt staging → intermediate → mart layer on an Airflow schedule. Phase 1 of a larger SF Civic Intelligence Platform.

**What this demonstrates:** watermark-driven incremental ingestion to a durable S3 raw lake; ELT into Snowflake with dbt (staging → intermediate → marts) plus data-quality tests and source freshness; an Airflow-native observability loop (DAG / test health → composite trust score); a Plotly Dash app; and a static [Evidence](https://evidence.dev) snapshot published to GitHub Pages (**[live demo](https://ukiah-heasley.github.io/sf-urban-health/)**).

## Architecture

```
DataSF SODA API (permits / evictions / incidents)
      │
      ▼
Python extractor  (airflow/include/scripts/{permits,evictions,incident_reports}.py)
      │
      ▼
S3  raw/{dataset}/YYYY/MM/DD/{dataset}.json
      │
      ▼
Snowflake RAW.{PERMITS|EVICTIONS|INCIDENTS}  (COPY INTO, orchestrated by Airflow)
      │
      ▼
METADATA.INGEST_WATERMARKS  (high-water mark per dataset)
      │
      ▼
dbt  staging → intermediate → marts  (transform_all DAG)
```

| Stage         | Tool                              |
|---------------|-----------------------------------|
| Orchestration | Apache Airflow (Astro Runtime)    |
| Ingestion     | Python 3.11 + `requests`          |
| Raw lake      | AWS S3                            |
| Warehouse     | Snowflake                         |
| Transform     | dbt-core + dbt-snowflake          |
| Tests         | dbt generic tests + `dbt_utils`   |

## DAG architecture

The pipeline uses **five Airflow DAGs** with a clear separation of concerns:

**Three ingest DAGs** (one per dataset, run at 06:00 UTC daily):
```
extract_{dataset}_to_s3 → load_s3_to_snowflake → update_watermark
```

**One shared transform DAG** (`transform_all`, also scheduled at 06:00 UTC):
```
wait_permits_watermark  ─┐
wait_evictions_watermark ─┼─► dbt_deps ─► run_dbt_staging ─► run_dbt_marts
wait_incidents_watermark ─┘
```

`transform_all` uses `ExternalTaskSensor` to wait for each dataset's `update_watermark` task before running dbt once across all models. Sensors use `mode="reschedule"` (no worker slot held while waiting), `poke_interval=120s`, `timeout=3h`. (A known quirk with this fan-in pattern is tracked in `TODO.md` under "Airflow code quality" — B1.)

**One observability DAG** (`ingest_pipeline_metadata`, daily at 07:00 UTC):
```
collect_pipeline_metadata
```

A single `collect_pipeline_metadata` task pulls Airflow's REST API for DAG-run + task-instance state and writes to `METADATA.AIRFLOW_DAG_RUNS` / `METADATA.AIRFLOW_TASK_INSTANCES`, where dbt builds the four observability marts that drive the Pipeline Health, Eng Health, and Data Trust dashboard pages.

## Repository layout

```
.
├── dbt/                              dbt project — canonical location, edit here
│   ├── dbt_project.yml
│   ├── packages.yml                  installs dbt_utils + elementary
│   ├── profiles.yml                  env-var-driven; safe to commit
│   ├── macros/
│   └── models/{staging,intermediate,marts,metadata}/
├── airflow/                          Astro CLI project root
│   ├── Dockerfile                    Astro Runtime image (Airflow 3)
│   ├── dags/
│   │   ├── dag_factory.py            Factory for ingest DAGs
│   │   ├── ingest_permits.py         Permits ingest DAG
│   │   ├── ingest_evictions.py       Evictions ingest DAG
│   │   ├── ingest_incidents.py       Incidents ingest DAG
│   │   ├── transform_all.py          Shared dbt transform DAG
│   │   └── ingest_pipeline_metadata.py  Airflow → Snowflake observability
│   ├── include/scripts/
│   │   ├── soda_ingest.py            Generic SODA API engine
│   │   ├── permits.py / evictions.py / incident_reports.py
│   │   └── airflow_rest_client.py    Airflow REST API → Snowflake
│   ├── include/sql/                  SQL templates loaded by Airflow operators
│   ├── include/dbt/                  Mirror of dbt/ (gitignored, refreshed by `make sync-dbt`)
│   ├── requirements.txt              Python deps inside the Airflow image
│   └── .env.example                  Snowflake / AWS / DataSF credentials template
├── dashboard/                        Plotly Dash multi-page app (6 pages, in-memory Polars)
├── snowflake/                        One-shot bootstrap SQL
├── tests/                            pytest unit tests (dev group)
├── wiki/                             Versioned wiki source (synced to GitHub Wiki)
├── docs/                             Operator-facing docs (architecture, deploy, wiki sync)
├── .github/workflows/                CI: lint + test + pre-commit + gitleaks + dag-integrity + dbt
├── .pre-commit-config.yaml           Ruff + yamllint + sqlfluff + dbt-checkpoint
├── .sqlfluff / .sqlfluffignore       Snowflake-dialect SQL lint config
├── .yamllint                         YAML lint config
├── LICENSE                           MIT
├── Makefile                          One-line entrypoints for every workflow
├── README.md (this file)
├── TODO.md                           Tracked follow-ups, including deferred Airflow + dbt audit
└── pyproject.toml                    uv-managed Python deps
```

> **Why the dbt mirror?** Astro CLI's Docker build context is `airflow/`, so the image can only `COPY` from inside that directory. To keep `dbt/` as a true top-level peer, `make sync-dbt` rsyncs `dbt/` into `airflow/include/dbt/` before the image is built. `make airflow-up` runs the sync automatically; never edit the mirror by hand.

## Prerequisites

- Python 3.11 + [uv](https://docs.astral.sh/uv/)
- Docker Desktop + [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
- Snowflake account ([signup.snowflake.com](https://signup.snowflake.com))
- AWS account with an S3 bucket
- Free DataSF app token ([data.sfgov.org/profile/app_tokens](https://data.sfgov.org/profile/app_tokens))

## Setup

```bash
# 1. Credentials
cp airflow/.env.example airflow/.env
# fill in Snowflake, AWS, and DataSF values

# 2. Snowflake bootstrap (run once) — creates the database, schemas, S3 stage,
#    RAW + METADATA tables. See `Snowflake bootstrap` below.

# 3. dbt profile is rendered from airflow/.env via env_var() in
#    dbt/profiles.yml — no separate profile file needed.
```

### Snowflake bootstrap

Run [`snowflake/bootstrap.sql`](snowflake/bootstrap.sql) once (a Snowflake
worksheet, or `snow sql -f snowflake/bootstrap.sql`). It is idempotent and
creates the `SF_URBAN_HEALTH` database, the five schemas (`RAW`, `STAGING`,
`INTERMEDIATE`, `MARTS`, `METADATA`), the S3 external stage, the three `RAW.*`
VARIANT landing tables, `METADATA.INGEST_WATERMARKS`, and the two observability
tables. Fill in the S3 stage credentials at the top of the file first.

The `dbt` package set installed by `make dbt-deps` includes `dbt-labs/dbt_utils` and `elementary-data/elementary` — Elementary writes its observability tables on `dbt run`/`dbt test` (see TODO.md D4 for the on-run-end hook follow-up).

## Running it

Every workflow has a `make` target — `airflow/.env` is loaded automatically.

```bash
make dbt-deps       # install dbt packages (one-time, includes elementary)
make dbt-build      # run + test all dbt models
# After first dbt-deps, run elementary once to create its schema:
# cd dbt && dbt run --select elementary --profiles-dir . --target prod
make airflow-up     # start the local Airflow stack
make airflow-down
make airflow-logs   # tail scheduler logs
make lint
make test
```

The Airflow UI runs at [http://localhost:8080](http://localhost:8080) (`admin` / `admin`). Unpause all five DAGs (`ingest_permits`, `ingest_evictions`, `ingest_incidents`, `transform_all`, `ingest_pipeline_metadata`) to enable the full daily pipeline at 06:00 UTC plus the observability collector at 07:00 UTC.

### Initial backfill

For first-time setup, run each dataset's extractor via CLI in yearly chunks to avoid memory pressure (datasets with 500k+ records can approach 2 GB of RAM if loaded in a single run):

```bash
# Example: backfill permits year by year
uv run airflow/include/scripts/permits.py --since 2013-01-01 --run-date 2014-01-01
uv run airflow/include/scripts/permits.py --since 2014-01-01 --run-date 2015-01-01
# ... continue through to present
```

After each chunk loads into Snowflake the `METADATA.INGEST_WATERMARKS` row advances automatically on the next DAG run.

## Sample mart queries

Top neighborhoods by residential permits in the last year:

```sql
SELECT neighborhood,
       SUM(permits_filed)   AS permits,
       SUM(proposed_units)  AS proposed_units,
       SUM(net_units_added) AS net_units_added
FROM SF_URBAN_HEALTH.MARTS.MART_HOUSING_PRODUCTION
WHERE filed_month >= DATEADD(year, -1, CURRENT_DATE())
GROUP BY neighborhood
ORDER BY permits DESC
LIMIT 10;
```

Average days from filing to issuance, by supervisor district:

```sql
SELECT supervisor_district,
       ROUND(AVG(avg_days_to_issue), 1)    AS avg_days_to_issue,
       ROUND(AVG(median_days_to_issue), 1) AS median_days_to_issue
FROM SF_URBAN_HEALTH.MARTS.MART_HOUSING_PRODUCTION
WHERE filed_month >= DATEADD(year, -1, CURRENT_DATE())
GROUP BY supervisor_district
ORDER BY supervisor_district;
```

Status mix per month:

```sql
SELECT filed_month,
       SUM(permits_filed)     AS filed,
       SUM(permits_issued)    AS issued,
       SUM(permits_completed) AS completed,
       SUM(permits_expired)   AS expired
FROM SF_URBAN_HEALTH.MARTS.MART_HOUSING_PRODUCTION
GROUP BY filed_month
ORDER BY filed_month DESC;
```

## Design decisions

- **S3 is the durable raw layer.** Raw JSON lives in S3 permanently; Snowflake `RAW.*` tables are loading targets. To reprocess, replay from S3 — never re-hit DataSF.
- **Watermark-driven incremental loads.** Each dataset's high-water mark (`MAX(data_loaded_at)` from the last successful load) is stored in `METADATA.INGEST_WATERMARKS`. The next run reads this as its `since` filter. The watermark only advances after `load_s3_to_snowflake` succeeds, so a failed load automatically causes the next run to re-fetch the gap. Falls back to `config.epoch` on first run.
- **Newline-delimited JSON + `STRIP_OUTER_ARRAY = FALSE`.** One record per line; the COPY reads records independently.
- **Ingest and transform are separate DAGs.** The three ingest DAGs own extract → load → watermark. `transform_all` uses `ExternalTaskSensor` to wait for all three before running dbt once. dbt failures don't block ingestion, and dbt can be re-run independently without re-hitting the API.
- **Staging/intermediate are views; marts are tables.** Upstream always reflects the latest raw; marts materialize once per run so BI hits precomputed data.
- **Residential filter sits in the mart, not staging.** `stg_permits` is source-of-truth for all permits. The residential lens (rows with existing or proposed unit counts) is a reporting concern owned by `mart_housing_production`.
- **`normalize_neighborhood` macro.** Collapses DataSF's null/empty/"unknown" spellings into a single `'Unknown'` and `initcap`s the rest. Any mart that groups by neighborhood uses this macro.
- **Mart grain enforced by test.** `mart_housing_production`'s `(filed_month, neighborhood, supervisor_district, use_transition)` uniqueness is verified by `dbt_utils.unique_combination_of_columns`. Staging PK `permit_number` has `not_null` + `unique`.
- **dbt dev/prod isolation.** Two profile targets in [dbt/profiles.yml](dbt/profiles.yml). The Airflow DAG runs with `--target prod`. Local `make dbt-build` runs with `--target dev` and writes to a personal sandbox — the [generate_schema_name](dbt/macros/generate_schema_name.sql) macro adds the prefix. Set `DBT_DEV_SCHEMA` in `airflow/.env`.

## Why two dependency files

- [pyproject.toml](pyproject.toml) — Python deps for **local** work (`uv run` invokes the extractor, dbt, ruff, pytest).
- [airflow/requirements.txt](airflow/requirements.txt) — Python deps installed **inside the Airflow image** by `astro dev start`. Astro Runtime ships Airflow itself, so this file only adds providers and project-specific libs.

## CI

Two GitHub Actions workflows gate every PR:

| Workflow | Job | Triggers on | What it does |
|---|---|---|---|
| [`ci.yml`](.github/workflows/ci.yml) | `lint-and-test` | every PR + pushes to `main` | Ruff + yamllint + pytest (dev group, mocks the SODA API) |
| | `pre-commit` | same | All `pre-commit` hooks across the tree |
| | `secrets-scan` | same | `gitleaks` over the full git history |
| | `dag-integrity` | same | DAG-bag import + factory tests with the airflow group installed |
| [`dbt-ci.yml`](.github/workflows/dbt-ci.yml) | `dbt-compile` | PRs touching `dbt/**` | `dbt deps` + `dbt parse` + `dbt compile` against Snowflake — validates SQL renders with live source metadata; no models run, no data written |

`dbt-ci.yml` requires these GitHub repository secrets (Settings → Secrets and variables → Actions):
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_WAREHOUSE`.

Test runs against `dbt build` (with `--select` filters) are intentionally **not** part of `dbt-ci.yml` until the project-wide `+severity: warn` block in `dbt/dbt_project.yml` is removed (TODO.md item D1) — adding `dbt test` while severity is `warn` would always produce a falsely-green check.

## What's next (Phase 2+)

Phase 1 is a vertical slice: three data sources wired all the way through. Subsequent phases will add additional civic datasets (e.g. 311 service requests), a cross-domain mart joining them by neighborhood-month, a BI dashboard, and a RAG layer over Board of Supervisors meeting minutes.

## License

MIT — see [`LICENSE`](LICENSE).
