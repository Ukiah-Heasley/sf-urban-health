# SF Permits Pipeline — Phase 1

A production-style daily ETL pipeline that ingests SF building permit data from the DataSF SODA API, lands raw JSON in S3, loads it into Snowflake, and transforms it through a dbt staging → intermediate → mart layer on an Airflow schedule. Phase 1 of a larger SF Civic Intelligence Platform.

## Architecture

```
DataSF SODA API
      │
      ▼
Python extractor  (airflow/scripts/permits.py)
      │
      ▼
S3  raw/permits/YYYY/MM/DD/permits.json
      │
      ▼
Snowflake RAW.PERMITS  (COPY INTO, orchestrated by Airflow)
      │
      ▼
dbt  staging → intermediate → marts
      │
      ▼
mart_housing_production  (monthly by neighborhood + supervisor district)
```

| Stage         | Tool                              |
|---------------|-----------------------------------|
| Orchestration | Apache Airflow (Astro Runtime)    |
| Ingestion     | Python 3.11 + `requests`          |
| Raw lake      | AWS S3                            |
| Warehouse     | Snowflake                         |
| Transform     | dbt-core + dbt-snowflake          |
| Tests         | dbt generic tests + `dbt_utils`   |

## Repository layout

```
.
├── dbt/                         dbt project — canonical location, edit here
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml             env-var-driven; safe to commit
│   ├── macros/
│   └── models/{staging,intermediate,marts}/
├── airflow/                     Astro CLI project root
│   ├── Dockerfile               Astro Runtime image; bakes scripts/ + include/ at build time
│   ├── dags/ingest_permits.py   DAG: extract → COPY INTO → dbt → tests
│   ├── scripts/permits.py       DataSF API client + S3 writer
│   ├── include/dbt/             generated mirror of dbt/ (gitignored, refreshed by `make sync-dbt`)
│   ├── requirements.txt         Python deps installed inside the Airflow image
│   └── .env.example             Snowflake / AWS / DataSF credentials template
├── Makefile                     One-line entrypoints for every workflow
└── pyproject.toml               uv-managed deps for local Python work
```

> **Why the mirror?** Astro CLI's Docker build context is `airflow/`, so the image can only `COPY` from inside that directory. To keep dbt as a true top-level peer (the industry-standard layout), `make sync-dbt` rsyncs `dbt/` into `airflow/include/dbt/` before the image is built. `make airflow-up` runs the sync automatically; you never edit the mirror by hand.

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

# 2. Snowflake bootstrap (run once, in the Snowflake UI)
#    See `Snowflake bootstrap` section below.

# 3. dbt profile is rendered from airflow/.env via env_var() in
#    dbt/profiles.yml — no separate profile file needed.
```

### Snowflake bootstrap

```sql
CREATE DATABASE SF_URBAN_HEALTH;
CREATE SCHEMA SF_URBAN_HEALTH.RAW;
CREATE SCHEMA SF_URBAN_HEALTH.STAGING;
CREATE SCHEMA SF_URBAN_HEALTH.INTERMEDIATE;
CREATE SCHEMA SF_URBAN_HEALTH.MARTS;

CREATE STAGE SF_URBAN_HEALTH.RAW.S3_STAGE
  URL = 's3://sf-urban-health/'
  CREDENTIALS = (AWS_KEY_ID = '...' AWS_SECRET_KEY = '...')
  FILE_FORMAT = (TYPE = JSON);

CREATE TABLE SF_URBAN_HEALTH.RAW.PERMITS (
    payload    VARIANT,
    _loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE SF_URBAN_HEALTH.RAW.INCIDENTS (
    payload    VARIANT,
    _loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
```

## Running it

Every workflow has a `make` target — `airflow/.env` is loaded automatically.

```bash
make ingest         # run the extractor: DataSF → S3
make dbt-deps       # install dbt packages (one-time)
make dbt-build      # run + test all dbt models
make airflow-up     # start the local Airflow stack
make airflow-down
make airflow-logs   # tail scheduler logs
make lint
make test
```

The Airflow UI runs at [http://localhost:8080](http://localhost:8080) (`admin` / `admin`). Unpause `ingest_permits` to schedule daily runs at 06:00 UTC, or trigger a one-off run from the UI.

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

- **S3 is the durable raw layer.** Raw JSON lives in S3 permanently; Snowflake `RAW.PERMITS` is a loading target. To reprocess, replay from S3 — never re-hit DataSF.
- **Checkpoint-driven incremental loads.** `permits.run()` reads `s3://$AWS_S3_BUCKET/checkpoints/permits.json` for the last successful `since` date and resumes from `resume_offset` if the prior run was incomplete. Snowflake `COPY INTO` is idempotent on the stage, so overlapping loads are safe.
- **Newline-delimited JSON + `STRIP_OUTER_ARRAY = FALSE`.** One record per line; the COPY reads records independently.
- **Staging/intermediate are views; marts are tables.** Upstream always reflects the latest raw; marts materialize once per run so BI hits precomputed data.
- **Residential filter sits in the mart, not staging.** `stg_permits` is source-of-truth for all permits. The residential lens (rows with existing or proposed unit counts) is a reporting concern owned by `mart_housing_production`.
- **`normalize_neighborhood` macro.** Collapses DataSF's null/empty/"unknown" spellings into a single `'Unknown'` and `initcap`s the rest. Any mart that groups by neighborhood uses this macro.
- **Mart grain enforced by test.** `(filed_month, neighborhood, supervisor_district)` uniqueness is verified by `dbt_utils.unique_combination_of_columns`. Staging PK `permit_number` has `not_null` + `unique`.
- **dbt dev/prod isolation.** Two profile targets in [dbt/profiles.yml](dbt/profiles.yml). The Airflow DAG runs with `--target prod` and writes to bare schemas (`STAGING`, `INTERMEDIATE`, `MARTS`). Local `make dbt-build` runs with `--target dev` and writes to a personal sandbox like `DBT_UKIAH_STAGING` — the [generate_schema_name](dbt/macros/generate_schema_name.sql) macro adds the prefix. Set `DBT_DEV_SCHEMA` in `airflow/.env`.

## Why two dependency files

- [pyproject.toml](pyproject.toml) — Python deps for **local** work (`uv run` invokes the extractor, dbt, ruff, pytest).
- [airflow/requirements.txt](airflow/requirements.txt) — Python deps installed **inside the Airflow image** by `astro dev start`. Astro Runtime ships Airflow itself, so this file only adds providers and project-specific libs.

## CI

Two GitHub Actions workflows gate every PR:

| Workflow | Triggers on | What it does |
|---|---|---|
| [`ci.yml`](.github/workflows/ci.yml) | every PR + pushes to `main` | `ruff check` + `pytest` (mocks the SODA API) |
| [`dbt-ci.yml`](.github/workflows/dbt-ci.yml) | PRs touching `dbt/**` | `dbt parse` + `dbt compile` against Snowflake — validates SQL renders with live source metadata; no models run, no data written |

`dbt-ci.yml` requires these GitHub repository secrets (Settings → Secrets and variables → Actions):
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_WAREHOUSE`.

## What's next (Phase 2+)

Phase 1 is a vertical slice: one data source, wired all the way through. Subsequent phases will add additional civic datasets (311 service requests, Muni transit performance), a cross-domain mart joining them by neighborhood-month, a BI dashboard, and a RAG layer over Board of Supervisors meeting minutes.
