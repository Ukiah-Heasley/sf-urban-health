# SF Urban Health Pipeline — Phase 1

A production-style daily ETL pipeline that ingests SF civic datasets (building permits, eviction notices, police incidents) from the DataSF SODA API, lands raw JSON in S3, loads into Snowflake, and transforms through a dbt staging → intermediate → mart layer on an Airflow schedule. Phase 1 of a larger SF Civic Intelligence Platform.

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

The pipeline uses four Airflow DAGs with a clear separation of concerns:

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

`transform_all` uses `ExternalTaskSensor` to wait for each dataset's `update_watermark` task before running dbt once across all models. Sensors use `mode="reschedule"` (no worker slot held while waiting), `poke_interval=120s`, `timeout=3h`.

## Repository layout

```
.
├── dbt/                              dbt project — canonical location, edit here
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml                  env-var-driven; safe to commit
│   ├── macros/
│   └── models/{staging,intermediate,marts}/
├── airflow/                          Astro CLI project root
│   ├── Dockerfile                    Astro Runtime image
│   ├── dags/
│   │   ├── dag_factory.py            Factory for ingest DAGs
│   │   ├── ingest_permits.py         Permits ingest DAG
│   │   ├── ingest_evictions.py       Evictions ingest DAG
│   │   ├── ingest_incidents.py       Incidents ingest DAG
│   │   └── transform_all.py          Shared dbt transform DAG
│   ├── include/scripts/
│   │   ├── soda_ingest.py            Generic SODA API engine
│   │   ├── permits.py                Permits config + CLI
│   │   ├── evictions.py              Evictions config + CLI
│   │   └── incident_reports.py       Incidents config + CLI
│   ├── include/dbt/                  Mirror of dbt/ (gitignored, refreshed by `make sync-dbt`)
│   ├── requirements.txt              Python deps inside the Airflow image
│   └── .env.example                  Snowflake / AWS / DataSF credentials template
├── Makefile                          One-line entrypoints for every workflow
└── pyproject.toml                    uv-managed deps for local Python work
```

> **Why the mirror?** Astro CLI's Docker build context is `airflow/`, so the image can only `COPY` from inside that directory. To keep dbt as a true top-level peer, `make sync-dbt` rsyncs `dbt/` into `airflow/include/dbt/` before the image is built. `make airflow-up` runs the sync automatically; never edit the mirror by hand.

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
CREATE SCHEMA SF_URBAN_HEALTH.METADATA;

CREATE STAGE SF_URBAN_HEALTH.RAW.S3_STAGE
  URL = 's3://sf-urban-health/'
  CREDENTIALS = (AWS_KEY_ID = '...' AWS_SECRET_KEY = '...')
  FILE_FORMAT = (TYPE = JSON);

CREATE TABLE SF_URBAN_HEALTH.RAW.PERMITS (
    payload    VARIANT,
    _loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE SF_URBAN_HEALTH.RAW.EVICTIONS (
    payload    VARIANT,
    _loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE SF_URBAN_HEALTH.RAW.INCIDENTS (
    payload    VARIANT,
    _loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE SF_URBAN_HEALTH.METADATA.INGEST_WATERMARKS (
    dataset_name  VARCHAR       NOT NULL,
    watermark     TIMESTAMP_NTZ NOT NULL,
    updated_at    TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT pk_ingest_watermarks PRIMARY KEY (dataset_name)
);
```

## Running it

Every workflow has a `make` target — `airflow/.env` is loaded automatically.

```bash
make dbt-deps       # install dbt packages (one-time)
make dbt-build      # run + test all dbt models
make airflow-up     # start the local Airflow stack
make airflow-down
make airflow-logs   # tail scheduler logs
make lint
make test
```

The Airflow UI runs at [http://localhost:8080](http://localhost:8080) (`admin` / `admin`). Unpause all four DAGs (`ingest_permits`, `ingest_evictions`, `ingest_incidents`, `transform_all`) to enable the full daily pipeline at 06:00 UTC.

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
- **Mart grain enforced by test.** `(filed_month, neighborhood, supervisor_district)` uniqueness is verified by `dbt_utils.unique_combination_of_columns`. Staging PK `permit_number` has `not_null` + `unique`.
- **dbt dev/prod isolation.** Two profile targets in [dbt/profiles.yml](dbt/profiles.yml). The Airflow DAG runs with `--target prod`. Local `make dbt-build` runs with `--target dev` and writes to a personal sandbox — the [generate_schema_name](dbt/macros/generate_schema_name.sql) macro adds the prefix. Set `DBT_DEV_SCHEMA` in `airflow/.env`.

## Why two dependency files

- [pyproject.toml](pyproject.toml) — Python deps for **local** work (`uv run` invokes the extractor, dbt, ruff, pytest).
- [airflow/requirements.txt](airflow/requirements.txt) — Python deps installed **inside the Airflow image** by `astro dev start`. Astro Runtime ships Airflow itself, so this file only adds providers and project-specific libs.

## CI

Two GitHub Actions workflows gate every PR:

| Workflow | Triggers on | What it does |
|----------|-------------|--------------|
| [`ci.yml`](.github/workflows/ci.yml) | every PR + pushes to `main` | `ruff check` + `pytest` (mocks the SODA API) |
| [`dbt-ci.yml`](.github/workflows/dbt-ci.yml) | PRs touching `dbt/**` | `dbt parse` + `dbt compile` against Snowflake — validates SQL renders with live source metadata; no models run, no data written |

`dbt-ci.yml` requires these GitHub repository secrets (Settings → Secrets and variables → Actions):
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_WAREHOUSE`.

## What's next (Phase 2+)

Phase 1 is a vertical slice: three data sources wired all the way through. Subsequent phases will add additional civic datasets (311 service requests, Muni transit performance), a cross-domain mart joining them by neighborhood-month, a BI dashboard, and a RAG layer over Board of Supervisors meeting minutes.
