# SF Permits Pipeline — Phase 1

A production-style daily ETL pipeline that ingests SF building permit data from the DataSF SODA API, lands raw JSON in S3, loads it into Snowflake, and transforms it through a dbt staging → intermediate → mart layer on an Airflow schedule. Phase 1 of a larger SF Civic Intelligence Platform.

## Architecture

```
DataSF SODA API
      │
      ▼
Python extractor  (ingestion/permits.py)
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

Layering follows dbt's recommended structure:

- `staging/` — one model per source, rename + cast only.
- `intermediate/` — joins and derived business logic (lifecycle timings, net units).
- `marts/` — aggregated, BI-ready tables. Materialized as tables; upstream is views.

## Stack

| Layer         | Tool                              |
|---------------|-----------------------------------|
| Orchestration | Apache Airflow 2.9 (LocalExecutor)|
| Ingestion     | Python 3.11 + `requests`          |
| Raw lake      | AWS S3                            |
| Warehouse     | Snowflake                         |
| Transform     | dbt-core + dbt-snowflake          |
| Tests         | dbt generic tests + `dbt_utils`   |

## Repository layout

```
.
├── dags/ingest_permits.py      Airflow DAG: extract → COPY INTO → dbt run → dbt test
├── ingestion/permits.py        DataSF API client + S3 writer
├── dbt/
│   ├── models/
│   │   ├── staging/            stg_permits.sql, _sources.yml
│   │   ├── intermediate/       int_permit_timelines.sql
│   │   └── marts/              mart_housing_production.sql
│   ├── macros/                 normalize_neighborhood.sql
│   ├── dbt_project.yml
│   └── packages.yml
├── docker-compose.yml          Local Airflow stack
├── requirements.txt
├── pyproject.toml              uv-based dep groups
└── .env.example
```

## Prerequisites

- Docker Desktop
- Snowflake trial account ([signup.snowflake.com](https://signup.snowflake.com))
- AWS account with an S3 bucket
- Free DataSF app token ([data.sfgov.org/profile/app_tokens](https://data.sfgov.org/profile/app_tokens))

## Setup

### 1. Environment

```bash
cp .env.example .env
# fill in Snowflake, AWS, and DataSF credentials
```

### 2. Snowflake one-time bootstrap

```sql
CREATE DATABASE SF_URBAN_HEALTH;
CREATE SCHEMA SF_URBAN_HEALTH.RAW;
CREATE SCHEMA SF_URBAN_HEALTH.STAGING;
CREATE SCHEMA SF_URBAN_HEALTH.INTERMEDIATE;
CREATE SCHEMA SF_URBAN_HEALTH.MARTS;

-- External stage pointing at the S3 raw bucket
CREATE STAGE SF_URBAN_HEALTH.RAW.S3_STAGE
  URL = 's3://sf-urban-health/'
  CREDENTIALS = (AWS_KEY_ID = '...' AWS_SECRET_KEY = '...')
  FILE_FORMAT = (TYPE = JSON);

-- Raw table receives COPY INTO from S3
CREATE TABLE SF_URBAN_HEALTH.RAW.PERMITS (
    payload VARIANT,
    _loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 3. Local Airflow

```bash
docker compose up airflow-init
docker compose up -d scheduler webserver
# UI at http://localhost:8080 (admin / admin)
```

Add a `snowflake_default` connection in the Airflow UI pointing at your trial account.

### 4. dbt profile

```bash
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
# edit with your Snowflake creds
cd dbt && dbt deps
```

## Running the pipeline

### Full pipeline via Airflow

Unpause `ingest_permits` in the Airflow UI. The DAG runs daily at 06:00 UTC and executes:

```
extract_permits_to_s3 → load_s3_to_snowflake → run_dbt_staging → run_dbt_marts → run_dbt_tests
```

Trigger a one-off run from the UI or CLI (`airflow dags trigger ingest_permits`).

### Ingestion only (no AWS, no Snowflake)

The extractor falls back to `./data/` when `AWS_S3_BUCKET` is unset, so you can iterate on shape without cloud credentials:

```bash
python -m ingestion.permits --lookback-days 7
# writes ./data/raw/permits/YYYY/MM/DD/permits.json
```

### dbt only (against already-loaded RAW data)

```bash
cd dbt
dbt build      # run + test everything
```

## Sample mart queries

Which neighborhoods filed the most residential permits last year?

```sql
SELECT neighborhood,
       SUM(permits_filed)      AS permits,
       SUM(proposed_units)     AS proposed_units,
       SUM(net_units_added)    AS net_units_added
FROM SF_URBAN_HEALTH.MARTS.MART_HOUSING_PRODUCTION
WHERE filed_month >= DATEADD(year, -1, CURRENT_DATE())
GROUP BY neighborhood
ORDER BY permits DESC
LIMIT 10;
```

How long does it take to get a permit issued by district?

```sql
SELECT supervisor_district,
       ROUND(AVG(avg_days_to_issue), 1)  AS avg_days_to_issue,
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

## Design notes

- **S3 is the durable raw layer.** Snowflake is compute/transform; raw JSON lives in S3 so reprocessing never re-hits DataSF rate limits.
- **Newline-delimited JSON + `STRIP_OUTER_ARRAY = FALSE`.** One record per line — the COPY reads records independently.
- **7-day lookback, not 1-day.** DataSF backfills records late; a strict daily window silently loses data. The COPY is idempotent in the stage.
- **Neighborhood normalization is a macro.** `normalize_neighborhood` collapses the various null/empty/"unknown" spellings DataSF emits into a single `'Unknown'` and title-cases the rest.
- **Staging/intermediate are views; marts are tables.** Upstream models always reflect the latest raw. Marts materialize once per run so downstream BI hits precomputed data.
- **Residential filter lives in the mart.** `mart_housing_production` keeps only permits that report an existing or proposed residential unit count — that's the most reliable signal for housing production in DataSF's free-text `permit_type` field.

## What's next (Phase 2+)

Phase 1 is a vertical slice: one data source, wired all the way through. Subsequent phases will add additional civic datasets (311 service requests, Muni transit performance), a cross-domain mart joining them by neighborhood-month, a BI dashboard, and a RAG layer over Board of Supervisors meeting minutes. Those components live on a separate branch and are not part of this release.