# SF Urban Health Monitor

Production-style data pipeline tracking three civic indicators for San Francisco: housing permit production, Muni transit on-time performance, and 311 service requests. Built as a data engineering portfolio project.

## Status

Vertical slice: **building permits** is wired end-to-end (ingest → raw → staging → intermediate → mart → BI). 311 and Muni will follow the same shape once the permits slice is validated against real data.

## Architecture

```
DataSF SODA API ──► Python ingestion ──► S3 raw/permits/YYYY/MM/DD/
                                          │
                                          ▼
                                  Snowflake RAW.PERMITS  (COPY INTO via Airflow)
                                          │
                                          ▼
                           dbt staging ► intermediate ► marts
                                          │
                                          ▼
                                  Power BI (DirectQuery)
```

Layering mirrors dbt's recommended structure:

- `staging/` — one model per source table, rename and cast only.
- `intermediate/` — joins, derived metrics, business logic (e.g. permit lifecycle timings).
- `marts/` — aggregated, BI-ready tables. Materialized as tables; everything upstream is a view.

## Stack

| Layer        | Tool                              |
|--------------|-----------------------------------|
| Orchestration| Apache Airflow 2.9 (LocalExecutor)|
| Ingestion    | Python 3.11 + `requests`          |
| Lake         | AWS S3                            |
| Warehouse    | Snowflake                         |
| Transform    | dbt-core + dbt-snowflake          |
| Tests        | dbt generic tests + `dbt_utils`   |
| BI           | Power BI Desktop                  |

## Repository layout

```
.
├── dags/                  Airflow DAGs (one per data source)
├── ingestion/             Python extractors that write to S3
├── dbt/
│   ├── models/
│   │   ├── staging/       source-shaped, typed & renamed
│   │   ├── intermediate/  business logic
│   │   └── marts/         BI-facing tables
│   ├── macros/            reusable SQL (e.g. normalize_neighborhood)
│   ├── tests/             singular tests
│   ├── dbt_project.yml
│   ├── packages.yml
│   └── profiles.yml.example
├── docker-compose.yml     local Airflow stack
├── requirements.txt
└── .env.example
```

## Setup

### 1. Prerequisites

- Docker Desktop
- Snowflake trial account (`https://signup.snowflake.com`)
- AWS account with an S3 bucket named in `AWS_S3_BUCKET`
- Free DataSF app token: `https://data.sfgov.org/profile/app_tokens`

### 2. Environment

```bash
cp .env.example .env
# fill in credentials
```

### 3. Snowflake one-time bootstrap

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
    payload VARIANT,
    _loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Transcripts RAG (see rag/)
CREATE TABLE SF_URBAN_HEALTH.RAW.TRANSCRIPT_CHUNKS (
    payload VARIANT,
    _loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
CREATE TABLE SF_URBAN_HEALTH.RAW.TRANSCRIPT_INGESTS (
    source_url STRING,
    meeting_date DATE,
    meeting_type STRING,
    ingested_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (source_url)
);
```

### 4. Local Airflow

```bash
docker compose up airflow-init
docker compose up -d scheduler webserver
# UI at http://localhost:8080 (admin / admin)
```

Add a `snowflake_default` connection in the Airflow UI pointing at your trial account.

### 5. dbt

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
cd dbt && dbt deps && dbt build
```

### 6. Power BI

Connect to the `SF_URBAN_HEALTH.MARTS` schema via the Snowflake connector. One page per mart (`mart_housing_production` is the first).

## Running ingestion locally (no AWS)

The extractor falls back to `./data/` when `AWS_S3_BUCKET` is unset, so you can iterate on shape without S3 credentials:

```bash
python -m ingestion.permits
```

## Design notes

- **S3 as the durable raw layer.** Snowflake is the compute/transform surface, but raw JSON lives in S3 so re-loading or re-partitioning never requires re-hitting a rate-limited municipal API.
- **Incremental filing-date windows.** The permits extractor pulls a 7-day lookback on every run rather than a strict daily slice; DataSF backfills records out-of-order and a tight window loses data.
- **Neighborhood taxonomy is a macro, not inline SQL.** Every mart will group by neighborhood, and DataSF's three datasets label them slightly differently. Centralizing normalization in `normalize_neighborhood` is the only way the cross-domain `mart_city_health` will be defensible.
- **Marts are tables, everything upstream is a view.** Staging/intermediate are cheap to re-derive and should always reflect the latest raw. Marts are what Power BI hits — materializing them caps query latency and isolates dashboard performance from refactors upstream.

## Roadmap

- [x] Permits: ingestion + DAG + staging → intermediate → mart
- [ ] 311 service requests: same shape, `mart_quality_of_life`
- [ ] Muni on-time performance: `mart_transit_performance`
- [ ] Cross-domain `mart_city_health` joining all three by neighborhood-month
- [ ] BI dashboard (tool TBD)
- [ ] Freshness SLAs on all three sources
- [ ] **RAG over BOS meeting minutes** (scaffolded in `ingestion/transcripts.py`, `ingestion/chunker.py`, `dbt/models/**/transcript*`, `rag/retriever.py`) — Snowflake Cortex embeddings, incremental dbt model, `VECTOR_COSINE_SIMILARITY` retrieval.
