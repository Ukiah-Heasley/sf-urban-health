# Codebase Walkthrough — Phase 1

A reader's guide to the repo. Follow it top-to-bottom and you'll understand what every file does, why it exists, and how data flows from DataSF to the `mart_housing_production` table.

---

## 1. What this repo is

Phase 1 of an SF civic-data platform: one vertical slice, end-to-end.

| Stage            | Tool                              | Artifact                                      |
|------------------|-----------------------------------|-----------------------------------------------|
| Extract          | Python + `requests`               | `ingestion/permits.py`                        |
| Land             | AWS S3                            | `s3://$AWS_S3_BUCKET/raw/permits/YYYY/MM/DD/` |
| Load             | Snowflake `COPY INTO`             | `SF_URBAN_HEALTH.RAW.PERMITS`                 |
| Transform        | dbt-core + dbt-snowflake          | `staging/` → `intermediate/` → `marts/`       |
| Orchestrate      | Apache Airflow 2.9                | `dags/ingest_permits.py`                      |
| Test             | dbt generic + `dbt_utils`         | `_*.yml` files alongside models               |

---

## 2. Data flow (one sentence per hop)

1. **DataSF SODA API** — paginated JSON from `data.sfgov.org/resource/i98e-djp9.json`.
2. **Python extractor** ([ingestion/permits.py](ingestion/permits.py)) — 7-day lookback window, writes newline-delimited JSON.
3. **S3 raw layer** — `s3://$AWS_S3_BUCKET/raw/permits/YYYY/MM/DD/permits.json` (falls back to `./data/` locally).
4. **Airflow DAG** ([dags/ingest_permits.py](dags/ingest_permits.py)) — schedules the extractor, fires Snowflake `COPY INTO`, then runs dbt.
5. **Snowflake `RAW.PERMITS`** — one row per record, `payload VARIANT` + `_loaded_at`.
6. **dbt staging** ([stg_permits.sql](dbt/models/staging/stg_permits.sql)) — unpacks the VARIANT into typed columns.
7. **dbt intermediate** ([int_permit_timelines.sql](dbt/models/intermediate/int_permit_timelines.sql)) — derives lifecycle metrics.
8. **dbt mart** ([mart_housing_production.sql](dbt/models/marts/mart_housing_production.sql)) — filters to residential, aggregates monthly by neighborhood + supervisor district.

---

## 3. File-by-file tour

### Ingestion

**[ingestion/permits.py](ingestion/permits.py)** — the extractor.
- `_session()` builds a `requests` session with exponential backoff on 429/5xx and attaches the DataSF app token if present.
- `fetch_permits(since)` yields permit records filed on or after `since`, paginated at 1000 per page, ordered by `permit_number` for deterministic paging.
- `_write_local` / `_write_s3` emit **newline-delimited JSON** (one record per line) — the Snowflake COPY uses `STRIP_OUTER_ARRAY = FALSE` and expects this shape.
- `run(run_date, lookback_days=7)` is the entry point. The 7-day lookback exists because DataSF backfills records out-of-order; a strict daily window silently loses data.
- `__main__` block lets you run `python -m ingestion.permits` for local iteration without Airflow.

**[ingestion/\_\_init\_\_.py](ingestion/__init__.py)** — empty, exists so the DAG's `from ingestion import permits` works.

### Orchestration

**[dags/ingest_permits.py](dags/ingest_permits.py)** — the Airflow DAG, five tasks in sequence:

```
extract_permits_to_s3  →  load_s3_to_snowflake  →  run_dbt_staging  →  run_dbt_marts  →  run_dbt_tests
```

- `extract_permits_to_s3` (`PythonOperator`) — calls `permits.run(run_date=ds)`. A thin wrapper parses the `{{ ds }}` template string into a `date`.
- `load_s3_to_snowflake` (`SnowflakeOperator`) — templated `COPY INTO` reading from the S3 stage partition matching the run date.
- `run_dbt_staging` / `run_dbt_marts` / `run_dbt_tests` (`BashOperator`) — shell out to dbt inside the Airflow container. `DBT_PROJECT_DIR` and `DBT_PROFILES_DIR` are set in `docker-compose.yml`.
- Schedule: daily at 06:00 UTC. `catchup=False`.
- Retries: 3, with 5-minute delay.
- Tags: `sf-civic`, `permits`, `daily`.

> For a production-grade setup you'd likely use [Cosmos](https://www.astronomer.io/cosmos/) to represent each dbt model as its own Airflow task. BashOperators are used here to keep the DAG simple and the task contract faithful to the Phase 1 brief.

**[docker-compose.yml](docker-compose.yml)** — local Airflow (2.9.3, LocalExecutor + Postgres metadata DB). Mounts `./ingestion` into `/opt/airflow/dags/ingestion` so the DAG import resolves, and `./dbt` into `/opt/airflow/dbt` so the BashOperator tasks find the dbt project. `_PIP_ADDITIONAL_REQUIREMENTS` layers in the Snowflake + Amazon providers and `dbt-core`/`dbt-snowflake`.

### dbt project

**[dbt/dbt_project.yml](dbt/dbt_project.yml)** — sets the materialization policy: staging and intermediate are **views** (cheap, always fresh), marts are **tables** (fast for BI, isolates downstream performance from upstream refactors).

**[dbt/models/staging/\_sources.yml](dbt/models/staging/_sources.yml)** — declares `raw.permits` with a freshness SLA (warn at 36h, error at 72h). Freshness is how dbt proves the pipeline is alive.

**[dbt/models/staging/stg_permits.sql](dbt/models/staging/stg_permits.sql)** — strictly rename + cast. Reads the VARIANT `payload` column, pulls typed fields out with `::timestamp_ntz` / `::number` / `::integer`, and lowercases `current_status` so downstream comparisons are deterministic. Filters out rows with null `permit_number`. No business logic.

**[dbt/models/staging/\_stg_permits.yml](dbt/models/staging/_stg_permits.yml)** — column docs + tests:
- `not_null` + `unique` on `permit_number`
- `not_null` on `filed_at`
- `not_null` + `accepted_values` on `current_status` (lowercase lifecycle states)

**[dbt/models/intermediate/int_permit_timelines.sql](dbt/models/intermediate/int_permit_timelines.sql)** — where business logic lives:
- `net_units_added = proposed - existing` (negative = demolition).
- `project_cost = coalesce(revised_cost, estimated_cost)` — revised overrides when present.
- `days_to_issue`, `days_issue_to_complete`, `days_total` — calendar-day lifecycle metrics.
- `lifecycle_stage` — latest reached stage (`completed` > `issued` > `filed`).

**[dbt/models/intermediate/\_int_permit_timelines.yml](dbt/models/intermediate/_int_permit_timelines.yml)** — tests + docs on the derived columns.

**[dbt/models/marts/mart_housing_production.sql](dbt/models/marts/mart_housing_production.sql)** — the BI-facing table.
- Residential filter: keep only permits reporting an existing or proposed residential unit count.
- Groups by `(filed_month, normalize_neighborhood(neighborhood), supervisor_district)`.
- Aggregates: permits filed / issued / completed / expired, proposed_units, net_units_added, total project cost, and avg/median days to issue.
- Materialized as a table.

**[dbt/models/marts/\_mart_housing_production.yml](dbt/models/marts/_mart_housing_production.yml)** — uses `dbt_utils.unique_combination_of_columns` to enforce the grain.

**[dbt/macros/normalize_neighborhood.sql](dbt/macros/normalize_neighborhood.sql)** — collapses nulls, empty strings, and the various "unknown" spellings DataSF emits to a single `'Unknown'` value and `initcap`s the rest. Lives as a macro so future marts stay consistent.

**[dbt/packages.yml](dbt/packages.yml)** — just `dbt_utils` (for the uniqueness test).

**[dbt/profiles.yml.example](dbt/profiles.yml.example)** — template; the real `profiles.yml` lives outside the repo (`~/.dbt/profiles.yml`, gitignored).

### Config

**[.env.example](.env.example)** — Snowflake, AWS, DataSF credentials template. Copy to `.env` and fill in.

**[.gitignore](.gitignore)** — excludes `.env`, venvs, caches, `data/`, `logs/`, `dbt/target/`, `dbt/dbt_packages/`, and `dbt/profiles.yml` (credentials).

**[requirements.txt](requirements.txt)** — Airflow 2.9.3, Snowflake + Amazon providers, dbt-core 1.8.5, dbt-snowflake, requests, boto3, pytest, ruff.

**[pyproject.toml](pyproject.toml)** — uv-managed dependency groups (`airflow`, `dbt`, `dev`) for local dev without a full Airflow container.

---

## 4. Design decisions worth knowing

1. **S3 is the durable raw layer, not Snowflake.** Raw JSON lives in S3 forever. Snowflake `RAW` is a loading target; to re-ingest or re-partition, replay from S3 without re-hitting DataSF rate limits.
2. **Newline-delimited JSON + `STRIP_OUTER_ARRAY = FALSE`.** Each line is one record; the COPY reads records independently.
3. **7-day lookback, not 1-day.** DataSF backfills records late. We rely on idempotent COPY behavior in the stage to avoid double-loads.
4. **Neighborhood normalization lives in a macro.** Keeps the taxonomy consistent and makes any future cross-domain mart straightforward.
5. **Staging/intermediate are views; marts are tables.** Upstream always reflects the latest raw; marts materialize once per run.
6. **Residential filter sits in the mart, not staging.** Staging is source-of-truth for all permits; the residential lens is a reporting concern.
7. **Every PK gets `not_null` + `unique`.** `permit_number` at staging and intermediate; the mart grain is enforced by a composite uniqueness test.

---

## 5. How to run it

Local extractor only (no AWS, no Snowflake):

```bash
python -m ingestion.permits
# writes ./data/raw/permits/YYYY/MM/DD/permits.json
```

Full local stack:

```bash
cp .env.example .env        # fill in credentials
docker compose up airflow-init
docker compose up -d scheduler webserver
# configure snowflake_default connection in the Airflow UI, unpause ingest_permits

# dbt standalone (optional — the DAG runs dbt too)
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
cd dbt && dbt deps && dbt build
```

---

## 6. Where this goes next

Phase 1 deliberately stops at one data source and one mart. Future phases will add additional civic datasets, a cross-domain mart joining them by neighborhood-month, a BI dashboard, and a RAG layer over Board of Supervisors meeting minutes. That scaffolding lives on the `phase-2-wip` local branch and is intentionally excluded from `main`.