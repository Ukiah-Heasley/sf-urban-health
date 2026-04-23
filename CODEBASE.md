# Codebase Walkthrough

A reader's guide to the SF Urban Health repo. Follow it top-to-bottom and you'll understand what every file does, why it exists, and how data flows from DataSF to Power BI.

---

## 1. What this repo actually is

The [README](README.md) describes a **vertical slice** of a larger civic-data platform. Only one of three planned domains is wired end-to-end:

| Domain | Status |
|---|---|
| Building permits | ✅ ingest → raw → staging → intermediate → mart |
| 311 service requests | ⏳ planned |
| Muni on-time performance | ⏳ planned |

> **Note on scope vs. the original brief.** An earlier brief scoped a broader platform with PDF-RAG over meeting transcripts, a Claude tool-use agent, and a Streamlit frontend. This repo deliberately drops those layers in favor of a cleaner data-engineering slice with Power BI as the BI consumer. The permits pipeline is the proof that the pattern works; 311 and Muni are expected to mirror its shape.

---

## 2. Data flow (one sentence per hop)

1. **DataSF SODA API** — paginated JSON from `data.sfgov.org/resource/i98e-djp9.json`.
2. **Python extractor** ([ingestion/permits.py](ingestion/permits.py)) — pulls a 7-day lookback window, writes newline-delimited JSON.
3. **S3 raw layer** — `s3://$AWS_S3_BUCKET/raw/permits/YYYY/MM/DD/permits.json` (falls back to `./data/` locally).
4. **Airflow DAG** ([dags/ingest_permits.py](dags/ingest_permits.py)) — schedules the extractor, then fires a Snowflake `COPY INTO`.
5. **Snowflake `RAW.PERMITS`** — one row per S3 record, `payload VARIANT` plus `_loaded_at`.
6. **dbt staging** ([stg_permits.sql](dbt/models/staging/stg_permits.sql)) — unpacks the VARIANT into typed columns.
7. **dbt intermediate** ([int_permit_timelines.sql](dbt/models/intermediate/int_permit_timelines.sql)) — derives lifecycle metrics.
8. **dbt mart** ([mart_housing_production.sql](dbt/models/marts/mart_housing_production.sql)) — monthly aggregate by neighborhood + supervisor district.
9. **Power BI** — DirectQuery against `SF_URBAN_HEALTH.MARTS`.

---

## 3. File-by-file tour

### Ingestion

**[ingestion/permits.py](ingestion/permits.py)** — the extractor.
- `_session()` builds a `requests` session with exponential backoff on 429/5xx and attaches the DataSF app token if present.
- `fetch_permits(since)` yields permit records filed on or after `since`, paginated at 1000 per page, ordered by `permit_number` for deterministic paging.
- `_write_local` / `_write_s3` emit **newline-delimited JSON** (one record per line) — important because the Snowflake COPY uses `STRIP_OUTER_ARRAY = FALSE` and expects this shape.
- `run(run_date, lookback_days=7)` is the entry point. The 7-day lookback exists because DataSF backfills records out-of-order; a strict daily window silently loses data.
- `__main__` block lets you run `python -m ingestion.permits` for local iteration without Airflow.

**[ingestion/\_\_init\_\_.py](ingestion/__init__.py)** — empty, exists so the DAG's `from ingestion import permits` works.

### Orchestration

**[dags/ingest_permits.py](dags/ingest_permits.py)** — the Airflow DAG.
- `_extract_with_date_parsing()` is a thin wrapper. Airflow passes `{{ ds }}` as a string via templating; the extractor wants a `datetime.date`. The wrapper bridges that.
- `PythonOperator("extract_to_s3")` calls the extractor.
- `SnowflakeOperator("copy_into_raw")` fires a templated `COPY INTO` that reads from the S3 stage partition `raw/permits/YYYY/MM/DD/` matching the run date.
- Schedule: daily at 07:00 UTC. `catchup=False` — we don't backfill automatically; use `dbt build --full-refresh` against historical S3 if needed.
- Retries: 3 with 5-minute delay (configured in `default_args`).

**[docker-compose.yml](docker-compose.yml)** — local Airflow (2.9.3, LocalExecutor + Postgres metadata DB). Notable: mounts `./ingestion` into `/opt/airflow/dags/ingestion` so the DAG's import resolves inside the container.

### dbt project

**[dbt/dbt_project.yml](dbt/dbt_project.yml)** — sets the materialization policy: staging and intermediate are **views** (cheap, always fresh), marts are **tables** (fast for BI, isolates dashboard performance from upstream refactors).

**[dbt/models/staging/\_sources.yml](dbt/models/staging/_sources.yml)** — declares `raw.permits` with a freshness SLA (warn at 36h, error at 72h). Freshness is how dbt proves the pipeline is alive.

**[dbt/models/staging/stg_permits.sql](dbt/models/staging/stg_permits.sql)** — strictly rename + cast. Reads the VARIANT `payload` column, pulls typed fields out with `::timestamp_ntz` / `::number` / `::integer`. Filters out rows with null `permit_number`. No business logic.

**[dbt/models/staging/\_stg_permits.yml](dbt/models/staging/_stg_permits.yml)** — column docs + `not_null` / `unique` tests on `permit_number`, `not_null` on `filed_at` and `current_status`.

**[dbt/models/intermediate/int_permit_timelines.sql](dbt/models/intermediate/int_permit_timelines.sql)** — where business logic lives:
- `net_units_added = proposed - existing` (negative = demolition).
- `project_cost = coalesce(revised_cost, estimated_cost)` — revised overrides when present.
- `days_to_issue`, `days_issue_to_complete`, `days_total` — calendar-day lifecycle metrics.
- `lifecycle_stage` — coalesces the latest reached stage (`completed` > `issued` > `filed`).

**[dbt/models/intermediate/\_int_permit_timelines.yml](dbt/models/intermediate/_int_permit_timelines.yml)** — tests + docs on the derived columns.

**[dbt/models/marts/mart_housing_production.sql](dbt/models/marts/mart_housing_production.sql)** — the BI-facing table. Groups by `(filed_month, normalize_neighborhood(neighborhood), supervisor_district)` and aggregates counts, unit totals, cost totals, and approval-time averages/medians. Materialized as a table.

**[dbt/models/marts/\_mart_housing_production.yml](dbt/models/marts/_mart_housing_production.yml)** — uses `dbt_utils.unique_combination_of_columns` to enforce the grain.

**[dbt/macros/normalize_neighborhood.sql](dbt/macros/normalize_neighborhood.sql)** — the one non-trivial macro. Collapses nulls, empty strings, and the various "unknown" spellings DataSF emits to a single `'Unknown'` value and `initcap`s the rest. Lives as a macro so permits, 311, and Muni marts all normalize identically — a prerequisite for the future cross-domain mart.

**[dbt/packages.yml](dbt/packages.yml)** — just `dbt_utils` (for the uniqueness test).

**[dbt/profiles.yml.example](dbt/profiles.yml.example)** — template; real `profiles.yml` lives outside the repo (`~/.dbt/profiles.yml`, gitignored).

### Config

**[.env.example](.env.example)** — Snowflake, AWS, DataSF. Copy to `.env` and fill in.

**[.gitignore](.gitignore)** — excludes `.env`, venvs, caches, `data/`, `logs/`, `dbt/target/`, `dbt/dbt_packages/`, and `dbt/profiles.yml` (which contains credentials).

**[requirements.txt](requirements.txt)** — Airflow 2.9.3, Snowflake + Amazon providers, dbt-core 1.8.5, dbt-snowflake, requests, boto3, pytest, ruff.

---

## 4. Design decisions worth knowing

These are the choices that shape the repo and are easy to miss from the file listing:

1. **S3 is the durable raw layer, not Snowflake.** Raw JSON lives in S3 forever. Snowflake `RAW` is a loading target; if we ever need to re-ingest or re-partition, we replay from S3 without re-hitting DataSF's rate limits.
2. **Newline-delimited JSON + `STRIP_OUTER_ARRAY = FALSE`.** Each line is one record, so the COPY reads records independently. Had we written a single JSON array per file we'd need `STRIP_OUTER_ARRAY = TRUE` — either works, but not mixed.
3. **7-day lookback, not 1-day.** Each run pulls the last seven days of filings because DataSF backfills records late. We rely on idempotent COPY behavior (Snowflake dedupes by file hash in the stage) to avoid double-loads.
4. **Neighborhood taxonomy lives in a macro.** Three future domains will all group by neighborhood and DataSF labels them differently across datasets. The only defensible cross-domain join is a normalized name — so normalization is a single macro everyone must call.
5. **Staging/intermediate are views; marts are tables.** Upstream models re-derive on every query (always fresh, no refresh cost). Marts materialize once per run so Power BI hits precomputed data.
6. **Every PK gets `not_null` + `unique`.** `permit_number` at staging and intermediate; the mart grain is enforced by a composite uniqueness test.

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
# configure snowflake_default connection in the Airflow UI, then unpause ingest_permits

# dbt against the loaded data
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
cd dbt && dbt deps && dbt build
```

---

## 6. Where to extend

When you add 311 or Muni:

- New extractor under `ingestion/` that mirrors `permits.py` (paginated, lookback window, S3/local fallback).
- New DAG under `dags/` with the same extract → COPY INTO shape.
- New source entry in [_sources.yml](dbt/models/staging/_sources.yml) with its own freshness SLA.
- Staging → intermediate → mart chain mirroring the permits tree; any neighborhood column flows through `normalize_neighborhood`.
- A cross-domain `mart_city_health` joining all three marts on `(neighborhood, month)` — this is where the macro pays off.
