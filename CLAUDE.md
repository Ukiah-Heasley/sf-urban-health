# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run ingestion locally (no AWS/Snowflake needed — falls back to ./data/)
uv run python -m ingestion.permits --lookback-days 7

# Lint
uv run ruff check .

# Tests
uv run pytest

# dbt (requires Snowflake credentials in ~/.dbt/profiles.yml)
cd dbt && dbt deps && dbt build
cd dbt && dbt run --select stg_permits
cd dbt && dbt test

# Local Airflow stack
docker compose up airflow-init
docker compose up -d scheduler webserver
# UI: http://localhost:8080 (admin / admin)
```

## Architecture

```
DataSF SODA API → ingestion/permits.py → S3 raw/permits/YYYY/MM/DD/permits.json
                                       → ./data/ (local fallback when AWS_S3_BUCKET unset)
S3 → Snowflake RAW.PERMITS (COPY INTO via Airflow SnowflakeOperator)
Snowflake → dbt staging → intermediate → marts
Airflow DAG (dags/ingest_permits.py) orchestrates all five steps daily at 06:00 UTC
```

## Key design decisions

**S3 is the durable raw layer.** Raw JSON lives in S3 permanently. Snowflake RAW is a loading target. To reprocess, replay from S3 — never re-hit the DataSF API.

**7-day lookback, not 1-day.** DataSF backfills records late. Snowflake COPY INTO is idempotent on the stage, so overlapping loads are safe.

**Newline-delimited JSON.** `_write_local` / `_write_s3` emit one JSON object per line. The COPY uses `STRIP_OUTER_ARRAY = FALSE` — do not change this to array format.

**dbt materialization policy.** Staging and intermediate are views (always fresh, cheap). Marts are tables (fast for BI). Defined in `dbt/dbt_project.yml`.

**Residential filter sits in the mart, not staging.** `stg_permits` is source-of-truth for all permits. The residential lens (`existing_units IS NOT NULL OR proposed_units IS NOT NULL`) is a reporting concern owned by `mart_housing_production`.

**`normalize_neighborhood` macro.** Collapses DataSF's null/empty/"unknown" neighborhood spellings into `'Unknown'` and `initcap`s the rest. Any future mart that groups by neighborhood must use this macro.

## Local vs cloud execution

`ingestion/permits.py::run()` branches on `AWS_S3_BUCKET`:
- **Set** → writes to S3, does not write locally or touch DuckDB
- **Unset** → writes to `./data/raw/permits/YYYY/MM/DD/`, then rebuilds `./data/permits.db` via `_load_duckdb()`

DuckDB (`data/permits.db`) is only used for local development exploration — it is not part of the production pipeline.

## Airflow → ingestion import path

`docker-compose.yml` mounts `./ingestion` into `/opt/airflow/dags/ingestion`. The DAG does `from ingestion import permits`. If you rename or restructure the `ingestion/` package, update this mount and the DAG import.

## dbt grain and tests

The mart grain is `(filed_month, neighborhood, supervisor_district)` — enforced by a `dbt_utils.unique_combination_of_columns` test. Staging PK is `permit_number` with `not_null` + `unique`. Do not introduce aggregations in staging or intermediate that would break these constraints.
