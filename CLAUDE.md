# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All workflows go through the root `Makefile`, which auto-loads `airflow/.env`.

```bash
make ingest         # DataSF -> S3 (uv run airflow/scripts/permits.py)
make dbt-deps       # one-time: install dbt packages
make dbt-build      # dbt run + test against Snowflake
make dbt-test
make airflow-up     # astro dev start (UI at http://localhost:8080, admin/admin)
make airflow-down
make airflow-logs   # tail scheduler
make lint           # uv run ruff check .
make test           # uv run pytest
```

For ad-hoc dbt selectors not covered by a target, run from `airflow/include/dbt/` after sourcing `airflow/.env`:
`uv run --group dbt dbt run --select stg_permits --profiles-dir .`

## Architecture

```
DataSF SODA API → airflow/scripts/permits.py → S3 raw/permits/YYYY/MM/DD/permits.json
S3 → Snowflake RAW.PERMITS (COPY INTO via Airflow SnowflakeOperator)
Snowflake → dbt staging → intermediate → marts
Airflow DAG (airflow/dags/ingest_permits.py) orchestrates all five steps daily at 06:00 UTC
```

## Key design decisions

**S3 is the durable raw layer.** Raw JSON lives in S3 permanently. Snowflake RAW is a loading target. To reprocess, replay from S3 — never re-hit the DataSF API.

**Checkpoint-driven incremental loads.** `run()` reads `s3://$AWS_S3_BUCKET/checkpoints/permits.json` for the last successful `since` date (epoch fallback: 2013-01-01) and resumes from `resume_offset` if a prior run was incomplete. Snowflake COPY INTO is idempotent on the stage, so overlapping loads are safe.

**Newline-delimited JSON.** `_write_s3` emits one JSON object per line. The COPY uses `STRIP_OUTER_ARRAY = FALSE` — do not change this to array format.

**dbt materialization policy.** Staging and intermediate are views (always fresh, cheap). Marts are tables (fast for BI). Defined in `airflow/include/dbt/dbt_project.yml`.

**Residential filter sits in the mart, not staging.** `stg_permits` is source-of-truth for all permits. The residential lens (`existing_units IS NOT NULL OR proposed_units IS NOT NULL`) is a reporting concern owned by `mart_housing_production`.

**`normalize_neighborhood` macro.** Collapses DataSF's null/empty/"unknown" neighborhood spellings into `'Unknown'` and `initcap`s the rest. Any future mart that groups by neighborhood must use this macro.

## Airflow → scripts import path

`airflow/Dockerfile` copies `scripts/` to `/opt/airflow/dags/scripts/` at image build time. The DAG does `from scripts import permits`. If you rename or restructure the `scripts/` package, update the `COPY` line in `Dockerfile` and the DAG import.

## dbt grain and tests

The mart grain is `(filed_month, neighborhood, supervisor_district)` — enforced by a `dbt_utils.unique_combination_of_columns` test. Staging PK is `permit_number` with `not_null` + `unique`. Do not introduce aggregations in staging or intermediate that would break these constraints.
