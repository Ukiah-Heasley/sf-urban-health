# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All workflows go through the root `Makefile`, which auto-loads `airflow/.env`.

```bash
make ingest         # DataSF -> S3 (uv run airflow/include/scripts/permits.py)
make dbt-deps       # one-time: install dbt packages (dbt_utils, elementary)
make dbt-build      # dbt run + test against Snowflake
make dbt-test
make airflow-up     # astro dev start (UI at http://localhost:8080, admin/admin)
make airflow-down
make airflow-logs   # tail scheduler
make dashboard-dev  # python -m dashboard.app on http://localhost:8050
make lint           # uv run ruff check .
make yamllint       # uv run yamllint .
make pre-commit     # uv run pre-commit run --all-files
make test           # uv run pytest
```

For ad-hoc dbt selectors not covered by a target, run from `dbt/` after sourcing `airflow/.env`:
`uv run --group dbt dbt run --select stg_permits --profiles-dir .`

## Architecture

```
DataSF SODA API → airflow/include/scripts/soda_ingest.py → S3 raw/<dataset>/YYYY/MM/DD/<dataset>.json
S3 → Snowflake RAW.<DATASET>  (COPY INTO via SQLExecuteQueryOperator)
RAW.<DATASET> → METADATA.INGEST_WATERMARKS  (per-dataset high-water mark)
Snowflake RAW → dbt staging → intermediate → marts (transform_all DAG)
Airflow REST API → METADATA.AIRFLOW_DAG_RUNS / AIRFLOW_TASK_INSTANCES → observability marts
```

Five DAGs orchestrate this:
- Three ingest DAGs (one per dataset, factory'd in `airflow/dags/dag_factory.py`), daily at 06:00 UTC.
- `transform_all` runs `dbt build` once after the three ingests succeed.
- `ingest_pipeline_metadata` polls the Airflow REST API every 30 minutes for the observability marts.

## Key design decisions

**S3 is the durable raw layer.** Raw NDJSON lives in S3 permanently. Snowflake `RAW.*` is a `COPY INTO` target. To reprocess, replay from S3 — never re-hit the DataSF API.

**Watermark-driven incremental loads.** Each ingest reads its last watermark from `METADATA.INGEST_WATERMARKS` (epoch fallback per `DatasetConfig.epoch`), fetches `where data_loaded_at >= <wm>`, and writes the new watermark via Snowflake `MERGE` after a successful load. Failed loads cause the next run to re-fetch the gap automatically. (A known boundary-overlap quirk on the `>=` predicate is tracked in TODO.md item H1.)

**Newline-delimited JSON.** `_write_s3` emits one JSON object per line. The COPY uses `STRIP_OUTER_ARRAY = FALSE` — do not change this to array format.

**dbt materialization policy.** Staging and intermediate are views (always fresh, cheap). Marts are tables (fast for BI). Defined in `dbt/dbt_project.yml`.

**Residential filter sits in the mart, not staging.** `stg_permits` is source-of-truth for all permits. The residential lens (`existing_units IS NOT NULL OR proposed_units IS NOT NULL`) is a reporting concern owned by `mart_housing_production`.

**`normalize_neighborhood` macro.** Collapses DataSF's null/empty/"unknown" neighborhood spellings into `'Unknown'` and `initcap`s the rest. Any future mart that groups by neighborhood must use this macro.

## SQL templating: bind params vs Jinja

`SQLExecuteQueryOperator` accepts both `params=` (Jinja-templated render) and `parameters=` (driver bind). The convention in this repo:
- **Identifiers and stage-path components** → `params={...}` (Jinja). Snowflake bind parameters cannot bind table identifiers or stage paths. See `airflow/include/sql/copy_into.sql`.
- **Values** (dataset names, timestamps, IDs) → `parameters={...}` (driver bind). Eliminates SQL injection surface even when inputs come from XCom. See `airflow/include/sql/update_watermark.sql` and `upsert_dag_runs.sql`.

## Airflow → scripts import path

Scripts live in `airflow/include/scripts/`. Astro auto-mounts `include/` at `/usr/local/airflow/include/` in all containers; the Dockerfile sets `PYTHONPATH` to include that directory so `from scripts import permits` resolves. No `COPY` step is needed — changes to scripts are picked up automatically on `astro dev restart` without a full image rebuild.

## dbt grain and tests

The mart grain is `(filed_month, neighborhood, supervisor_district)` — declared via a `dbt_utils.unique_combination_of_columns` test. Staging PK is `permit_number` with `not_null` + `unique`. **Important caveat:** the project-level `tests: +severity: warn` block in `dbt/dbt_project.yml` neuters every test today; removing that block (TODO.md D1) is the next blocker for true grain enforcement.

Do not introduce aggregations in staging or intermediate that would break these constraints.

## Where deferred work lives

`TODO.md` has two sections, "Airflow code quality (deferred audit)" and "dbt code quality (deferred audit)", that capture every blocker / high finding from the May 2026 polish review with file/line citations. When making changes to `airflow/dags/`, `airflow/include/`, or `dbt/`, check those sections first — many "obvious" bugs are already known and intentionally deferred to a follow-up pass.

## Testing the dashboard locally

`make test` runs the dev-only test subset by default. The dashboard import probe uses subprocess + skipif to handle wheel-loadability issues on bleeding-edge platforms — set `SKIP_DASHBOARD_TESTS=1` if your local pyarrow / polars / snowflake-connector-python wheels can't load (e.g. very recent macOS arm64). CI on Linux x86_64 always exercises the import.
