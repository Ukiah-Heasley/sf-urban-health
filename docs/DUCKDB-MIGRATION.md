# DuckDB migration — difficulty assessment

**Status: assessment only. The project ships on Snowflake.** This documents how
hard a move to DuckDB would be, based on a review of the actual Snowflake-
specific surface in the codebase. It is a decision aid, not a committed plan.

## Verdict

The **transform layer is easy** to port; the **ingest/load layer is the real
cost** because it leans on warehouse-native constructs (`COPY INTO`, external
stages, `MERGE`) that DuckDB doesn't have. A DuckDB build is therefore best
thought of as a **second track / re-architecture of the loader**, not an
in-place adapter swap. The read path is already de-risked: the Evidence demo
([reports/](../reports/)) reads marts as parquet **through DuckDB** today.

Rough total: a focused **2–4 day** effort for a parallel DuckDB target that
runs the dbt layer locally on file-based data; longer if the production Airflow
+ S3 + Snowflake pipeline must keep running in parallel.

## Effort by component

| Area | Change required | Effort |
|---|---|---|
| dbt adapter / profile | Add a `duckdb` output in `dbt/profiles.yml` (`dbt-duckdb`); `generate_schema_name` macro is dialect-neutral | **S** |
| dbt model SQL (functions) | `datediff`, `date_trunc`, `count_if`, window fns, `initcap` all exist in DuckDB; `median`/`percentile_cont` and `like any` need small rewrites | **S–M** |
| dbt staging — VARIANT extraction | The `payload:field::type` Snowflake syntax (×~33 across 3 staging models) → DuckDB JSON (`payload->>'field'` + `cast`); best done once via the planned JSON-typing macro (TODO) | **M** |
| `generate_surrogate_key`, `dbt_utils` | dbt_utils + elementary support DuckDB; no change | **S** |
| Raw load (the hard part) | `COPY INTO` + external `S3_STAGE` + `STRIP_OUTER_ARRAY` have no DuckDB equivalent. Replace with DuckDB `read_json_auto('s3://…')` (httpfs) or land files locally and `read_json`. The `copy_into.sql` template and `SQLExecuteQueryOperator` load task go away | **L** |
| Watermarks | `MERGE` upsert into `INGEST_WATERMARKS` → DuckDB `INSERT … ON CONFLICT`, or a small parquet/table; `update_watermark.sql` rewritten | **M** |
| Airflow operators | `SnowflakeHook` (dag_factory, airflow_rest_client) + `SQLExecuteQueryOperator` → DuckDB Python tasks. Airflow's role shrinks to "fetch → land files → `dbt build` over DuckDB" | **M–L** |
| Observability writer | `airflow_rest_client.py` upserts via `SnowflakeHook` + `upsert_*.sql` `MERGE` → DuckDB `ON CONFLICT` | **M** |
| Dashboard | `dashboard/data/snowflake.py` (`snowflake-connector-python` + `fetch_arrow_all`) → `duckdb` (`.arrow()`); cache layer otherwise unchanged (already Arrow→Polars) | **S–M** |
| CI | `dbt-ci.yml` compiles against live Snowflake; with DuckDB it could run a **full `dbt build` + `dbt test`** in CI (no warehouse, no secrets) — a net improvement | **S** (and a win) |
| Evidence demo | Already DuckDB-over-parquet — **no change** | none |

## Detailed notes

- **Where the Snowflake-specific SQL lives:** the `payload:…::type` extraction in
  `dbt/models/staging/stg_{permits,evictions,incidents}.sql`; `count_if`,
  `median`, `like any`, `qualify`/`row_number` in intermediate + marts; the
  `MERGE` statements in `airflow/include/sql/{update_watermark,upsert_*}.sql`;
  the `COPY INTO` in `airflow/include/sql/copy_into.sql`.
- **State to migrate:** `METADATA.INGEST_WATERMARKS` and the two
  `AIRFLOW_*` observability tables — small, easy to recreate as DuckDB tables or
  parquet.
- **httpfs vs. local files:** DuckDB can read S3 directly with the `httpfs`
  extension, preserving "S3 is the durable raw layer". Alternatively land files
  locally for a fully self-contained clone-and-run repo.
- **Materialization policy** (views for staging/intermediate, tables for marts)
  carries over unchanged.

## What gets easier with DuckDB

- **Clone-and-run with zero cloud accounts** — the headline portfolio win, and
  what the reference `ecommerce-analytics-dbt` repo demonstrates.
- **CI can run the full dbt build + tests** (currently parse/compile only,
  because tests need a live warehouse) — this also unblocks TODO **D1**.
- **No warehouse cost**, no `SNOWFLAKE_*` secrets, simpler local dev.

## Suggested sequencing (if pursued)

1. Add a `duckdb` dbt target + the JSON-typing macro; get `dbt build` green on a
   local file-based raw layer (sample data). Lowest risk, highest demo value.
2. Port the loader: a DuckDB ingest task (`read_json_auto` from S3/local) +
   `ON CONFLICT` watermarks, replacing the `COPY INTO`/`MERGE` templates.
3. Point the dashboard + observability writer at DuckDB.
4. Flip CI to a full `dbt build`/`dbt test` and remove the `+severity: warn`
   block (D1).

Trade-off to weigh: the current Snowflake + S3 + managed-Airflow stack is the
more impressive *production data-engineering* story and differentiates this repo
from the DuckDB ecommerce one. Migrating trades that narrative for
reviewability. Keeping Snowflake and shipping the Evidence/DuckDB **read** demo
(done) captures much of the upside without the rebuild.
