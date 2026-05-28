# TODO

## Performance
- [ ] Migrate staging models from views to incremental materialization — currently views are fine but will become expensive as raw tables grow. Revisit when query performance degrades.
- [ ] Investigate streaming S3 writes per page-batch (avoid full in-memory accumulation in `soda_ingest.run()`) if large datasets cause OOM or API timeout issues during backfill — at 5–10 KB/record, datasets with 500k+ records can approach 2 GB of RAM in a single run.

## Airflow / DAG
- [x] Consolidate dbt tasks — moved to `transform_all.py` shared DAG with `ExternalTaskSensor` fan-in; dbt now runs once per day across all datasets instead of once per ingest DAG.

## Observability
- [ ] Persist Airflow task logs to S3 — currently stored only in Docker containers and lost on `astro dev stop`. Set `AIRFLOW__LOGGING__REMOTE_LOGGING=True` and point at existing S3 bucket.

---

## Airflow code quality (deferred audit, May 2026)

Findings from a pre-public-release senior-engineer review of `airflow/dags/`, `airflow/include/scripts/`, and `airflow/include/sql/`. Captured here so they are tracked in-repo. Severity legend: **B** = blocker, **H** = high, **M/L** = medium / low.

### Blockers
- [ ] **B1** — `airflow/dags/transform_all.py` lines 13–53: `_latest_success` resolves to *yesterday's* successful `update_watermark` run, so `dbt_run_marts` runs against stale data when the ingest DAG and `transform_all` are both scheduled at 06:00 UTC. Fix: migrate to Airflow 3 Assets — emit a `Dataset(...)` from each `update_watermark` task and set `transform_all`'s `schedule=[asset_permits, asset_evictions, asset_incidents]`. Remove the sensor block.
- [ ] **B2** — `airflow/include/sql/update_watermark.sql`: bind-param refactor done (May 2026 polish pass); still need to short-circuit the `update_watermark` task when `records_fetched == 0` so a no-new-records day doesn't write a `NULL`-watermark row.
- [ ] **B4** — `airflow/include/scripts/soda_ingest.py` line ~131: `max(record[...] for record in records)` raises `ValueError` on empty input. Guard with `if not records: return RunResult(s3_path=None, records_fetched=0, max_watermark=None, since=since)`.

### Highs
- [ ] **H1** — `airflow/dags/dag_factory.py` line 35 + `airflow/include/scripts/soda_ingest.py` line ~74: watermark stored as truncated date with `>=` predicate causes duplicate inserts. Either change watermark column to `TIMESTAMP_NTZ` and predicate to strict `>`, or add a `qualify row_number() over (partition by permit_number order by _loaded_at desc) = 1` dedupe in `stg_permits`.
- [ ] **H2** — `airflow/include/scripts/soda_ingest.py`: stream NDJSON to S3 page-by-page instead of accumulating the whole result list in memory (existing TODO above is a duplicate — collapse into this one when fixing).
- [ ] **H3** — `airflow/airflow_settings.yaml` holds a literal Snowflake password. Verified gitignored. Refactor to `${SNOWFLAKE_PASSWORD}` env interpolation, or delete the file (Astro reads `.env` directly).
- [ ] **H4** — Zero alerting: add `on_failure_callback` to a shared `default_args` block in `dag_factory.py` so failed ingests post somewhere (Slack / stdout / log scan).
- [ ] **H5** — `airflow/include/scripts/permits.py:23`, `evictions.py:23`, `incident_reports.py:23` declare `-> tuple[str, date]` but return a 4-field `RunResult`. Change to `-> RunResult`.
- [ ] **H6** — `pyproject.toml` lines 13–15 pin `apache-airflow==2.9.3` while the container runs Airflow 3 (Astro Runtime 3.2-2). Bump local pin to match. *(Done in May 2026 polish pass — verify it stuck.)*

### Followups (medium / low)
- [ ] DAG `start_date` drift across files (2026-03-24 / 2026-04-30 / 2026-05-01) — pick one constant.
- [ ] Audit `catchup=False` on each DAG.
- [ ] Replace classic Operators with TaskFlow `@task` where it tightens the code (especially in `transform_all.py`).
- [ ] Extract magic numbers (`poke_interval=120`, `timeout=10800`) into module constants.

---

## dbt code quality (deferred audit, May 2026)

Findings from a pre-public-release senior-AE review of `dbt/`. Severity legend: **D** = dbt finding (B-blocker / H-high / M-medium).

### Blocker
- [ ] **D1** — `dbt/dbt_project.yml` lines 39–41: project-wide `tests: +severity: warn` neuters all ~66 tests including `unique(permit_number)`, mart-grain `unique_combination_of_columns`, every `accepted_values`. Delete the block. If specific tests are noisy, mark only *those* with `severity: warn` per-model.

### Highs
- [ ] **D2** — `mart_permit_pipeline` is materialized as `table` despite the "snapshot" naming, so every run replaces all rows and history is destroyed. Convert to `materialized='incremental'` keyed on `(permit_number, snapshot_date)` with `on_schema_change='append_new_columns'`.
- [ ] **D3** — `mart_dbt_test_health` 7d/30d aggregations use `qualify row_number() over (...) <= 7` (row-counted, not day-counted). Rewrite predicates as `where snapshot_date >= dateadd(day, -7, current_date())`.
- [ ] **D4** — `dbt/packages.yml` installs `elementary-data/elementary` but `dbt_project.yml` has no `on-run-end` hook calling `elementary.upload_results(results)`. Wire it.
- [ ] **D5** — `supervisor_district` types disagree across staging: permits + incidents cast to `STRING`, evictions casts to `INTEGER`. Standardize on `STRING` (DataSF returns string; values include "Citywide" / null).
- [ ] **D6** — `mart_public_safety` mis-buckets nulls as "open": `open_count` aggregates rows where `resolution IS NULL`, but DataSF uses null for both "no resolution data yet" and "open". Add an explicit `resolution_status` column at staging with `coalesce(resolution, 'unknown')`.
- [ ] **D7** — `dbt/models/metadata/_sources.yml` declares the airflow metadata sources but defines no `loaded_at_field` / `freshness:` block. A wedged Airflow-metadata writer goes unnoticed. Add `loaded_at_field: _loaded_at` and `freshness: warn_after: {count: 6, period: hour}, error_after: {count: 24, period: hour}`.
- [ ] **D8** — Several aggregations use `count(distinct a||'|'||b)` as a "composite key"; `'1|23'` collides with `'12|3'`. Replace with `count(distinct {{ dbt_utils.surrogate_key(['a', 'b']) }})`.
- [ ] **D9** — Most staging YAML files document <55% of the columns selected by their model. Fill the gap, scoped to columns called from downstream first.

### Followups (medium / low)
- [ ] Extract `{% macro latest_record_per_pk(...) %}` from the copy-pasted dedupe block in 3 staging models.
- [ ] Extract a JSON-typing macro from the 33× repeated `payload:field::type` extractor.
- [ ] Add shared `dim_neighborhood` and `dim_date`.
- [ ] Marts are denormalized — consider a fact-vs-dim split in a v2 pass.
- [ ] `use_transition` case-branch ordering buries `commercial_to_residential` and `sfr_to_multifamily` behind `unit_addition`.
- [ ] Lifecycle stage doesn't handle terminal `cancelled` / `withdrawn` statuses.
- [ ] `dbt/tests/` and `dbt/seeds/` are declared in `dbt_project.yml` lines 9–10 but don't exist (harmless warning).
- [ ] No model contracts, no model versioning, no `meta:owner`, no tags.
- [ ] Observability project (`dbt/models/metadata/`) arguably belongs in its own dbt project.

---

## Live dashboard

The `dashboard/` Dash app reads live from Snowflake at request time, so the
"build static site → publish to GitHub Pages" pattern doesn't transfer
directly. Decide between:

- [ ] **Container PaaS** — Render.com / Fly.io / Railway / Cloud Run; deploy `dashboard/Dockerfile` with Snowflake env vars. Always-on infra; needs a public-facing read role.
- [ ] **Static snapshot** — nightly job dumps the marts to JSON/Parquet under `reports/data/` and a small static viewer in `reports/pages/` deploys to GitHub Pages. Portfolio-friendly, no runtime Snowflake.
- [ ] **Hybrid** — keep the Dash app for prod plus a read-only static snapshot site on Pages.
- [ ] **Screenshots only** — record GIFs of the Dash app, link from README.

See [docs/DEPLOY.md](docs/DEPLOY.md).
