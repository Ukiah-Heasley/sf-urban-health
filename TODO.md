# TODO

Tracked follow-ups. The "deferred audit" sections capture findings from the
senior-engineer review with file/line citations; check them before editing
`airflow/dags/`, `airflow/include/`, or `dbt/` — several "obvious" bugs are
known and intentionally deferred.

---

## Done in the May 2026 polish pass

Fixed and verified offline (ruff / pytest / `dbt parse` / yamllint / Evidence build):

- **D5** — `stg_evictions.supervisor_district` now `::string` (was `integer`); redundant cast removed from `mart_evictions`.
- **D6** — `int_incident_timelines` adds non-null `resolution_status` (`coalesce(resolution,'Unknown')`); `mart_public_safety` splits `open_count` (true "Open or Active") from new `unknown_count`.
- **D7** — `loaded_at_field` + `freshness` added to the metadata sources (`_sources.yml`).
- **D8** — `mart_data_trust` composite key uses `dbt_utils.generate_surrogate_key([...])` instead of `a || '|' || b`.
- **D9** — every selected column in `_stg_permits.yml` / `_stg_incidents.yml` is now documented.
- **H5** — the mis-typed `run() -> tuple[str, date]` wrappers are gone; the 3 CLI extractors share `soda_ingest.cli(config)` (removed ~60 lines of duplication).
- **H6** — verified: `pyproject.toml` pins `apache-airflow==3.0.2` (matches the Astro image).
- **New (fixed):** `mart_permit_pipeline` referenced a same-SELECT alias (`days_in_stage`) Snowflake rejects → now `avg(days_since_filed)`.
- **New (fixed):** dead always-null `execution_time_seconds` column dropped from `mart_dbt_test_health`.
- **New (fixed):** `dashboard/pages/housing.py` crashed the whole app at import when a mart loaded empty (no Snowflake); now guarded like the evictions/incidents pages.
- **Docs/CI:** mart grains reconciled across CLAUDE.md / README / wiki to match the actual `unique_combination_of_columns` tests; "every 30 min" → "daily 07:00 UTC" (the DAG is `0 7 * * *`); `ingest_pipeline_metadata` docstring/description `RAW` → `METADATA`; created real `snowflake/bootstrap.sql` (DEPLOY.md referenced a missing file); removed dead `LOCAL_DATA_DIR`; corrected the inaccurate `dag-integrity` CI comment; added README badges + live-demo link.
- **Live demo:** built the Evidence static-snapshot → GitHub Pages pipeline (`reports/`, `pages.yml`). See [docs/DEPLOY.md](docs/DEPLOY.md).

---

## Done in the SODA ingestion hardening pass

Fixed and verified offline (ruff / pytest / DAG integrity):

- **B1** — replaced `transform_all`'s `ExternalTaskSensor` fan-in with Airflow
  Assets emitted by each ingest DAG's terminal `ingest_complete` task.
- **B2 + B4** — no-new-records days now return
  `RunResult(s3_path=None, max_watermark=None, records_fetched=0, ...)`, skip
  `COPY INTO` and watermark update, and still emit the ingest-complete asset.
- **H1** — watermarks now stay timestamp-grain and scheduled increments query
  DataSF with strict `data_loaded_at > watermark`; first loads include the
  dataset epoch boundary.
- **H2** — `soda_ingest.py` now streams SODA records through
  `NdjsonS3Writer` instead of accumulating the full response list in memory.
- **Airflow 3 imports** — touched DAGs now use `airflow.sdk` plus
  `airflow.providers.standard.*` operator imports.
- **New (fixed):** `count_records()` remains available for diagnostics, but
  the daily ingest path no longer issues an extra full-count API request.

---

## Security (high)

- [ ] **H3** — `airflow/airflow_settings.yaml` holds a **plaintext Snowflake password** (gitignored, never committed — verified). Action: **rotate that Snowflake password**, then either interpolate `conn_password: ${SNOWFLAKE_PASSWORD}` (add a tracked `airflow_settings.yaml.example`) or delete the file and define the connection via `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` in `.env`. Managed deploys should define the connection in the platform UI (see DEPLOY.md).

---

## Performance

- [ ] Migrate staging from views to incremental as raw tables grow (fine as views today; revisit when query perf degrades).

## Observability

- [ ] Persist Airflow task logs to S3 (`AIRFLOW__LOGGING__REMOTE_LOGGING=True`) — currently lost on `astro dev stop`.

---

## Airflow code quality (deferred audit, May 2026)

Severity: **B** blocker / **H** high / **M·L** medium·low.

### Blockers
- None open.

### Highs
- [ ] **H4** — zero alerting: add an `on_failure_callback` to a shared `default_args` in `dag_factory.py` (Slack / stdout / log scan).

### Followups (M·L)
- [ ] DAG `start_date` drift across files — pick one constant.
- [ ] Audit `catchup=False` on each DAG.

---

## dbt code quality (deferred audit, May 2026)

### Blocker
- [ ] **D1** — remove the project-wide `tests: +severity: warn` in `dbt/dbt_project.yml` so tests actually fail the build (left in place this pass at the owner's request; remove + re-test against Snowflake, then mark per-test `severity: warn` only where genuinely noisy). Unblocks adding `dbt test` to CI.

### Highs
- [ ] **D2** — `mart_permit_pipeline` is `table` despite "snapshot" semantics, so history is overwritten each run. Convert to `materialized='incremental'` keyed on `(neighborhood, supervisor_district, lifecycle_stage, age_bucket, snapshot_date)` with `on_schema_change='append_new_columns'`.
- [ ] **D3** — `mart_dbt_test_health` 7d/30d rolling windows use `rows between N preceding and current row` (row-counted, not calendar-day). Rewrite as day-bounded (`range`/date predicate) so gaps in runs don't skew the averages.
- [ ] **D4** — `packages.yml` installs `elementary` but `dbt_project.yml` has no `on-run-end: "{{ elementary.upload_results(results) }}"` hook. Wire it (or document that Elementary writes on its own model run).

### Followups (M·L)
- [ ] Extract `latest_record_per_pk(...)` macro from the copy-pasted dedupe block in the 3 staging models.
- [ ] Extract a JSON-typing macro for the ~33× `payload:field::type` pattern (also the main lever for the DuckDB port — see [docs/DUCKDB-MIGRATION.md](docs/DUCKDB-MIGRATION.md)).
- [ ] Add shared `dim_neighborhood` and `dim_date`.
- [ ] Marts are denormalized — consider a fact/dim split in v2.
- [ ] `int_permit_timelines.use_transition`: reorder the `case` so `commercial_to_residential` / `sfr_to_multifamily` aren't shadowed by `unit_addition`.
- [ ] Lifecycle stage doesn't handle terminal `cancelled` / `withdrawn`.
- [ ] No model contracts, versioning, `meta:owner`, or tags (beyond `daily`).
- [ ] Observability project (`dbt/models/metadata/`) arguably belongs in its own dbt project.
- [ ] **New (L):** `mart_data_trust.days_since_last_load` is `NULL` for never-loaded datasets (the `datediff` uses the un-coalesced `last_loaded_date`); cosmetic — `coalesce` it.

---

## Dashboard

- [ ] **New (L):** `dashboard/components/figures.py` registers the Plotly templates for *all* pages as an import side-effect (`app.py` imports it first). Move template registration into `theme_utils` (or an explicit `register_templates()`) so page-module import order isn't load-bearing.

---

## Live dashboard — DECIDED

Resolved in the May 2026 pass: **static Evidence snapshot published to GitHub
Pages** (free, no runtime warehouse), with the Plotly Dash app kept as the
optional live/operator view. Implementation in [`reports/`](reports/) +
[`.github/workflows/pages.yml`](.github/workflows/pages.yml); see
[docs/DEPLOY.md](docs/DEPLOY.md). Remaining owner action: enable Pages
(Source = GitHub Actions) and confirm the `SNOWFLAKE_*` secrets.
