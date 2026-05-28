# Data Model

## Sources

All sources are DataSF SODA endpoints landed verbatim into S3 and
re-loaded into Snowflake `RAW.*`. The shape is always `{ payload VARIANT }`
— one row per record, lossless.

| dbt source | DataSF dataset | RAW table | Watermark column |
|---|---|---|---|
| `raw.permits` | [`i98e-djp9`](https://data.sfgov.org/Housing-and-Buildings/Building-Permits/i98e-djp9) | `RAW.PERMITS` | `data_loaded_at` |
| `raw.evictions` | [`5cei-gny5`](https://data.sfgov.org/Housing-and-Buildings/Eviction-Notices/5cei-gny5) | `RAW.EVICTIONS` | `data_loaded_at` |
| `raw.incidents` | [`wg3w-h783`](https://data.sfgov.org/Public-Safety/Police-Department-Incident-Reports-2018-to-Present/wg3w-h783) | `RAW.INCIDENTS` | `data_loaded_at` |
| `metadata.airflow_dag_runs` | Airflow REST API | `METADATA.AIRFLOW_DAG_RUNS` | `start_date` |
| `metadata.airflow_task_instances` | Airflow REST API | `METADATA.AIRFLOW_TASK_INSTANCES` | `start_date` |

## Staging

| Model | PK | Notable typing / cleaning |
|---|---|---|
| `stg_permits` | `permit_number` | `existing_units` / `proposed_units` cast to `INTEGER`; `neighborhood` normalized via `normalize_neighborhood`. |
| `stg_evictions` | `eviction_id` | `eviction_type` derived from no-fault / Ellis Act / for-cause flags; `supervisor_district` cast to `INTEGER`. |
| `stg_incidents` | `row_id` | `incident_category` initcap'd; `police_district` and `supervisor_district` cast to `STRING`. |
| `stg_airflow_dag_runs` | `(dag_id, run_id)` | Durations from `start_date` / `end_date` deltas. |
| `stg_airflow_task_instances` | `(dag_id, run_id, task_id)` | XCom payloads (`records_fetched`, `max_watermark`, `s3_path`) lifted out for the extract tasks. |

PK tests (`unique` + `not_null`) are declared in the per-model YAML files
under `dbt/models/staging/_stg_*.yml`.

> **Heads-up:** the project-level `tests: +severity: warn` in
> `dbt/dbt_project.yml` is a known-deferred bug — see TODO.md item D1.
> Tests *report* warnings today but don't fail the build. The intent is
> for these PKs to be hard constraints, and that fix is on the dbt
> code-quality follow-up checklist.

## Intermediate

- `int_permit_timelines` — one row per permit-status transition, derived
  from `stg_permits` plus the status-history columns. Used by
  `mart_permit_pipeline`.
- `int_incident_timelines` — one row per incident; computes lifecycle
  bucketing for `mart_public_safety`.

## Marts

### `mart_housing_production`

**Grain:** `(filed_month, neighborhood, supervisor_district)` — enforced
by `dbt_utils.unique_combination_of_columns`.

The residential lens (`existing_units IS NOT NULL OR proposed_units IS NOT NULL`)
sits in this mart, not in staging — see [[Design-Decisions]].

Key columns: `permits_filed`, `net_units_added`, `total_project_cost`,
`avg_cost_per_unit`, `median_days_to_issue`, `use_transition`.

### `mart_evictions`

**Grain:** `(filed_month, neighborhood, eviction_type)`.

Key columns: `eviction_count`, `ellis_act_count`, `no_fault_pct`.

### `mart_public_safety`

**Grain:** `(incident_month, neighborhood, incident_category)`.

Key columns: `total_incidents`, `resolved_count`, `open_count`,
`police_district`.

### `mart_permit_pipeline`

**Grain:** `(permit_number, snapshot_date)`. Currently materialized as
`table` (snapshot semantics broken — see TODO.md D2).

### Observability marts (under `dbt/models/metadata/`)

| Mart | Grain | What it surfaces |
|---|---|---|
| `mart_pipeline_health` | `(run_date, dag_id)` | Per-DAG success rate + duration |
| `mart_dbt_test_health` | `(snapshot_date, model_name, test_name)` | Test-by-test pass/fail trend |
| `mart_pipeline_summary` | `dag_id` | Aggregate "how often does this DAG succeed?" |
| `mart_data_trust` | `dataset_name` | Composite freshness × pass-rate × volume score |

## Macros

- `normalize_neighborhood(col)` — collapses DataSF's null / empty /
  "unknown" / "Unknown" neighborhood spellings to `'Unknown'` and
  `initcap`s the rest. Required on every neighborhood-grain mart.
- (planned, see TODO.md) `latest_record_per_pk(...)` — extract the
  copy-pasted dedupe block in the three staging models.
- (planned) JSON-typing helper to replace the 33× `payload:field::type`
  pattern.
