# Data Model

## Raw S3 records

Each DataSF response record is preserved as one JSON object per line. The raw
extractor does not add columns to the record itself.

| Dataset | SODA resource | Configured timestamp | Stable order key |
| --- | --- | --- | --- |
| permits | `i98e-djp9` | `data_loaded_at` | `permit_number` |
| evictions | `5cei-gny5` | `data_loaded_at` | `eviction_id` |
| incidents | `wg3w-h783` | `data_loaded_at` | `row_id` |

Raw object identity is the dataset plus normalized interval bounds. Retrying the
same interval overwrites the same key.

## Lakehouse contract registry

The repository defines YAML contracts under `contracts/lakehouse/` for parquet
table layouts in bronze, silver, gold, and metadata layers. Each contract
declares grain, partition columns, column types, quality checks, and an S3 path
template under `lake/parquet/{layer}/{name}/`.

`airflow/include/scripts/lakehouse_contracts.py` loads and validates those
contracts. `airflow/include/scripts/lakehouse_load.py` promotes raw NDJSON into
bronze Parquet. `airflow/include/scripts/lakehouse_metadata.py` writes current S3 metadata
events, attempt audit events, and compacts current ingest events into
contract-compatible metadata Parquet.

Bronze promotion is source-faithful: no deduplication, residential filtering,
status normalization, or downstream derivations such as `eviction_type`. Bronze
contracts require shared lineage metadata columns (`_ingest_run_id`,
`_raw_s3_path`, `_raw_s3_key`, `_data_interval_start`, `_data_interval_end`,
`_effective_start`, `_loaded_at`, `_extracted_at`, `_source_dataset_id`,
`_record_hash`, `_raw_payload`) plus the dataset natural key. Gold contract
grains match the analytical marts documented below.

| Bronze table | Natural key | Partition columns |
| --- | --- | --- |
| `permits` | `permit_number` | `data_interval_start`, `data_interval_end` |
| `evictions` | `eviction_id` | `data_interval_start`, `data_interval_end` |
| `incidents` | `row_id` | `data_interval_start`, `data_interval_end` |

S3 JSON metadata events under `lake/metadata/events/` are the durable metadata
source of truth for promotion control flow:

- `ingest_runs_current/` holds one overwriteable current event per dataset
  interval; the planner and compaction read this prefix only.
- `ingest_run_attempts/` records per-run audit events and does not drive
  promotion.
- `file_manifest/` records every emitted bronze Parquet object.

The compacted `ingest_runs` metadata table grain is
`(dataset_name, data_interval_start, data_interval_end)` with `ingest_run_id` as
descriptive lineage for the latest extract.

`plan_lakehouse_intervals` groups current ingest events by
`(data_interval_start, data_interval_end)`, requires all three datasets, and
selects at most one interval per DAG run. Compacted metadata Parquet is a
query/reporting layer only and is fully rebuilt from JSON on each compaction.

`compact_lakehouse_metadata` deletes the full metadata Parquet output prefixes
under `lake/parquet/metadata/ingest_runs/` and
`lake/parquet/metadata/file_manifest/`, then compacts current JSON events into
fresh metadata Parquet under `lake/parquet/metadata/`. Metadata export files are
not self-manifested. DuckDB is used only inside that single compaction task as
an in-memory engine.

## Snowflake dbt sources

The current dbt project declares three `SF_URBAN_HEALTH.RAW` tables containing
VARIANT payloads and `_loaded_at` ingestion timestamps:

- `raw.permits`
- `raw.evictions`
- `raw.incidents`

It separately declares Airflow run and task-instance tables in the Snowflake
`METADATA` schema.

## Staging

| Model | Grain | Behavior |
| --- | --- | --- |
| `stg_permits` | `permit_number` | Latest payload per permit; typed permit, status, date, cost, unit, address, and use fields |
| `stg_evictions` | `eviction_id` | Latest payload per eviction; typed filing, location, and reason flags; derived eviction type |
| `stg_incidents` | `row_id` | Latest payload per row; typed report, incident, resolution, district, and coordinate fields |
| `stg_airflow_dag_runs` | `run_id` | Cleaned Airflow DAG-run state and duration |
| `stg_airflow_task_instances` | `(dag_id, run_id, task_id)` | Cleaned task state plus extract count, timestamp, and S3-path fields |

Source staging models filter out rows whose source primary key is null. They do
not apply analytical population filters.

## Intermediate

- `int_permit_timelines` keeps one row per permit and derives lifecycle timing,
  unit change, cost-per-unit, and use transition.
- `int_incident_timelines` keeps one row per incident row and derives time
  buckets, weekend status, and resolution status.

## Analytical marts

| Mart | Declared grain |
| --- | --- |
| `mart_housing_production` | `(filed_month, neighborhood, supervisor_district, use_transition)` |
| `mart_evictions` | `(filed_month, neighborhood, supervisor_district, eviction_type)` |
| `mart_public_safety` | `(incident_month, neighborhood, supervisor_district, police_district, incident_category)` |
| `mart_permit_pipeline` | `(neighborhood, supervisor_district, lifecycle_stage, age_bucket)` |

`mart_housing_production` applies the residential lens: a permit must include
residential unit data. Staging continues to represent all permits.

## Observability marts

| Mart | Grain or output scope |
| --- | --- |
| `mart_pipeline_health` | `(run_date, dag_id)` |
| `mart_dbt_test_health` | `(run_date, run_id, test_name)` |
| `mart_pipeline_summary` | one row per DAG |
| `mart_data_trust` | one row per configured dataset |

The current project-wide dbt configuration sets generic test severity to
`warn`. Tests report violations but do not fail the dbt command solely because
of those violations.

## Shared conventions

- `normalize_neighborhood(column)` maps null, empty, and case-insensitive
  `unknown` values to `Unknown`, then applies `initcap` to other values.
- Staging and intermediate models are views.
- Mart and metadata models are tables unless a model overrides the project
  default.
- `_loaded_at` is the retained warehouse ingestion timestamp used for latest-row
  selection and freshness, distinct from source business timestamps.
