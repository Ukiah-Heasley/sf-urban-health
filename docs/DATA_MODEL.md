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

The loader and validator live in
`airflow/include/scripts/lakehouse_contracts.py`. It is checked in for future
loaders, dbt models, and tests; it does not write parquet or replace the
current Snowflake path.

Bronze contracts require shared lineage metadata columns (`_ingest_run_id`,
`_raw_s3_path`, `_raw_s3_key`, interval bounds, `_loaded_at`, `_extracted_at`,
`_source_dataset_id`, `_record_hash`, `_raw_payload`) plus the dataset natural
key. Gold contract grains match the analytical marts documented below.

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
