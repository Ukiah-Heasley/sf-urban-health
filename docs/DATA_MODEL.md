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

The repository defines YAML contracts under `contracts/lakehouse/` for bronze
Parquet layouts and silver/gold relation semantics. Parquet contracts declare
grain, partition columns, column types, quality checks, and an S3 path template
under `lake/parquet/{layer}/{name}/`. Iceberg silver contracts such as
`permits_current` declare `table_format: iceberg` plus Spark catalog schema/name
instead of a Parquet path template.

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
grains describe the analytical marts the lakehouse path is designed to support.

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

## Local silver development (dbt + Spark + Iceberg)

The `lakehouse/` Compose stack provides MinIO, deterministic bucket creation,
and Spark Thrift Server with pinned Iceberg and S3A dependencies. dbt connects
through `dbt/profiles.yml`.

Local fixture preparation (`make lakehouse-prepare-permits-fixture`) is
destructive to the local MinIO bucket. It seeds
`tests/fixtures/lakehouse/permits.ndjson` as raw NDJSON under
`raw/permits/data_interval_start=20240315T060000Z/data_interval_end=20240316T060000Z/records.ndjson`,
writes the ingest metadata event, promotes bronze Parquet to
`lake/parquet/bronze/permits/.../records.parquet`, and restarts Spark Thrift so
catalog namespaces are rebuilt after the bucket wipe.

`stg_bronze_permits` is an ephemeral dbt staging model over the bronze Parquet
prefix in MinIO. It is inlined into `permits_current` because the Iceberg
catalog does not support persisted views.
`permits_current` reads that staging relation, deduplicates to one latest row per `permit_number`
using `_loaded_at desc` with deterministic tie-breakers, derives
`current_status = lower(status)` and `completed_at` from `status_date` when
status is complete, filters only null `permit_number`, and materializes as an
Iceberg table in `sf_urban_health`. The `smoke_iceberg` model remains a harmless
local connectivity check and does not read bronze.

## Consumer snapshot shapes

The Evidence shell reads committed Parquet snapshots for three mart-shaped
tables:

- `mart_housing_production`
- `mart_pipeline_health`
- `mart_data_trust`

`reports/scripts/make_sample_data.py` regenerates deterministic sample rows with
the same column shapes so the static site builds without a warehouse.
