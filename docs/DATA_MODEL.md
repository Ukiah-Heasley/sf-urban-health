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

## Consumer snapshot shapes

The Evidence shell reads committed Parquet snapshots for three mart-shaped
tables:

- `mart_housing_production`
- `mart_pipeline_health`
- `mart_data_trust`

`reports/scripts/make_sample_data.py` regenerates deterministic sample rows with
the same column shapes so the static site builds without a warehouse.
