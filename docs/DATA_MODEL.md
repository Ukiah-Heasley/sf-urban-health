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
under `lake/parquet/{layer}/{name}/`. Iceberg silver and gold contracts declare
`table_format: iceberg` plus Spark catalog schema/name instead of a Parquet path
template.

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
grains describe the business-facing analytical tables the lakehouse path
materializes in the `gold/` dbt layer.

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

## Medallion naming (dbt + contracts)

The lakehouse dbt project uses medallion vocabulary consistently:

| Layer | Role | dbt folder | Examples |
| --- | --- | --- | --- |
| Bronze | Source-faithful records plus lineage (Python promotion; dbt read adapters) | `dbt/models/bronze/` | `bronze_permits`, `bronze_evictions`, `bronze_incidents` |
| Silver | Cleaned, validated, deduped entity tables (Iceberg) | `dbt/models/silver/` | `permits_current`, `evictions_current`, `incidents_current` |
| Gold | Business-facing analytical tables (Iceberg) | `dbt/models/gold/` | `housing_production`, `permit_pipeline`, `evictions`, `public_safety` |

Do not mix dbt `staging/`, `intermediate/`, `stg_*`, `int_*`, or `mart_*`
model names with this lakehouse slice. Gold tables do not use a `mart_` prefix
because the `gold` layer already denotes business-facing marts.

## Local lakehouse development (dbt + Spark + Iceberg)

The `lakehouse/` Compose stack provides MinIO, deterministic bucket creation,
and Spark Thrift Server with pinned Iceberg and S3A dependencies. dbt connects
through `dbt/profiles.yml`.

Local fixture preparation (`make lakehouse-prepare-fixtures`) is destructive to
the local MinIO bucket. It seeds
`tests/fixtures/lakehouse/{permits,evictions,incidents}.ndjson` as raw NDJSON
under production-shaped interval keys, writes ingest metadata events, promotes
bronze Parquet for all three datasets, and restarts Spark Thrift after the
bucket wipe. `lakehouse-prepare-permits-fixture` is a compatibility alias.

`bronze_*` models are ephemeral dbt read adapters over bronze Parquet prefixes
in MinIO. They are inlined into downstream models because the Iceberg catalog
does not support persisted views.

Silver `*_current` models deduplicate bronze to one latest row per natural key
using `_loaded_at desc` with deterministic tie-breakers (`_extracted_at`,
`_data_interval_end`, `_record_hash`). `permits_current` derives
`current_status = lower(status)` and `completed_at` from `status_date` when
status is complete. `evictions_current` coerces nullable boolean cause flags to
false and derives `eviction_type` as `no_fault` when any no-fault flag is true,
otherwise `at_fault`. All three silver models materialize as Iceberg tables in
`sf_urban_health`.

Gold models aggregate silver for monthly housing production, in-flight permit
pipeline snapshots, monthly eviction counts, and monthly public-safety incident
counts. They materialize as Iceberg tables in `sf_urban_health`. The
`smoke_iceberg` model remains a harmless local connectivity check and does not
read bronze.

## Consumer snapshot shapes

The Evidence shell reads committed Parquet snapshots for three mart-shaped
tables:

- `mart_housing_production`
- `mart_pipeline_health`
- `mart_data_trust`

These snapshot names predate the lakehouse gold rename and remain the Evidence
source contract. `make export-evidence-snapshots` writes
`mart_housing_production.parquet` from the gold Iceberg table
`sf_urban_health.housing_production` after local lakehouse gold builds.
`mart_pipeline_health.parquet` and `mart_data_trust.parquet` remain
deterministic observability snapshots in this slice because lakehouse metadata
Parquet is not registered in the Spark catalog.

`reports/scripts/make_sample_data.py` remains the fallback demo-data generator
when Spark is unavailable. Dash is not wired to live Spark/Iceberg gold tables.
GitHub Pages builds from committed snapshots only.
