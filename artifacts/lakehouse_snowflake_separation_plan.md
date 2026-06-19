# Lakehouse And Snowflake Separation Plan

Purpose: introduce the S3/DuckDB/DuckLake lakehouse path without tangling it
with the existing Snowflake implementation.

The goal is not to delete Snowflake code in the first pass. The goal is to make
the active local path lakehouse-oriented while preserving Snowflake as a
reference/backout path.

---

## Design Principle

Separate by responsibility, not by sprinkling conditionals everywhere.

Good separation:

```text
shared extraction code
  -> snowflake-specific load/state path
  -> lakehouse-specific load/state path
```

Avoid:

```text
one large DAG factory with if target == "snowflake" blocks everywhere
one dbt source file that mixes Snowflake RAW and Parquet/DuckLake details
one script that both extracts API data and mutates warehouse state
```

The clean boundary is:

```text
DataSF extraction is shared.
Warehouse/lake loading is target-specific.
dbt profiles and sources select the active target.
Airflow DAG factories are target-specific or use thin target adapters.
```

---

## Current Snowflake Surface Area

Keep these as the Snowflake reference path:

```text
airflow/dags/dag_factory.py
airflow/include/sql/copy_into.sql
airflow/include/sql/update_watermark.sql
dbt/profiles.yml snowflake targets
dbt/models/staging/_sources.yml Snowflake RAW sources
Snowflake JSON extraction syntax in current staging models
```

Current Snowflake flow:

```text
DataSF -> S3 raw NDJSON -> Snowflake RAW -> dbt Snowflake models
Snowflake METADATA.INGEST_WATERMARKS controls extraction state
```

Future lakehouse flow:

```text
DataSF -> S3 raw NDJSON -> S3 Parquet/DuckLake bronze -> dbt-duckdb models
Airflow data intervals control extraction state
lake metadata records interval success
```

---

## Proposed Code Layout

### Shared Extraction

Keep SODA/API extraction reusable:

```text
airflow/include/scripts/soda_ingest.py
```

Shared responsibilities:

```text
DatasetConfig
ExtractWindow
SodaClient
S3NdjsonWriter
extract_to_raw(...)
timestamp parsing/formatting
S3 raw key construction
```

Do not put Snowflake, DuckDB, DuckLake, dbt, or warehouse-specific state logic
in this module.

### Snowflake Path

Move or preserve Snowflake-specific orchestration under explicit names:

```text
airflow/dags/snowflake_dag_factory.py
airflow/include/scripts/snowflake_load.py        optional
airflow/include/sql/snowflake/copy_into.sql
airflow/include/sql/snowflake/update_watermark.sql
```

If files are not moved immediately, at least make the names and imports clear:

```text
make_snowflake_ingest_dag(...)
SnowflakeDagConfig
load_s3_to_snowflake
update_snowflake_watermark
```

### Lakehouse Path

Add a parallel lakehouse lane:

```text
airflow/dags/lakehouse_dag_factory.py
airflow/include/scripts/lakehouse_load.py
airflow/include/sql/lakehouse/                 optional
```

Lakehouse responsibilities:

```text
load_raw_ndjson_to_bronze(...)
record_ingest_run(...)
build_duckdb_connection(...)
attach_ducklake(...)
write_bronze_parquet_or_ducklake(...)
```

Suggested naming:

```text
LakehouseDagConfig
make_lakehouse_ingest_dag(...)
extract_{dataset}_to_raw
load_raw_to_bronze
record_ingest_run
```

---

## DAG Separation

### Option A: Separate Factories

Preferred for clarity:

```text
snowflake_dag_factory.py
lakehouse_dag_factory.py
```

Each dataset DAG imports one factory:

```python
# ingest_permits.py
from lakehouse_dag_factory import LakehouseDagConfig, make_lakehouse_ingest_dag
```

Snowflake can stay available as:

```python
from snowflake_dag_factory import SnowflakeDagConfig, make_snowflake_ingest_dag
```

Pros:

- Easy to reason about.
- No target-condition branches inside DAG construction.
- Airflow task IDs can differ cleanly by target.
- Snowflake rollback path stays obvious.

Cons:

- Some duplicated DAG skeleton code.
- Shared retry/tags/default args should be extracted if duplication grows.

### Option B: One Factory With Target Adapters

Acceptable later, but not necessary for v1:

```text
dag_factory.py
  IngestTargetProtocol
  SnowflakeTarget
  LakehouseTarget
```

This is cleaner only after both paths are stable. Before then, target adapters
can become premature abstraction.

Recommendation:

```text
Use separate factories first.
Extract shared DAG defaults later if repetition becomes annoying.
```

---

## Active Path Selection

Avoid runtime conditionals inside one DAG. Instead, decide which DAG modules are
active.

Simple option:

```text
ingest_permits.py imports lakehouse factory
ingest_evictions.py imports lakehouse factory
ingest_incidents.py imports lakehouse factory
```

Keep Snowflake modules in the repo but not imported by active DAG files.

If both DAG families must coexist in Airflow, suffix IDs explicitly:

```text
ingest_permits_lakehouse
ingest_permits_snowflake
transform_all_lakehouse
transform_all_snowflake
```

Do not run both under the same asset names unless you intentionally want both to
trigger the same downstream transform.

---

## Asset Separation

Current assets:

```text
ingest_permits
ingest_evictions
ingest_incidents
```

For a clean migration, either:

### Option A: Reuse Assets For Active Path Only

Only the active lakehouse DAG emits:

```text
ingest_permits
ingest_evictions
ingest_incidents
```

Snowflake DAGs are disabled or renamed.

This keeps `transform_all` conceptually unchanged.

### Option B: Target-Specific Assets

Use:

```text
lakehouse_ingest_permits
lakehouse_ingest_evictions
lakehouse_ingest_incidents

snowflake_ingest_permits
snowflake_ingest_evictions
snowflake_ingest_incidents
```

Then create target-specific transform DAGs.

Recommendation:

```text
Use Option A while only one path is active.
Use Option B only if both paths run side-by-side.
```

---

## dbt Separation

### Profiles

Keep Snowflake targets:

```yaml
dev:
  type: snowflake

prod:
  type: snowflake
```

Add DuckDB/lakehouse targets:

```yaml
duckdb:
  type: duckdb
  path: "{{ env_var('DUCKDB_PATH', 'var/sf_urban_health.duckdb') }}"
  schema: main
  extensions:
    - httpfs
    - parquet
    - ducklake
```

If DuckLake is used:

```yaml
lakehouse:
  type: duckdb
  path: "{{ env_var('DUCKDB_PATH', 'var/sf_urban_health.duckdb') }}"
  schema: main
  extensions:
    - httpfs
    - parquet
    - ducklake
  attach:
    - path: "ducklake:{{ env_var('DUCKLAKE_CATALOG', 'var/sf_urban_health.ducklake') }}"
      alias: lake
      options:
        data_path: "{{ env_var('DUCKLAKE_DATA_PATH', 's3://bucket/lake/ducklake/') }}"
```

Exact `attach` syntax should be verified during implementation against the
installed `dbt-duckdb` version.

### Source Definitions

Avoid mixing Snowflake and lakehouse sources in one file if target-specific
syntax differs substantially.

Option:

```text
dbt/models/staging/_sources_snowflake.yml
dbt/models/staging/_sources_lakehouse.yml
```

Use target-aware `enabled` config:

```yaml
config:
  enabled: "{{ target.type == 'snowflake' }}"
```

and:

```yaml
config:
  enabled: "{{ target.type == 'duckdb' }}"
```

Alternative:

```text
dbt/models/snowflake/staging/...
dbt/models/lakehouse/staging/...
```

That is heavier, but it may be useful while Snowflake JSON syntax and DuckDB SQL
syntax diverge.

Recommendation:

```text
Use target-specific source YAML.
Keep shared model names where SQL is portable.
Fork only the staging models that need warehouse-specific JSON syntax.
```

### Model Forking

Snowflake staging currently depends on syntax like:

```sql
payload:permit_number::string
payload:filed_date::timestamp_ntz
```

DuckDB/lakehouse staging should use typed bronze columns where possible:

```sql
permit_number
filed_at
_loaded_at
```

To avoid one model full of adapter branches, use:

```text
dbt/models/staging/snowflake/stg_permits.sql
dbt/models/staging/lakehouse/stg_permits.sql
```

or adapter-dispatched macros for field extraction only:

```text
macros/json_value.sql
macros/snowflake/json_value.sql
macros/duckdb/json_value.sql
```

Recommendation:

```text
Prefer typed bronze columns and keep lakehouse staging SQL simple.
Keep Snowflake staging untouched until Snowflake is retired.
```

---

## Transform DAG Separation

Current `transform_all.py` runs dbt with:

```text
--target prod
```

For lakehouse, avoid changing the meaning of `prod` immediately. Add explicit
targets:

```text
transform_all_lakehouse.py
  dbt run --target lakehouse
  dbt test --target lakehouse

transform_all_snowflake.py
  dbt run --target prod
  dbt test --target prod
```

If only one transform DAG is active, keep the DAG ID as `transform_all` but make
the target configurable:

```text
DBT_TARGET=lakehouse
```

Recommendation:

```text
During migration, use explicit transform DAG names.
After Snowflake is retired, rename the lakehouse DAG back to transform_all if desired.
```

---

## Makefile Separation

Keep existing Snowflake targets for reference:

```text
make dbt-build
make dbt-run-prod
```

Add lakehouse-specific targets:

```text
make dbt-build-lakehouse
make dbt-run-lakehouse
make ducklake-shell
make lakehouse-smoke-test
```

Example intent:

```make
dbt-build-lakehouse:
    cd $(DBT_DIR) && uv run --group dbt dbt build --target lakehouse --profiles-dir .
```

Avoid changing `make dbt-build` until the lakehouse path is the default active
path.

---

## Environment Variables

Separate Snowflake and lakehouse settings clearly.

Snowflake:

```text
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_ROLE
SNOWFLAKE_DATABASE
SNOWFLAKE_WAREHOUSE
```

Shared:

```text
AWS_S3_BUCKET
AWS_REGION
DATASF_APP_TOKEN
```

Lakehouse:

```text
DBT_TARGET=lakehouse
DUCKDB_PATH=var/sf_urban_health.duckdb
DUCKLAKE_CATALOG=var/sf_urban_health.ducklake
DUCKLAKE_DATA_PATH=s3://{bucket}/lake/ducklake/
LAKEHOUSE_RAW_PREFIX=raw/
LAKEHOUSE_PARQUET_PREFIX=lake/parquet/
```

Do not overload Snowflake env vars to configure lakehouse behavior.

---

## Metadata State Separation

Snowflake path owns:

```text
METADATA.INGEST_WATERMARKS
Snowflake RAW tables
Snowflake observability marts
```

Lakehouse path owns:

```text
lake.metadata.ingest_runs
lake.metadata.file_manifest
lake.bronze.*
lake.silver.*
lake.gold.*
```

Do not let lakehouse extraction read Snowflake watermarks. The lakehouse
extraction source of truth is Airflow interval context plus lakehouse ingest-run
metadata for observability.

---

## Directory And Naming Conventions

Use names that keep the target obvious:

```text
snowflake_dag_factory.py
lakehouse_dag_factory.py
snowflake_load.py
lakehouse_load.py
_sources_snowflake.yml
_sources_lakehouse.yml
transform_all_snowflake.py
transform_all_lakehouse.py
```

Avoid vague names:

```text
load.py
warehouse.py
target.py
factory2.py
new_dag_factory.py
```

---

## Test Separation

Snowflake tests:

```text
tests/airflow/test_snowflake_dag_factory.py
tests/dbt/test_snowflake_parse.py
```

Lakehouse tests:

```text
tests/airflow/test_lakehouse_dag_factory.py
tests/scripts/test_lakehouse_load.py
tests/dbt/test_lakehouse_parse.py
```

Shared extraction tests:

```text
tests/scripts/test_soda_ingest.py
```

The important assertion:

```text
lakehouse DAG tests should not import Snowflake hooks
lakehouse ingestion tests should not require Snowflake env vars
Snowflake tests should not require DuckLake catalog setup
```

---

## Migration Sequence

1. Refactor `soda_ingest.py` into shared interval extraction.
2. Preserve current Snowflake DAG factory as `snowflake_dag_factory.py`.
3. Add `lakehouse_load.py` for DuckDB/Parquet or DuckLake writes.
4. Add `lakehouse_dag_factory.py`.
5. Point active ingest DAG files at the lakehouse factory.
6. Add DuckDB/lakehouse dbt target.
7. Add lakehouse source definitions and DuckDB-compatible staging models.
8. Add `transform_all_lakehouse.py` or make target-specific transform commands.
9. Run lakehouse path end to end.
10. Keep Snowflake path available but inactive.

Only after the lakehouse path is trusted:

```text
remove Snowflake DAGs from active Airflow deployment
mark Snowflake docs/code as reference/deprecated
eventually delete Snowflake code in a separate cleanup PR
```

---

## Review Checklist

Before merging lakehouse implementation:

- `soda_ingest.py` contains no Snowflake or DuckLake-specific state writes.
- Lakehouse DAGs do not import `SnowflakeHook`.
- Snowflake DAGs do not import lakehouse loaders.
- dbt can parse with Snowflake target.
- dbt can parse with lakehouse target.
- Active Airflow DAG IDs are unambiguous.
- Assets are emitted by only one active ingest path.
- No lakehouse test requires Snowflake credentials.
- No Snowflake test requires DuckLake setup.
- README/AGENTS instructions identify the active path.

---

## Long-Term Cleanup

Once the lakehouse path replaces Snowflake:

- Rename lakehouse DAGs back to canonical names if desired.
- Make `make dbt-build` target the lakehouse path by default.
- Move Snowflake docs into `artifacts/legacy_snowflake_reference.md`.
- Delete Snowflake SQL files and profile targets in a dedicated cleanup pass.
- Remove Snowflake dependencies only after CI no longer imports them.

Do not combine the cleanup with the first migration. Keeping deletion separate
makes rollback much easier.
