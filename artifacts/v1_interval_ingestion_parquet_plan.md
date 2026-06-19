# V1 Interval Ingestion And Parquet Medallion Plan

Purpose: adapt the original interval-ingestion rewrite plan to the v1 lakehouse
direction:

```text
DataSF SODA API
  -> S3 raw NDJSON, immutable replay layer
  -> S3 bronze Parquet
  -> dbt-duckdb transformations
  -> S3 silver/gold Parquet
  -> dashboard reads gold Parquet through DuckDB
```

This is still a v1 design. It intentionally avoids Iceberg table metadata for
now, but keeps names, schemas, metadata columns, and partition choices friendly
to a later Iceberg promotion.

---

## Current Behavior To Replace

Current ingest DAG flow:

```text
read Snowflake watermark
extract records after watermark
write raw NDJSON to S3
COPY S3 object into Snowflake RAW table
update Snowflake watermark
emit ingest asset
```

V1 target flow:

```text
build ExtractWindow from Airflow data interval
extract interval to S3 raw NDJSON
if records_fetched > 0:
  load raw NDJSON object into S3 bronze Parquet with DuckDB
record ingest-run metadata
emit ingest asset
```

Snowflake-specific code can remain as a reference path during this pass, but it
should no longer be the active local ingestion path.

---

## `soda_ingest.py` Rewrite

### Public Concepts

Introduce clearer value objects:

```python
@dataclass(frozen=True)
class ExtractWindow:
    start: datetime
    end: datetime
    lookback: timedelta = timedelta(0)

    @property
    def effective_start(self) -> datetime:
        return self.start - self.lookback


@dataclass
class ExtractAccumulator:
    records_fetched: int = 0
    candidate_watermark: datetime | None = None


@dataclass(frozen=True)
class ExtractResult:
    raw_path: str | None
    records_fetched: int
    candidate_watermark: datetime | None
    bytes_written: int
    duration_seconds: float
    window_start: datetime
    window_end: datetime
    effective_start: datetime
```

Important naming:

- `candidate_watermark` is observed metadata, not control state.
- Airflow's interval is the control state.
- `raw_path` replaces `s3_path` when the result specifically points at raw
  NDJSON.
- `extract_to_raw(...)` replaces the vague `run(...)`.

### Query Semantics

The SODA query should become a half-open interval:

```sql
`data_loaded_at` >= '<effective_start>'
and `data_loaded_at` < '<window_end>'
```

Use `ExtractWindow.effective_start` for the lower bound. Default lookback is
zero. Keep per-dataset lookback configurable, but do not enable it for v1.

Ordering should stay stable:

```text
order by date_field, order_field
```

If `order_field == date_field`, only order by the timestamp field once.

### Streaming Pipeline

Keep record-level streaming:

```text
SodaClient.fetch_records(config, window)
  -> ExtractAccumulator observes record count and candidate max data_loaded_at
  -> S3NdjsonWriter streams records to temp NDJSON
  -> upload once if at least one record was written
  -> ExtractResult returned
```

Do not materialize all records in memory. The accumulator should update as each
record passes through the generator.

### Side-Effect Boundaries

Use explicit dependency injection for testable core logic:

```python
extract_to_raw(
    config,
    window,
    run_date,
    client=client,
    writer=writer,
)
```

Side-effectful builders stay at the Airflow/CLI boundary:

```python
build_soda_session()
S3NdjsonWriter.from_env()
```

The core extraction path should not silently read credentials except through
those builders.

### S3 Raw Key Shape

The current raw key shape writes one file per dataset per run date:

```text
raw/{dataset}/YYYY/MM/DD/{dataset}.json
```

For interval ingestion, prefer a key that carries the extraction interval and
Airflow run identity:

```text
raw/{dataset}/window_start={YYYYMMDDTHHMMSS}/window_end={YYYYMMDDTHHMMSS}/run_id={safe_run_id}/{dataset}.ndjson
```

Reasoning:

- Backfills and scheduled runs are distinguishable.
- Manual reruns do not overwrite scheduled raw files.
- Task retries for the same Airflow run can remain idempotent if they reuse the
  same key.
- The `.ndjson` suffix matches the actual file format.

### Empty Extracts

For empty extracts:

- Do not upload an empty raw object.
- Return `raw_path=None`.
- Return `records_fetched=0`, `bytes_written=0`, and
  `candidate_watermark=None`.
- Let the DAG record a successful no-record metadata row.

---

## V1 Parquet Lake Additions

### New DuckDB Lake Loader

Add a small module outside `soda_ingest.py`, for example:

```text
airflow/include/scripts/parquet_lake.py
```

Responsibilities:

```text
load_raw_ndjson_to_bronze_parquet(...)
record_ingest_run(...)
build_duckdb_connection(...)
```

Keep this separate from `soda_ingest.py` so API extraction and lake materializing
do not blur together.

### Bronze Parquet Contract

Bronze is append-only and interval-oriented. It should preserve one row per
DataSF record occurrence, not one row per business entity.

Recommended bronze columns:

```text
source fields, typed where reasonable
_raw_payload             JSON or TEXT, optional but useful for replay/debugging
_ingest_run_id           TEXT
_raw_s3_path             TEXT
_window_start            TIMESTAMP
_window_end              TIMESTAMP
_effective_start         TIMESTAMP
_loaded_at               TIMESTAMP, parsed from data_loaded_at
_extracted_at            TIMESTAMP
_record_hash             TEXT, optional but useful for dedupe tests
```

For promotion friendliness, prefer typed source columns over a single
`payload JSON` column. Keeping `_raw_payload` as a companion column is fine.

### Bronze Path Shape

Use a separate Parquet root so later Iceberg tables can be built beside it:

```text
s3://{bucket}/lake/parquet/bronze/{dataset}/loaded_date=YYYY-MM-DD/run_id={safe_run_id}/part-000.parquet
```

`loaded_date` should be derived from the source extraction timestamp
`data_loaded_at`, not from Airflow run date.

### Ingest Metadata

V1 can store metadata in a local DuckDB file, with an option to export it as
Parquet later:

```text
metadata.ingest_runs
  dataset_name
  window_start
  window_end
  effective_start
  status
  records_fetched
  candidate_max_loaded_at
  raw_s3_path
  bronze_s3_path
  airflow_dag_id
  airflow_run_id
  started_at
  finished_at
```

Record a successful row for both branches:

- `records_fetched > 0`: raw extract plus bronze Parquet load.
- `records_fetched = 0`: no raw object and no bronze write, but still a
  successful interval.

Optional but useful:

```text
metadata.file_manifest
  dataset_name
  layer
  table_name
  s3_path
  row_count
  bytes_written
  source_raw_s3_path
  airflow_run_id
  created_at
```

---

## `dag_factory.py` Rewrite

### New Flow

Replace the Snowflake watermark flow with:

```text
extract_{dataset}_to_raw
  -> choose_load_path
  -> load_raw_to_bronze_parquet
  -> record_ingest_run
  -> ingest_complete

choose_load_path
  -> no_new_records
  -> record_ingest_run
  -> ingest_complete
```

Use task variable names that match task IDs:

```python
extract_permits_to_raw = PythonOperator(task_id="extract_permits_to_raw", ...)
choose_load_path = BranchPythonOperator(task_id="choose_load_path", ...)
load_raw_to_bronze_parquet = PythonOperator(task_id="load_raw_to_bronze_parquet", ...)
no_new_records = EmptyOperator(task_id="no_new_records")
record_ingest_run = PythonOperator(task_id="record_ingest_run", ...)
ingest_complete = EmptyOperator(task_id="ingest_complete", ...)
```

### Extract Task

The extract task should build the window from Airflow context:

```python
window = ExtractWindow(
    start=context["data_interval_start"],
    end=context["data_interval_end"],
    lookback=cfg.dataset.lookback,
)
```

Normalize Airflow's timezone-aware datetimes at the API boundary. Internally,
keep one convention and document it. The current code normalizes timestamps to
UTC-naive before sending them to DataSF; keeping that convention is acceptable
if it is explicit.

Push these XComs:

```text
raw_path
records_fetched
candidate_watermark
bytes_written
duration_seconds
window_start
window_end
effective_start
```

### Branch Task

Branch on actual records fetched:

```python
return (
    "load_raw_to_bronze_parquet"
    if int(records_fetched or 0) > 0
    else "no_new_records"
)
```

### Bronze Load Task

The bronze load task should:

- Read only the raw NDJSON object produced by this extract.
- Write one or more Parquet files under the bronze path.
- Add the standard metadata columns.
- Return bronze path, row count, and bytes if available.
- Fail if its output row count does not match `records_fetched`.

### Metadata Task

`record_ingest_run` should run after both branches with a trigger rule that
requires the chosen branch to succeed:

```text
NONE_FAILED_MIN_ONE_SUCCESS
```

This task records success for no-record intervals too. The ingest asset should
fire only after metadata has been recorded.

### Asset Behavior

Keep the existing asset model:

```text
ingest_permits
ingest_evictions
ingest_incidents
  -> transform_all
```

The no-record path should still emit the ingest-complete asset, because the
interval has been successfully evaluated.

---

## dbt-DuckDB And Parquet Changes

### Profile

Add a DuckDB target alongside Snowflake:

```yaml
duckdb:
  type: duckdb
  path: "{{ env_var('DUCKDB_PATH', 'var/sf_urban_health.duckdb') }}"
  schema: main
  threads: 4
  extensions:
    - httpfs
    - parquet
  secrets:
    - type: s3
      provider: credential_chain
```

Use environment-specific S3 settings if the credential chain is not enough.

### Sources

For the DuckDB target, raw dbt sources should point at bronze Parquet, not
Snowflake RAW tables:

```yaml
sources:
  - name: bronze
    meta:
      external_location: "read_parquet('s3://bucket/lake/parquet/bronze/{name}/**/*.parquet', hive_partitioning=true)"
    tables:
      - name: permits
      - name: incidents
      - name: evictions
```

Use variables for bucket/prefix rather than hard-coded paths.

### Model SQL

The current staging models use Snowflake stage/variant syntax:

```sql
select $1 as payload, _loaded_at
payload:permit_number::string
```

For v1, prefer typed bronze columns so staging models can select direct columns:

```sql
select
    permit_number,
    permit_type,
    filed_at,
    _loaded_at
from {{ source('bronze', 'permits') }}
```

If a model must read `_raw_payload`, use DuckDB JSON extraction functions in
that model only. Do not make the whole silver/gold stack depend on raw JSON
syntax.

### Materialization Policy

dbt-duckdb external Parquet models are the natural fit for this project, but
they are full-refresh oriented. For v1, that is acceptable.

Recommended v1 policy:

```text
bronze: written by Airflow DuckDB loader as Parquet
silver/staging: external Parquet if you want a physical medallion layer
intermediate: views during development, external Parquet for portfolio demo
gold/marts: external Parquet
metadata: DuckDB tables for v1, optional Parquet export
```

If build time stays small, full-refresh silver/gold Parquet is simpler than
trying to hand-roll incremental external Parquet.

---

## Test Plan

Unit tests:

- `ExtractWindow.effective_start` with zero lookback.
- `ExtractWindow.effective_start` with configured lookback.
- SODA query uses `>= effective_start` and `< window_end`.
- `SodaClient.fetch_records` keeps stable ordering and paginates.
- Accumulator captures `records_fetched` and `candidate_watermark` while
  streaming.
- Writer emits `.ndjson`, uses `application/x-ndjson`, counts records/bytes,
  and skips upload for empty extracts.

DAG tests:

- `_extract` builds the window from Airflow interval context.
- No Snowflake watermark hook is used in the active path.
- Branch task returns `load_raw_to_bronze_parquet` only when
  `records_fetched > 0`.
- No-record branch still reaches metadata and ingest asset emission.
- Task variable names match task IDs.

DuckDB/Parquet tests:

- Raw NDJSON loads into bronze Parquet with expected row count.
- Bronze rows include metadata columns.
- Empty extracts do not write bronze files.
- Duplicate reruns are detectable by `_ingest_run_id` and metadata rows.
- dbt can parse against the DuckDB profile.
- dbt can build silver/gold Parquet from bronze Parquet.

Acceptance criteria:

- A scheduled interval can complete with records and produce raw NDJSON plus
  bronze Parquet.
- A scheduled interval can complete with zero records and still emit the ingest
  asset.
- dbt-duckdb can build marts from S3 Parquet without Snowflake.
- The dashboard can read gold Parquet through DuckDB.
