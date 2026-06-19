# `soda_ingest.py` Refactor Notes

Purpose: running notes from the teaching walkthrough. These are not immediate
implementation instructions; they are a parking lot for improvements to revisit
after we finish understanding the current code.

---

## Current Design Read

The module already follows a useful hybrid style:

```text
Dataclasses / value objects:
  DatasetConfig
  RunResult
  WriteResult

Stateful classes:
  SodaClient
  NdjsonS3Writer

Pure-ish helper functions:
  _coerce_datetime
  _format_soda_timestamp
  _parse_soda_timestamp
  _s3_key

Orchestration:
  run()
  cli()
```

This is directionally strong for API ingestion:

- Use classes for stateful resources like HTTP sessions and S3 clients.
- Use functions for deterministic transformations like timestamp formatting,
  query construction, and key/path generation.
- Keep per-dataset metadata separate from reusable extraction mechanics.

---

## Improvement Candidates

### 1. Make Config Objects Immutable

`DatasetConfig` should probably become:

```python
@dataclass(frozen=True)
class DatasetConfig:
    ...
```

Why:

- Dataset metadata should not mutate during a run.
- Immutability makes config objects safer and easier to reason about.
- This matches how config is actually used.

### 2. Consider Frozen Dataclasses For Result Types

`RunResult` and `WriteResult` are currently `NamedTuple`s.

Potential alternative:

```python
@dataclass(frozen=True)
class WriteResult:
    s3_path: str | None
    records_written: int
    bytes_written: int
```

Open question:

- `RunResult` may change substantially if we move from simple timestamp
  high-watermark extraction to compound cursors or Airflow interval extraction.

### 3. Clarify Session Construction

Current:

```python
def _session() -> requests.Session:
    ...

class SodaClient:
    def __init__(...):
        self._session = session or _session()
```

Potential issue:

- `_session()` function and `self._session` attribute have similar names but
  different meanings.

Potential improvement:

```python
def build_soda_session(...) -> requests.Session:
    ...
```

or:

```python
class SodaClient:
    @staticmethod
    def build_default_session(...) -> requests.Session:
        ...
```

Design preference:

- Module-level function is fine if the builder is generic.
- Static method is fine if it is specifically the default `SodaClient` session
  factory.
- Avoid instance method unless it needs instance state.

### 4. Separate Environment Loading From Client Construction

Current `_session()` reads `DATASF_APP_TOKEN` directly from `os.environ`.

This is acceptable for a small script, but a cleaner design would pass settings
explicitly:

```python
@dataclass(frozen=True)
class SodaSettings:
    app_token: str | None
    timeout_seconds: int
    retries: int
```

Why:

- Easier to test.
- Clearer configuration boundary.
- Less hidden dependency on process environment.

### 5. Keep Formatting At The API Boundary

Current `where_clause()` calls `_format_soda_timestamp(since)`, which is the
right general location.

Principle:

```text
Parse early.
Normalize internally.
Format late.
```

Potential future naming:

- `since` may become `lower_bound`, `cursor`, or `window_start` depending on
  the extraction model.

### 6. Revisit Static Query Builders

Current:

```python
SodaClient.where_clause(...)
SodaClient.order_clause(...)
```

These are static methods because they do not use `self`.

Potential alternative:

```python
build_where_clause(config, cursor)
build_order_clause(config)
```

Why:

- These are pure transformations.
- Pulling them out can make the class focus only on HTTP behavior.

Counterpoint:

- Keeping them on `SodaClient` is defensible because they are SODA-specific
  query builders.

### 7. Remove Or De-Emphasize Compatibility Wrappers

Current module-level wrappers:

```python
count_records(...)
fetch_records(...)
_write_s3(...)
```

These duplicate class-based APIs:

```python
SodaClient().count_records(...)
SodaClient().fetch_records(...)
NdjsonS3Writer().write_records(...)
```

Potential improvement:

- Choose one public API style.
- Prefer explicit client/writer objects for testability and dependency
  injection.

### 8. Improve Cursor / Incremental Extraction Model

Current:

```text
WHERE data_loaded_at > last_max_data_loaded_at
ORDER BY data_loaded_at, tie_breaker
```

Strength:

- Simple and understandable.

Limitation:

- Ordering uses the tie-breaker, but the persisted watermark does not.
- Timestamp-only cursors can miss rows when multiple records share the same
  timestamp or records arrive late with an old timestamp.

Future options:

- Compound high-watermark cursor: `(data_loaded_at, order_field)`.
- Airflow interval extraction: `[data_interval_start, data_interval_end)`.
- Interval extraction plus downstream dedupe for late-arriving records.

### 9. Make `run()` Less Clever

Current `run()` uses a nested generator with `nonlocal` mutation:

```python
def observed_records():
    nonlocal max_watermark, records_seen
    ...
```

Strength:

- Streams records without holding the full response in memory.
- Counts records and tracks watermark during the stream.

Concern:

- Hidden mutation inside a generator is harder to teach and reason about.

Potential improvement:

- Introduce an explicit stats/cursor accumulator object.
- Or split record observation from writing in a clearer pipeline abstraction.

### 10. Prepare Writer Boundary For Local / DuckDB Mode

Current writer:

```python
NdjsonS3Writer
```

Future local options:

- `LocalNdjsonWriter`
- `LocalParquetWriter`
- `DuckDbRawWriter`

Design direction:

- Keep extraction logic independent from destination.
- Let `run()` accept a writer interface.

### 10a. Move S3 Client Construction Out Of The Writer Constructor

Current:

```python
class NdjsonS3Writer:
    def __init__(self, bucket: str | None = None, s3_client=None) -> None:
        self.bucket = bucket or os.environ.get("AWS_S3_BUCKET")
        if not self.bucket:
            raise RuntimeError("AWS_S3_BUCKET must be set")
        if s3_client is None:
            import boto3

            s3_client = boto3.client("s3")
        self._s3 = s3_client
```

Concern:

- `boto3.client("s3")` is a side effect inside object construction.
- It may read AWS env vars, profiles, region config, credentials, etc.
- Tests that accidentally instantiate `NdjsonS3Writer()` without a fake client
  can touch real AWS configuration.
- The constructor mixes two responsibilities:
  - configure/build dependencies
  - represent a writer that uses those dependencies

Cleaner dependency-injection-first shape:

```python
class NdjsonS3Writer:
    def __init__(self, bucket: str, s3_client) -> None:
        self.bucket = bucket
        self._s3 = s3_client
```

Then put side effects in an explicit factory:

```python
def build_s3_writer_from_env() -> NdjsonS3Writer:
    import boto3

    bucket = os.environ["AWS_S3_BUCKET"]
    return NdjsonS3Writer(
        bucket=bucket,
        s3_client=boto3.client("s3"),
    )
```

or:

```python
class NdjsonS3Writer:
    @classmethod
    def from_env(cls) -> "NdjsonS3Writer":
        import boto3

        return cls(
            bucket=os.environ["AWS_S3_BUCKET"],
            s3_client=boto3.client("s3"),
        )
```

Decision to make later:

- Use a module-level factory if we want dependency construction outside the
  domain class.
- Use `from_env()` if we want a convenient named constructor while still making
  side effects explicit at the call site.

### 11. Revisit Raw Object Path Convention

Current:

```python
def _s3_key(config: DatasetConfig, run_date: date) -> str:
    return f"raw/{config.name}/{run_date:%Y/%m/%d}/{config.name}.json"
```

Strengths:

- Centralizes raw path construction.
- Keeps bucket separate from object key.
- Gives each dataset a clear raw namespace.
- Uses date-based path components that are easy to browse and replay.

Questions / improvements:

- The file extension is `.json`, but the content is newline-delimited JSON.
  Prefer `.ndjson` or a generic name like `records.ndjson`.
- `run_date` means the extraction run's logical/date label, not necessarily
  the source record date.
- If we move to Airflow interval extraction, consider encoding the interval in
  the path or metadata:

```text
raw/{dataset}/window_start={...}/window_end={...}/records.ndjson
```

or:

```text
raw/{dataset}/extract_date=YYYY-MM-DD/records.ndjson
```

Design note:

- Raw object paths are part of the data lake contract, not just string
  formatting. Downstream replay, discovery, and backfill behavior depend on
  this convention.

### 12. Simplify / Clarify `run()`

Current role:

```text
run()
  creates default client/writer if not provided
  normalizes the lower watermark
  starts timing
  wraps the API record stream with observation logic
  writes records through the writer
  validates observed count == written count
  returns RunResult
```

Current core:

```python
def observed_records() -> Iterator[dict]:
    nonlocal max_watermark, records_seen
    for record in client.fetch_records(config, since_dt, include_since=include_since):
        watermark = _parse_soda_timestamp(record.get(config.date_field))
        if max_watermark is None or watermark > max_watermark:
            max_watermark = watermark
        records_seen += 1
        yield record

write_result = writer.write_records(config, run_date, observed_records())
```

How the generator chain works:

```text
writer.write_records(...)
  loops over observed_records()
    observed_records() loops over client.fetch_records(...)
      client.fetch_records(...) fetches one API page at a time
      yields one record at a time from that page
    observed_records() updates records_seen and max_watermark
    observed_records() yields the same record onward
  writer writes each yielded record to the same temp NDJSON file
```

Memory behavior:

```text
one API page in memory at a time
one growing temp file on disk
not all source records in memory
one final S3 upload after the temp file is complete
```

Strengths:

- `run()` is independent from Airflow and can be called by tests or CLI.
- Supports dependency injection through optional `client` and `writer`.
- Streams records instead of materializing the whole extract.
- Tracks record count and candidate max watermark during streaming.
- Handles empty extracts explicitly.
- Uses `time.monotonic()` for elapsed duration.
- Returns structured metadata for the DAG layer.

Concerns:

- `client = client or SodaClient()` and `writer = writer or NdjsonS3Writer()`
  hide side effects inside `run()`.
- Nested generator plus `nonlocal` mutation is clever but harder to teach and
  reason about.
- `fetch_duration_seconds` includes writing to the temp file, not just HTTP
  fetch time.
- `RunResult` assumes simple timestamp high-watermark extraction.
- There is no upper bound, so the extract is not a fully reproducible interval.
- The returned `max_watermark` is only a candidate watermark; Airflow commits it
  later after loading.
- Writer type hint is concrete (`NdjsonS3Writer`) rather than a writer
  interface/protocol.

Potential future shape:

```text
run_extract(config, extraction_boundary, client, writer) -> ExtractResult
```

where:

```text
extraction_boundary =
  high-watermark cursor
  or compound cursor
  or Airflow data interval
```

and:

```text
client and writer are required dependencies
side-effectful factories live outside run_extract()
```

Possible teaching-friendly refactor:

```python
@dataclass
class RecordStats:
    records_seen: int = 0
    max_watermark: datetime | None = None

    def observe(self, record: dict, watermark_field: str) -> dict:
        watermark = parse_soda_timestamp(record.get(watermark_field))
        self.records_seen += 1
        if self.max_watermark is None or watermark > self.max_watermark:
            self.max_watermark = watermark
        return record
```

This would make the mutable state explicit instead of hidden in `nonlocal`.

Preferred direction after discussion:

- Keep the public client abstraction record-oriented:

```python
client.fetch_records(...) -> Iterator[dict]
```

- Do not expose pages unless the project later needs page-level metrics,
  checkpointing, or per-page output files.
- Keep the extractor shaped like a record-stream pipeline:

```text
source records
  -> capture cursor / extraction summary
  -> write raw records
```

Candidate code shape:

```python
@dataclass
class ExtractAccumulator:
    records_fetched: int = 0
    candidate_watermark: datetime | None = None

    def capture(self, record: dict, watermark_field: str) -> None:
        watermark = _parse_soda_timestamp(record.get(watermark_field))
        self.records_fetched += 1
        if self.candidate_watermark is None or watermark > self.candidate_watermark:
            self.candidate_watermark = watermark


def capture_watermark(
    records: Iterable[dict],
    accumulator: ExtractAccumulator,
    watermark_field: str,
) -> Iterator[dict]:
    for record in records:
        accumulator.capture(record, watermark_field)
        yield record
```

Then `run()` / future `extract_to_raw()` reads as an explicit pipeline:

```python
accumulator = ExtractAccumulator()

source_records = client.fetch_records(
    config,
    lower_bound_dt,
    include_since=include_lower_bound,
)

raw_records = capture_watermark(
    source_records,
    accumulator,
    config.date_field,
)

write_result = writer.write_records(config, run_date, raw_records)
```

Why this is preferable to the current nested generator:

- Keeps streaming behavior.
- Keeps API pagination hidden behind `SodaClient`.
- Makes the pipeline stages visible and named.
- Replaces `nonlocal` mutation with explicit accumulator state.
- Avoids materializing all records into memory.
- Keeps writer as the sink and client as the source.

Naming still open:

- `ExtractAccumulator`
- `ExtractSummaryBuilder`
- `CursorAccumulator`
- `WatermarkCapture`
- `capture_watermark`
- `capture_cursor`

Current preference:

- Use watermark language if we stay with timestamp high-watermark extraction.
- Use cursor language if we move toward compound cursors.

---

## Working Design Principle

Preferred future shape:

```text
Immutable configs:
  DatasetConfig
  SodaSettings
  Cursor or ExtractWindow

Stateful boundary classes:
  SodaClient
  RawWriter implementations
  StateStore implementations

Pure functions:
  build_query_params
  format_soda_timestamp
  parse_soda_timestamp
  build_raw_path

Thin orchestration:
  run_extract(config, cursor/window, client, writer, state_store)
```
