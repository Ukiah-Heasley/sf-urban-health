# Edge Cases

## Empty extraction intervals

If DataSF returns no records, the writer uploads no empty object. The extract
returns null raw paths, zero row/byte counts, and a null maximum timestamp. The
ingest DAG still succeeds and emits its asset. A no-op
`promote_raw_to_bronze` run still compacts metadata so all-empty intervals reach
the compacted observability layer.

## Failed extraction intervals

If an extract task fails, the metadata task still runs after the failed task,
recomputes the interval bounds, and writes current plus attempt ingest metadata
with `status="failed"`, null raw paths, and a non-null UTC `completed_at`. The
ingest-complete asset is not emitted, so failed intervals do not trigger bronze
promotion.

The failed current event remains the planner-visible state for that dataset and
interval. Rerun the failed interval with the same Airflow interval or matching
manual `window_start`/`window_end`; a successful rerun overwrites the current
failed event with `success` or `empty` and unblocks promotion planning.

Failed ingest metadata triggers `handle_ingest_failure_metadata`, which compacts
metadata and rebuilds `pipeline_health` and `data_trust` so the operational gold
tables can show the failure.

## Metadata compaction concurrency

`compact_lakehouse_metadata` deletes and rebuilds the compacted metadata Parquet
prefixes. It can be reached from `promote_raw_to_bronze` and
`handle_ingest_failure_metadata`; both DAGs have `max_active_runs=1`, but they
do not share an Airflow pool. At the current daily cadence this race is accepted.
Use a shared one-slot Airflow pool if compaction becomes frequent enough that
interleaved delete/write operations are realistic.

## Missing configured timestamps

`ExtractAccumulator` currently parses the configured timestamp before counting
or yielding a record. A missing field or JSON `null` raises `ValueError` and
fails the extract. A malformed non-null value also fails during datetime
normalization.

## Interval retries

Raw keys are deterministic from dataset and interval bounds. A retry of the same
interval uploads to the same key rather than producing a duplicate raw object.

## Half-open windows

Queries use `timestamp >= effective_start AND timestamp < data_interval_end`.
This prevents adjacent scheduled intervals from both selecting the upper-bound
timestamp. An optional non-negative lookback widens only the lower bound.

## Pagination order

The configured timestamp is the primary sort field. A dataset-specific source
key is the tie-breaker unless it is the same field. Pagination still uses
limit/offset, so deterministic ordering is required for stable page boundaries.

## Count reconciliation

After writing, `extract_to_raw` compares records observed by the accumulator
with records reported by the writer. A mismatch raises `RuntimeError` even if an
S3 object was uploaded.

## Naive datetimes

Naive Python datetimes are interpreted as UTC. Aware datetimes are converted to
UTC before query formatting or comparison.

## Dashboard startup without a warehouse

The dashboard cache catches mart-loading errors and substitutes empty Polars
frames so modules remain importable without credentials. Pages render their
empty-state behavior rather than preventing application import.
