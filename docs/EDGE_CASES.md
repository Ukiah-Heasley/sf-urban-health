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

Promotion snapshots its selected backlog in S3 before it begins draining. On a
task retry, each snapshot interval is resolved again from its current ingest
events and deterministic bronze manifest receipts. Intervals with complete
receipts are skipped, newly failed intervals are left blocked, and no interval
outside the original snapshot is introduced. Airflow task retries provide the
automatic recovery path; a terminally failed promotion DAG run still needs an
operator retry or another promotion wake-up.

Promotion plan snapshots remain under `lake/metadata/promotion_plans/` after a
run finishes so clearing or retrying that DAG run retains fixed-snapshot
semantics. They are not compacted with metadata events. Long-term retention is
an object-storage lifecycle concern; the application does not delete them.

## Genesis retries and boundaries

Genesis uses deterministic per-dataset Airflow run IDs. Existing queued,
running, and successful runs are reused. A failed or canceled run is rejected by
default because replaying a wide full extract can be expensive. Supplying
`--retry-failed` clears every task in that one dataset run and requeues it; this
also reruns successful failure-metadata tasks so the current event and emitted
assets reflect the new attempt.

Automatic genesis end selection stops at the first daily interval represented
by every dataset. If that interval contains a failed current event, genesis
fails closed rather than extending across it. An operator can repair the daily
interval or supply an explicit later end. The explicit later end may duplicate
source rows across genesis and daily bronze files; silver models deduplicate by
dataset natural key before gold aggregation.

## Daily interval coverage

`data_trust` generates expected UTC daily intervals only from each dataset's
first observed daily metadata event through a 30-hour grace cutoff. A wide
genesis full-load interval is intentionally not a daily baseline. Missing
permits or incidents intervals make the actionable gap dbt test fail; missing
eviction intervals remain warnings because the source restamps a later full
snapshot. Failed current events count as observed for this check so that failed
extract handling remains separate from absent-schedule detection.

The actionable interval-gap singular test is selected by both the full
lakehouse build and the failure-metadata observability build. A standing permits
or incidents gap can therefore keep both DAGs red even though the observability
models materialize before the test fails.

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
