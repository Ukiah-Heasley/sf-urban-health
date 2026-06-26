# Edge Cases

## Empty extraction intervals

If DataSF returns no records, the writer uploads no empty object. The extract
returns null raw paths, zero row/byte counts, and a null maximum timestamp. The
ingest DAG still succeeds and emits its asset.

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
