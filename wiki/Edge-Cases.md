# Edge Cases

A running list of behaviors that look surprising but are intentional —
and the things they protect against.

## Failed-load gap recovery

Each DAG resumes from `METADATA.INGEST_WATERMARKS` where the last
successful run left off, *not* from `data_interval_start`. A
catastrophic week-long outage is recovered by simply running the DAG —
the next ingest fetches everything since the last successful watermark.

**Tested by:** running the ingest with a manually rolled-back
watermark; it correctly fetches the gap.

## Backfill chunking

Full backfills (epoch = 2013-01-01 for permits) can return 500 k+
records in a single run. The extractor streams SODA records through a
temporary NDJSON file before uploading to S3, so it does not hold the
entire result set in memory.

The recommended backfill pattern is still yearly chunks so each API
call, S3 object, and Snowflake `COPY INTO` load stays easy to inspect:
pass `--since` and `--run-date` to the standalone script
(`airflow/include/scripts/permits.py --since 2018-01-01 --run-date 2019-01-01`).

## Watermark boundary behavior

Incremental scheduled runs use `WHERE data_loaded_at > :watermark` with a
`TIMESTAMP_NTZ` watermark. First runs use the dataset epoch at midnight
and include that boundary. This avoids re-fetching the prior boundary
day on every run.

The staging models still dedupe on source primary keys because upstream
civic datasets can revise records. The remaining edge case is a record
published later with the exact same `data_loaded_at` timestamp as the
stored high-water mark; that would require a compound watermark
(`data_loaded_at`, primary key) if it shows up in practice.

## DataSF "Unknown" neighborhood spellings

DataSF has at least four spellings for "we don't know which
neighborhood":

- `null`
- empty string `""`
- `"Unknown"`
- `"unknown"`
- `"None"` (rare, but observed)

The `normalize_neighborhood` macro collapses all of these to `'Unknown'`.
Any new neighborhood-grain mart **must** use the macro — otherwise
the mart-grain `unique_combination_of_columns` test would split the
"Unknown" bucket into multiple grain rows and break the dashboard.

## `STRIP_OUTER_ARRAY = FALSE`

The COPY templates explicitly set `STRIP_OUTER_ARRAY = FALSE` because
S3 holds NDJSON (one object per line), not a JSON array. Don't toggle
this even if you switch tooling — the alternative is a 16 MB-per-file
cap from Snowflake's `VARIANT` limit.

## Empty-results day

If a SODA query returns zero rows, the ingest DAG skips S3 upload,
Snowflake `COPY INTO`, and watermark update. It still emits the
dataset's ingest-complete Airflow asset so `transform_all` can rebuild
marts against unchanged source data after all datasets have checked in.

## Dash debug surface

`dashboard/app.py` gates `app.run(debug=...)` behind `DASH_DEBUG=1`.
Default is **off** — the interactive traceback / dev-tools panel is
opt-in only. If you start the local dashboard and hit a callback
exception, you'll see a clean error in the browser instead of a stack
trace and source viewer.

## Default Airflow API password is rejected against non-localhost

`airflow/include/scripts/airflow_rest_client.py` hard-fails at startup
if `AIRFLOW_API_PASSWORD == 'admin'` and the target host isn't in
`{localhost, 127.0.0.1, ::1, host.docker.internal}`. Catches the
"forgot to set the password" mistake before it leaves a real Airflow
instance auth'd as admin.
