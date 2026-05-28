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
records in a single run. Today the implementation accumulates the full
list in memory before writing S3 — this works for the steady-state
daily load (a few hundred records) but is fragile on a cold backfill.

A streaming refactor is tracked in TODO.md (item H2). Until then, the
recommended backfill pattern is yearly chunks: pass `--since` and
`--run-date` to the standalone script
(`airflow/include/scripts/permits.py --since 2018-01-01 --run-date 2019-01-01`).

## Watermark boundary overlap

The current `WHERE data_loaded_at >= :watermark` predicate plus a
date-truncated watermark re-fetches the prior day's last record on
every run. Today this is harmless because:

1. dbt's `unique(permit_number)` test would *eventually* catch a
   genuine duplicate (currently masked by the project-wide
   `+severity: warn`, see TODO.md D1), and
2. Snowflake `COPY INTO ... ON_ERROR = ABORT_STATEMENT` rejects the
   duplicate before it reaches the staging view.

The proper fix (strict `>` on a `TIMESTAMP_NTZ` watermark, or a
staging-layer dedupe via `qualify`) is on the deferred Airflow
checklist (item H1).

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

## Empty-results day → stale watermark

If a SODA query returns zero rows for the day, today the
`update_watermark` task crashes (B2 + B4 in TODO.md). Until that's
fixed, an empty day will show up as a failed Airflow task — which is
the right "loud failure" behavior, but inconvenient. Workaround: clear
the failure and let the retry pick up the next day's records.

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
