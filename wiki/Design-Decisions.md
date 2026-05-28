# Design Decisions

The README has the same headings; this page expands each with the
trade-offs that didn't fit in a project README.

## S3 is the durable raw layer; Snowflake `RAW` is a loading target

Raw NDJSON lives in S3 permanently. Snowflake `RAW.*` is a `COPY INTO`
target, not a source-of-truth. To reprocess history we replay from S3
— never re-hit the DataSF API.

**Why:** DataSF rate-limits the SODA API and rewrites the dataset
(occasionally) when the city corrects records. S3 is cheap, immutable,
and decouples reprocessing from the upstream's availability. If we
needed to drop and rebuild the warehouse tomorrow, an `aws s3 sync`
plus `dbt build` is 100% sufficient.

**Trade-off:** two write paths to keep in sync. We accept that because
the COPY operation is idempotent on the S3 stage path — overlapping
loads can't create duplicates at the storage level.

## Watermark-driven incremental loads via `METADATA.INGEST_WATERMARKS`

Each ingest reads its last-successful watermark from a small
`METADATA.INGEST_WATERMARKS` table, fetches `where data_loaded_at >=
<wm>`, then writes a new watermark on success.

**Why over a checkpoint file in S3:** atomicity and queryability.
Snowflake gives a transactional `MERGE` against the watermark row in
the same task that just succeeded. A JSON checkpoint in S3 would
require careful file-level concurrency control. As a bonus, the
watermark table is queryable from BI ("what's our freshest day for
permits?") without scanning S3.

**Trade-off:** known overlap on the boundary day. The current `>=`
predicate plus a date-truncated watermark re-fetches the prior day's
last record on each run — see TODO.md H1 for the planned fix
(strict `>` on a `TIMESTAMP_NTZ` watermark).

## Newline-delimited JSON, never an outer array

`_write_s3` emits one JSON object per line. The COPY uses
`STRIP_OUTER_ARRAY = FALSE`. **Don't change this.**

**Why:** NDJSON streams. A 5 GB result set can be re-loaded line by
line without holding the whole array in memory. An outer-array variant
would require Snowflake to stage the entire object before processing,
which (a) doubles RAM cost on the worker and (b) caps the file size at
Snowflake's variant limit (16 MB compressed).

## dbt materialization policy

Staging and intermediate are **views** — always fresh, cheap, never
need re-running. Marts are **tables** — fast for BI, rebuilt fully on
each `dbt run`. (Snapshot/incremental candidates are tracked in TODO.md.)

**Why not incremental staging:** the source tables are small enough
(< 5 M rows each) that a full view scan is sub-second. Incremental
materialization adds complexity (state, late-arrival handling) without
a real speedup. We'll revisit when a single source crosses ~50 M rows.

## The residential lens lives in the mart, not staging

`stg_permits` is the source-of-truth for **all** permits — residential
and commercial. The `existing_units IS NOT NULL OR proposed_units IS
NOT NULL` filter that defines "housing" sits in `mart_housing_production`,
not upstream.

**Why:** the same staging model now feeds `mart_public_safety`'s
near-permit-site joins (planned, Phase 2) and any future commercial-
permits dashboard. Filtering at staging would make those impossible
without re-materializing.

## `normalize_neighborhood` macro

DataSF's neighborhood field has at least four spellings of "no value":
`null`, empty string, `"Unknown"`, and `"unknown"`. The macro collapses
them all to `'Unknown'` and `initcap`s real values.

**Why a macro and not just SQL:** every neighborhood-grain mart needs
this. A macro means we can change the policy in one place — e.g. if
DataSF starts returning `"None"` we add it to the macro and every
mart picks it up on the next run.

## Plotly Dash + live Snowflake, not a static export

The dashboard reads marts directly from Snowflake at request time.
This gives the analyst a live view but also means the deployment story
is non-trivial — see TODO.md "Live dashboard" for the candidate options.

**Why not Evidence.dev / static export:** the marts span ~13 years of
permit history with ~40 neighborhoods × ~11 districts grain. A static
export of every cross-tab would balloon the page-load size and lose
interactive filtering. Live Snowflake at request time is < 200 ms per
mart for the cardinalities we have.

## Airflow DAG factory pattern

The three ingest DAGs share `airflow/dags/dag_factory.py`. Adding a
fourth dataset is a single `DagConfig(...)` declaration plus the SODA
endpoint metadata.

**Why:** every dataset has the same shape (extract → load → update
watermark). A factory removes boilerplate and ensures every dataset
gets identical retry/observability defaults.

**Trade-off:** factory'd DAGs are slightly harder to debug than
hand-written ones because the Airflow UI shows the rendered DAG, not
the factory call. We've kept the factory itself short (< 100 lines) so
the indirection cost stays low.
