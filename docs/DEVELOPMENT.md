# Development

The primary data path uses AWS S3 for raw and Parquet storage and the AWS Glue
catalog for Iceberg. MinIO with a Hadoop catalog is an optional local fixture
harness for tests and debugging; it is not a deployed layer.

## Prerequisites

- Python `>=3.11,<3.12` and [uv](https://docs.astral.sh/uv/)
- Docker Desktop and Astro CLI for the local Airflow container stack
- Docker Desktop for Spark Thrift, whether using the Glue proof or local harness
- AWS credentials, an S3 bucket, and Glue access for the AWS data path
- Node.js 20 only when building the Evidence site

## First check

Install dependencies and run the default test suite:

```bash
uv sync --all-groups
make test
```

## Environments

Private environment files are gitignored. Copy the template for the workflow
you are using and add credentials outside version control.

| Workflow | Airflow environment | Lakehouse environment | Start command |
| --- | --- | --- | --- |
| AWS S3 ingest + Glue catalog | `airflow/.env.aws` | `lakehouse/.env.aws` | `make airflow-up-aws` or `make spark-up-aws` |
| Optional local fixture harness | `airflow/.env.local` | `lakehouse/.env.local` | `make airflow-up-local` or `make spark-up` |

`make airflow-up` is an alias for the local harness, so use
`make airflow-up-aws` whenever the Airflow containers should use AWS S3.
`make spark-up-aws` starts Spark Thrift with the Glue catalog and no MinIO.

Asset-triggered promotion uses the active Airflow environment as its default
planner configuration: `LAKEHOUSE_PLAN_MODE`, optional
`LAKEHOUSE_PLAN_MAX_INTERVALS`, and optional matching
`LAKEHOUSE_PLAN_START` / `LAKEHOUSE_PLAN_END`. A manual promotion DAG run can
instead supply `plan_mode`, `plan_max_intervals`, and matching `plan_start` /
`plan_end` in its JSON configuration. `LAKEHOUSE_PLAN_LIMIT` is retired; its
presence makes promotion fail so a stale one-interval setting cannot silently
throttle a drain. The root Make targets also reject an active or selected
Airflow environment file containing the retired variable before starting or
using Airflow.

The host-side lakehouse commands authenticate to Airflow with
`AIRFLOW_API_BASE_URL`, `AIRFLOW_API_USER`, and `AIRFLOW_API_PASSWORD`. Local
Astro defaults are `http://localhost:8080` and `admin` / `admin`; the command
refuses those default credentials against a non-local host.

The local Airflow environment uses `host.docker.internal` for MinIO because it
is consumed inside Astro containers. The host-side Make targets instead honor
`LAKEHOUSE_CLI_AWS_ENDPOINT_URL`; keep its local template value of
`http://localhost:9000` so `lakehouse-status` and `lakehouse-genesis` can reach
the same host-published MinIO service. Leave it unset for normal AWS S3 use.

## AWS S3 + Glue workflow

The S3 ingestion DAGs run in the Astro container stack with AWS configuration.
The supported Glue transform proof is a host CLI workflow and requires bronze
and compacted metadata Parquet to already exist in S3. Airflow does not yet
orchestrate that Glue transform path.

```bash
cp airflow/.env.aws.example airflow/.env.aws
cp lakehouse/.env.aws.example lakehouse/.env.aws
make airflow-up-aws
```

In a separate terminal, start Spark Thrift with Glue and build the lakehouse:

```bash
make spark-up-aws
make dbt-lakehouse-debug LAKEHOUSE_ENV_FILE=lakehouse/.env.aws
make dbt-lakehouse-gold LAKEHOUSE_ENV_FILE=lakehouse/.env.aws
make export-evidence-snapshots LAKEHOUSE_ENV_FILE=lakehouse/.env.aws
```

`lakehouse/.env.aws` sets `LAKEHOUSE_WAREHOUSE_URI` with `s3://` for Iceberg
warehouse data and `LAKEHOUSE_BRONZE_BASE_URI` /
`LAKEHOUSE_METADATA_BASE_URI` with `s3a://` for Spark reads. The Airflow AWS
environment configures the S3 bucket for raw capture and promotion metadata.

### Manual full or backfill ingest

Scheduled DAGs use their Airflow data interval. To load a bounded history,
trigger an ingest DAG with JSON configuration:

```json
{
  "load_mode": "full",
  "window_start": "2018-01-01T00:00:00Z",
  "window_end": "2026-06-29T00:00:00Z",
  "lookback_hours": 0
}
```

`load_mode` accepts `"full"` or `"backfill"`; both require the two window
bounds. `lookback_hours` is optional and affects only the lower query boundary.
Trigger `promote_raw_to_bronze` with matching `plan_start` and `plan_end` to
drain that same window. The `lakehouse-promote` command supplies that promotion
configuration without changing the shared Airflow environment.

### Genesis full load and promotion control

The host-side CLI wraps the Airflow REST API for common operational actions:

```bash
make lakehouse-status
make lakehouse-promote LAKEHOUSE_CLI_ARGS='--mode pending --max-intervals 50'
make lakehouse-genesis
```

`make lakehouse-status` reads current S3 metadata directly, prints a state-count
summary, and reports pending, incomplete, and failed interval state. Pass
`LAKEHOUSE_CLI_ARGS='--all'` to include receipt-complete intervals.
`make lakehouse-promote` triggers one promotion DAG run and waits for it unless
`--no-wait` is supplied.
An omitted max-intervals value drains the whole discovered backlog. A positive
cap intentionally bounds only that one run; trigger promotion again (or wait
for a later ingest asset) to drain the remaining intervals.

`make lakehouse-genesis` triggers a full load with the same bounds for permits,
evictions, and incidents. Its default lower bound is `1997-01-01T00:00:00Z`.
With the default `--window-end auto`, it selects the first 24-hour interval
represented by all three datasets, which keeps the common genesis interval
separate from promotable daily history even when the DAG start dates differ. If
that earliest shared interval has a failed current event, the command stops
instead of moving the boundary forward across the failed source window. Repair
the interval first or supply an explicit `--window-end`; a later explicit bound
can overlap daily bronze and relies on silver natural-key deduplication.
After the three ingest runs succeed, the command explicitly triggers bounded
promotion for those same bounds. Its per-dataset Airflow run IDs are
deterministic from the selected window, so rerunning the same genesis command
reuses an existing queued, running, or successful ingest run instead of
submitting a duplicate full extract. A reused failed or canceled run stops the
command unless `--retry-failed` is supplied. That flag clears every task in the
failed dataset run and requeues the same deterministic run so extract metadata
and asset-emitting tasks rerun consistently.
Use an explicit end when needed:

```bash
make lakehouse-genesis \
  LAKEHOUSE_CLI_ARGS='--window-end 2026-05-01T06:00:00Z'
```

Retry a deterministic genesis run that previously failed:

```bash
make lakehouse-genesis LAKEHOUSE_CLI_ARGS='--retry-failed'
```

`--no-wait` submits only the three ingest runs; their emitted assets wake
promotion when the interval becomes complete. It still rejects a reused failed
run unless `--retry-failed` is also supplied.

## Common tasks

| Scope | Goal | Command |
| --- | --- | --- |
| General | Run Python tests | `make test` |
| General | Run all configured quality checks | `make pre-commit` |
| General | Check public documentation | `make docs-check` |
| AWS | Start Airflow with AWS S3 configuration | `make airflow-up-aws` |
| AWS | Start Spark Thrift with the Glue catalog | `make spark-up-aws` |
| AWS | Verify the dbt connection | `make dbt-lakehouse-debug LAKEHOUSE_ENV_FILE=lakehouse/.env.aws` |
| AWS | Build and test lakehouse models | `make dbt-lakehouse-gold LAKEHOUSE_ENV_FILE=lakehouse/.env.aws` |
| AWS | Export Evidence snapshots | `make export-evidence-snapshots LAKEHOUSE_ENV_FILE=lakehouse/.env.aws` |
| Airflow | Inspect promotion backlog and blockers | `make lakehouse-status` |
| Airflow | Trigger a configurable promotion drain | `make lakehouse-promote LAKEHOUSE_CLI_ARGS='--mode pending'` |
| Airflow | Trigger the matching three-DAG genesis full load | `make lakehouse-genesis` |
| Local only | Start MinIO + Spark Thrift | `make spark-up` |
| Local only | Reset and seed fixtures (destructive) | `make lakehouse-prepare-fixtures` |
| Local only | Start Airflow against MinIO | `make airflow-up-local` |

Run `make` for the complete target list. `make spark-down`, `make airflow-down`,
and `make airflow-logs` manage the local Docker-based services.

## Optional local fixture harness

Use the local stack when a repeatable test data set is more useful than AWS
integration. It starts MinIO and Spark Thrift with a Hadoop catalog, then loads
fixtures through the same raw-to-bronze promotion code.

```bash
cp airflow/.env.local.example airflow/.env.local
cp lakehouse/.env.local.example lakehouse/.env.local
make spark-up
make lakehouse-prepare-fixtures  # deletes objects in the local MinIO bucket
make dbt-lakehouse-gold
```

The fixed fixture interval ends at `2024-03-16T06:00:00Z`. For this local
Hadoop-catalog command only, `make dbt-lakehouse-gold` supplies that timestamp
as the interval-coverage cutoff so the historical fixture remains deterministic.
AWS/Glue builds leave the cutoff unset and use the production rolling 30-hour
grace period. Set `DBT_INTERVAL_COVERAGE_CUTOFF_EPOCH` explicitly only when
evaluating a deliberately bounded historical fixture.

For an Airflow smoke, start `make airflow-up-local`, trigger all three ingest
DAGs for the same logical date, and let `promote_raw_to_bronze` select the
complete interval. The Airflow UI is <http://localhost:8080> with development
credentials `admin` / `admin`.

## Targeted extraction

After activating the appropriate Airflow environment, run the standalone
permits extractor for an explicit interval:

```bash
uv run airflow/include/scripts/permits.py \
  --window-start 2024-01-01T00:00:00Z \
  --window-end 2024-02-01T00:00:00Z \
  --lookback-hours 0
```

Without explicit bounds, it extracts from the dataset epoch through the current
UTC time. `make ingest` runs the same permits extractor using the active
`airflow/.env` file.

## Evidence site

After exporting snapshots, build the static site:

```bash
cd reports
npm ci
npm run sources
npm run build
```

Use [Evidence reports](../reports/README.md) for report-page development and
the fallback demo-data command. Deployment details live in
[Deployment](DEPLOY.md).
