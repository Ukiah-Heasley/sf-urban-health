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

`promote_raw_to_bronze` reads `LAKEHOUSE_PLAN_MODE`,
`LAKEHOUSE_PLAN_LIMIT` (must be `1`), and optional
`LAKEHOUSE_PLAN_START` / `LAKEHOUSE_PLAN_END` from the active Airflow
environment. Set matching bounds when promoting a manual full or backfill
window.

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
To select that same window for bronze promotion, set matching
`LAKEHOUSE_PLAN_START` and `LAKEHOUSE_PLAN_END` in the Airflow environment.

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
