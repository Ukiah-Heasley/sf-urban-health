# SF Urban Health Pipeline

[![CI](https://github.com/Ukiah-Heasley/sf-urban-health/actions/workflows/ci.yml/badge.svg)](https://github.com/Ukiah-Heasley/sf-urban-health/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11-3776AB?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-3.0-017CEE?logo=apacheairflow&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS-S3-569A31?logo=amazons3&logoColor=white)
[![Live demo](https://img.shields.io/badge/live%20demo-GitHub%20Pages-2ea44f)](https://ukiah-heasley.github.io/sf-urban-health/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

SF Urban Health ingests three DataSF civic datasets—building permits,
eviction notices, and police incident reports—and supports housing, public
safety, eviction, pipeline-health, and data-trust analysis.

The active runtime ingests DataSF responses into durable newline-delimited JSON
in S3, promotes complete intervals to bronze Parquet, and compacts lakehouse
metadata. The repository also keeps a lakehouse-first dbt project with a local
Spark + Iceberg smoke path, a Plotly Dash consumer shell, and an Evidence site
built from committed Parquet snapshots.

## Current runtime boundary

```text
DataSF SODA API
    → Airflow ingest DAGs
    → S3 raw interval NDJSON + S3 ingest metadata events
    → ingest Airflow assets

Airflow ingest assets
    → promote_raw_to_bronze (plans intervals from S3 JSON metadata events)
    → bronze Parquet + S3 file-manifest events
    → compacted metadata Parquet
    → bronze promotion asset

Bronze promotion asset
    → build_lakehouse_gold (dbt inside Astro Airflow)
    → silver/gold Iceberg tables
    → gold transform completion asset
```

The ingest DAGs land raw NDJSON in S3 and do not load a warehouse. Dashboard
and Evidence pages are retained as consumer shells that render from empty frames
or committed sample Parquet until lakehouse consumer wiring lands.

## Ingest behavior

Three generated DAGs run daily at 06:00 UTC:

```text
extract_{dataset}_to_raw → record_{dataset}_extract_metadata → ingest_complete
```

Each extractor:

- queries a half-open Airflow interval,
  `[data_interval_start, data_interval_end)`;
- paginates in timestamp plus source-key order;
- streams records through a temporary NDJSON file;
- uploads a deterministic S3 object; and
- returns row, byte, timing, interval, and observed maximum-timestamp metadata.

Raw objects use this shape:

```text
raw/{dataset}/
  data_interval_start=YYYYMMDDTHHMMSSZ/
  data_interval_end=YYYYMMDDTHHMMSSZ/
  records.ndjson
```

An empty interval is successful, uploads no raw object, still writes an ingest
metadata event, and emits its ingest asset.

`promote_raw_to_bronze` runs after all three ingest assets update. It selects
the oldest pending complete interval from current S3 ingest metadata events
(default `LAKEHOUSE_PLAN_MODE=pending`, `LAKEHOUSE_PLAN_LIMIT=1`), promotes that
interval to bronze Parquet, writes file-manifest metadata events, compacts
current metadata events into contract-compatible Parquet, and emits a bronze
promotion asset. When no interval is selected, the DAG branches to a no-op path
that emits no bronze or bronze promotion assets. After bronze promotion completes,
`build_lakehouse_gold` runs `dbt debug` and `dbt build --select tag:lakehouse`
inside the Astro Airflow runtime against the mirrored project at
`airflow/include/dbt/`, materializing silver and gold Iceberg tables through
Spark Thrift, and emits a gold transform completion asset. Set
`LAKEHOUSE_PLAN_MODE=refresh` with optional start/end bounds to reprocess
intervals without a separate code path. Current JSON metadata events are the
promotion source of truth; attempt audit events and compacted metadata Parquet
are not planner inputs.

See [Architecture](docs/ARCHITECTURE.md) for the complete current-state flow.

## Repository layout

```text
airflow/                 Astro project, DAGs, and extractors
contracts/lakehouse/     YAML contracts for parquet lake table layouts
dbt/                     Lakehouse-first dbt project (Spark + Iceberg locally)
lakehouse/               Local MinIO and Spark Thrift Server Compose stack
dashboard/               Six-page Plotly Dash consumer shell
reports/                 Static Evidence site and sample Parquet snapshots
tests/                   Extractor, DAG, and dashboard import tests
docs/                    Current behavior and operator documentation
.codex/skills/           Repository-specific Codex workflows
```

`dbt/` and `contracts/` are canonical at the repo root. `make sync-dbt` mirrors
them into gitignored `airflow/include/` directories for the Astro Docker build
context.

## Prerequisites

- Python 3.11 and [uv](https://docs.astral.sh/uv/)
- AWS credentials and an S3 bucket for extraction
- Docker Desktop and the Astro CLI for local Airflow
- Docker Desktop for the local lakehouse stack (`make spark-up`)
- A DataSF app token is optional but recommended
- Node.js 20 for the Evidence site

## Setup

```bash
uv sync --all-groups
cp airflow/.env.example airflow/.env
cp lakehouse/.env.example lakehouse/.env
```

Fill in the environment values needed for the component you intend to run.
The Airflow file is gitignored. `lakehouse/.env` supplies local MinIO and Spark
ports; `make spark-up` creates it from the example when missing.

## Common commands

```bash
make ingest            # permits: DataSF → raw S3
make test              # pytest
make lakehouse-smoke   # promote fixture NDJSON locally without AWS
make lint              # Ruff
make yamllint          # YAML lint
make pre-commit        # all configured hooks
make docs-check        # documentation consistency checks

make spark-up          # local MinIO + Spark Thrift Server
make spark-down
make lakehouse-prepare-fixtures       # destructive: reset MinIO, seed all fixtures, promote bronze
make lakehouse-prepare-permits-fixture  # alias for lakehouse-prepare-fixtures
make dbt-lakehouse-debug
make dbt-lakehouse-smoke
make dbt-lakehouse-permits              # build/test permits_current silver Iceberg
make dbt-lakehouse-gold                 # build/test all lakehouse bronze/silver/gold Iceberg models

make airflow-up        # sync Airflow mirrors, then start Astro Airflow
make airflow-down
make airflow-logs

make dashboard-dev     # http://localhost:8050
make dashboard-docker
```

For an explicit raw interval:

```bash
uv run airflow/include/scripts/permits.py \
  --window-start 2024-01-01T00:00:00Z \
  --window-end 2024-02-01T00:00:00Z
```

The Airflow UI is available at <http://localhost:8080> with the local
`admin` / `admin` development credentials.

For a local ingest-to-bronze smoke against MinIO instead of AWS S3, see
[Development — Local MinIO Airflow smoke](docs/DEVELOPMENT.md#local-minio-airflow-smoke).
For local dbt from Astro Airflow containers, set `DBT_SPARK_HOST=host.docker.internal`
and `DBT_SPARK_PORT` to the host port published by `make spark-up` (see
`airflow/.env.example`).

## Local lakehouse (Spark + Iceberg + dbt)

`lakehouse/docker-compose.yml` runs MinIO, a one-shot bucket initializer, and a
repo-built Spark Thrift Server with Iceberg and S3A enabled. Spark bootstraps
`default` and `sf_urban_health` namespaces in the MinIO warehouse before Thrift
starts. Iceberg warehouse data is stored under `s3a://lakehouse/warehouse/` in
the local bucket.

```bash
make spark-up
make lakehouse-prepare-fixtures         # destructive local-only reset + bronze fixtures (all datasets)
make dbt-lakehouse-debug
make dbt-lakehouse-permits
make dbt-lakehouse-gold
make dbt-lakehouse-smoke
make spark-down
```

`lakehouse-prepare-fixtures` deletes every object in the local `lakehouse` MinIO
bucket, seeds `tests/fixtures/lakehouse/{permits,evictions,incidents}.ndjson` as
raw NDJSON under the production interval key shape, writes ingest metadata events,
promotes bronze Parquet for all three datasets through the existing Python
promotion code, and restarts Spark Thrift so catalog namespaces are rebuilt after
the bucket wipe. `lakehouse-prepare-permits-fixture` is a compatibility alias.
dbt reads bronze through ephemeral `bronze_*` models over MinIO Parquet. This
target is local-only and requires `make spark-up`.

The dbt project uses medallion folder names (`bronze/`, `silver/`, `gold/`) and
does not mix `staging/`, `intermediate/`, or `mart_*` model names in the
lakehouse path. Gold tables are business-facing analytical relations; the `gold`
layer carries that meaning without a `mart_` prefix.

`dbt/profiles.yml` connects to Spark Thrift on `localhost:10000` by default.
`SPARK_THRIFT_PORT` in `lakehouse/.env` sets the host port exposed by Compose; the
container always listens on port `10000`. Override `DBT_SPARK_HOST`, `DBT_SPARK_PORT`
(to match `SPARK_THRIFT_PORT`), and `DBT_SPARK_SCHEMA` when needed.
The `lakehouse` uv dependency group installs `dbt-core` and `dbt-spark`. Bronze
remains Parquet in MinIO. dbt materializes silver and gold models as Iceberg
catalog tables through Spark Thrift. Silver current models deduplicate bronze by
natural key; gold models aggregate silver for housing production, permit
pipeline, evictions, and public safety. The `smoke_iceberg` model is tagged
`smoke` and remains a harmless Iceberg connectivity check.

## Lakehouse contracts

Lakehouse table contracts live under `contracts/lakehouse/` and are validated by
`airflow/include/scripts/lakehouse_contracts.py`. Bronze promotion lives in
`airflow/include/scripts/lakehouse_load.py`. Silver and gold lakehouse models
(`permits_current`, `evictions_current`, `incidents_current`, `housing_production`,
`permit_pipeline`, `evictions`, `public_safety`) are Iceberg catalog relation
contracts. Immutable metadata events and compaction live
in `airflow/include/scripts/lakehouse_metadata.py`.

## Dashboards

- [Plotly Dash](dashboard/README.md) keeps six interactive pages as a consumer
  shell. Live warehouse loading is disabled; pages render empty-state layouts
  without credentials.
- [Evidence](reports/README.md) reads committed local Parquet snapshots with
  DuckDB and is published to
  [GitHub Pages](https://ukiah-heasley.github.io/sf-urban-health/).

## Documentation

- [Documentation index](docs/README.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Data model](docs/DATA_MODEL.md)
- [Edge cases](docs/EDGE_CASES.md)
- [Development](docs/DEVELOPMENT.md)
- [Deployment](docs/DEPLOY.md)

## License

MIT — see [LICENSE](LICENSE).
