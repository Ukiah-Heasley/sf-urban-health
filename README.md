# SF Urban Health Pipeline

[![CI](https://github.com/Ukiah-Heasley/sf-urban-health/actions/workflows/ci.yml/badge.svg)](https://github.com/Ukiah-Heasley/sf-urban-health/actions/workflows/ci.yml)
[![dbt CI](https://github.com/Ukiah-Heasley/sf-urban-health/actions/workflows/dbt-ci.yml/badge.svg)](https://github.com/Ukiah-Heasley/sf-urban-health/actions/workflows/dbt-ci.yml)
![Python](https://img.shields.io/badge/python-3.11-3776AB?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-3.0-017CEE?logo=apacheairflow&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS-S3-569A31?logo=amazons3&logoColor=white)
[![Live demo](https://img.shields.io/badge/live%20demo-GitHub%20Pages-2ea44f)](https://ukiah-heasley.github.io/sf-urban-health/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

SF Urban Health ingests three DataSF civic datasets—building permits,
eviction notices, and police incident reports—and supports housing, public
safety, eviction, pipeline-health, and data-trust analysis.

The current ingest path uses Airflow data intervals to stream DataSF responses
into durable newline-delimited JSON objects in S3. The repository also contains
a Snowflake/dbt transformation project, a Snowflake-backed Plotly Dash app, and
an Evidence site built from Parquet snapshots.

## Current runtime boundary

The codebase currently has two distinct boundaries:

```text
DataSF SODA API
    → Airflow ingest DAGs
    → S3 raw interval NDJSON + S3 ingest metadata events
    → ingest Airflow assets

Airflow ingest assets
    → transform_lakehouse (plans intervals from S3 JSON metadata events)
    → bronze Parquet + S3 file-manifest events
    → compacted metadata Parquet

Existing Snowflake source tables
    → asset-triggered transform_all DAG
    → dbt staging / intermediate / marts
    → Plotly Dash and Parquet exports for Evidence
```

The ingest DAGs do **not** currently copy their new S3 raw objects into
Snowflake. `transform_all` and the dashboards still operate against the
existing Snowflake sources. This distinction matters when running the project
from a clean environment.

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

`transform_lakehouse` runs after all three ingest assets update. It selects
the oldest pending complete interval from current S3 ingest metadata events
(default `LAKEHOUSE_PLAN_MODE=pending`, `LAKEHOUSE_PLAN_LIMIT=1`), promotes that
interval to bronze Parquet, writes file-manifest metadata events, compacts
current metadata events into contract-compatible Parquet, and emits a lakehouse
transform asset. When no interval is selected, the DAG branches to a no-op path
that emits no bronze or lakehouse transform assets. Set
`LAKEHOUSE_PLAN_MODE=refresh` with optional start/end bounds to reprocess
intervals without a separate code path. Current JSON metadata events are the
promotion source of truth; attempt audit events and compacted metadata Parquet
are not planner inputs. Snowflake `transform_all` remains triggered only by
ingest assets.

## Other Airflow DAGs

- `transform_all` waits for all three ingest assets and runs `dbt deps`,
  `dbt run`, and `dbt test` against the Snowflake `prod` target.
- `ingest_pipeline_metadata` runs at 07:00 UTC, reads Airflow run/task/XCom
  metadata through the REST API, and upserts it into Snowflake metadata tables.

See [Architecture](docs/ARCHITECTURE.md) for the complete current-state flow.

## Repository layout

```text
airflow/                 Astro project, DAGs, extractors, and SQL helpers
contracts/lakehouse/     YAML contracts for parquet lake table layouts
dbt/                     Canonical Snowflake dbt project
dashboard/               Six-page Plotly Dash application
reports/                 Static Evidence site and Parquet snapshot tooling
snowflake/               Existing Snowflake bootstrap and maintenance SQL
tests/                   Extractor, DAG, and dashboard import tests
docs/                    Current behavior and operator documentation
.codex/skills/           Repository-specific Codex workflows
```

`dbt/` is canonical. `make sync-dbt` mirrors it into the gitignored
`airflow/include/dbt/` directory for the Astro Docker build context.

## Prerequisites

- Python 3.11 and [uv](https://docs.astral.sh/uv/)
- AWS credentials and an S3 bucket for extraction
- Docker Desktop and the Astro CLI for local Airflow
- A DataSF app token is optional but recommended
- Snowflake credentials are required only for the dbt, live dashboard,
  observability, and real-data report-export paths

## Setup

```bash
uv sync --all-groups
cp airflow/.env.example airflow/.env
```

Fill in the environment values needed for the component you intend to run.
The file is gitignored.

## Common commands

```bash
make ingest            # permits: DataSF → raw S3
make test              # pytest
make lakehouse-smoke   # promote fixture NDJSON locally without AWS
make lint              # Ruff
make yamllint          # YAML lint
make pre-commit        # all configured hooks
make docs-check        # documentation consistency checks

make airflow-up        # sync dbt mirror, then start Astro Airflow
make airflow-down
make airflow-logs

make dbt-deps
make dbt-run           # Snowflake dev target
make dbt-build         # Snowflake dev target: run + test
make dbt-test

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

## Data products

The checked-in dbt project defines:

- source-cleaning staging models for permits, evictions, and incidents;
- permit and incident intermediate timelines;
- housing, eviction, public-safety, and permit-pipeline marts; and
- Airflow/dbt observability marts used by the engineering and trust pages.

The model contracts and grains are documented in
[Data Model](docs/DATA_MODEL.md). Lakehouse parquet table contracts live under
`contracts/lakehouse/` and are validated by
`airflow/include/scripts/lakehouse_contracts.py`. Bronze promotion lives in
`airflow/include/scripts/lakehouse_load.py`. Immutable metadata events and
compaction live in `airflow/include/scripts/lakehouse_metadata.py`.

## Dashboards

- [Plotly Dash](dashboard/README.md) loads Snowflake marts into Polars frames at
  process startup and serves six interactive pages.
- [Evidence](reports/README.md) reads local Parquet snapshots with DuckDB and is
  published to [GitHub Pages](https://ukiah-heasley.github.io/sf-urban-health/).

## Documentation

- [Documentation index](docs/README.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Data model](docs/DATA_MODEL.md)
- [Edge cases](docs/EDGE_CASES.md)
- [Development](docs/DEVELOPMENT.md)
- [Deployment](docs/DEPLOY.md)

## License

MIT — see [LICENSE](LICENSE).
