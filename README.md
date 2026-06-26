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
metadata. The repository also keeps a minimal dbt skeleton, a Plotly Dash
consumer shell, and an Evidence site built from committed Parquet snapshots.

## Current runtime boundary

```text
DataSF SODA API
    → Airflow ingest DAGs
    → S3 raw interval NDJSON + S3 ingest metadata events
    → ingest Airflow assets

Airflow ingest assets
    → transform_lakehouse (plans intervals from S3 JSON metadata events)
    → bronze Parquet + S3 file-manifest events
    → compacted metadata Parquet
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
are not planner inputs.

See [Architecture](docs/ARCHITECTURE.md) for the complete current-state flow.

## Repository layout

```text
airflow/                 Astro project, DAGs, and extractors
contracts/lakehouse/     YAML contracts for parquet lake table layouts
dbt/                     Minimal lakehouse-first dbt skeleton
dashboard/               Six-page Plotly Dash consumer shell
reports/                 Static Evidence site and sample Parquet snapshots
tests/                   Extractor, DAG, and dashboard import tests
docs/                    Current behavior and operator documentation
.codex/skills/           Repository-specific Codex workflows
```

`dbt/` is the canonical project. `make sync-dbt` mirrors it into the gitignored
`airflow/include/dbt/` directory for the Astro Docker build context.

## Prerequisites

- Python 3.11 and [uv](https://docs.astral.sh/uv/)
- AWS credentials and an S3 bucket for extraction
- Docker Desktop and the Astro CLI for local Airflow
- A DataSF app token is optional but recommended
- Node.js 20 for the Evidence site

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

## Lakehouse contracts

Lakehouse parquet table contracts live under `contracts/lakehouse/` and are
validated by `airflow/include/scripts/lakehouse_contracts.py`. Bronze promotion
lives in `airflow/include/scripts/lakehouse_load.py`. Immutable metadata events
and compaction live in `airflow/include/scripts/lakehouse_metadata.py`.

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
