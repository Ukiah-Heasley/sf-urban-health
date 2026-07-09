# SF Urban Health Pipeline

[![CI](https://github.com/Ukiah-Heasley/sf-urban-health/actions/workflows/ci.yml/badge.svg)](https://github.com/Ukiah-Heasley/sf-urban-health/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11-3776AB?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-3.0-017CEE?logo=apacheairflow&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS-S3-569A31?logo=amazons3&logoColor=white)
[![Live demo](https://img.shields.io/badge/live%20demo-GitHub%20Pages-2ea44f)](https://ukiah-heasley.github.io/sf-urban-health/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

SF Urban Health turns San Francisco building permits, eviction notices, and
police incident data into a documented lakehouse and public evidence site. It
is a practical reference implementation for interval-based ingestion,
metadata-driven orchestration, dbt + Iceberg modeling, and static reporting.

## Explore

- [Live Evidence site](https://ukiah-heasley.github.io/sf-urban-health/) — public views built from committed Parquet snapshots.
- [Interactive architecture whiteboard](https://ukiah-heasley.github.io/sf-urban-health/architecture/sf-urban-health.pipeflow.html) — explore the system and its data contracts.
- [Documentation guide](docs/README.md) — setup, architecture, data model, operations, and deployment.

The whiteboard's [PipeFlow JSON source](reports/static/architecture/sf-urban-health.pipeflow.json)
is version controlled for review and future edits.

## At a glance

```text
DataSF APIs
    → Airflow ingest DAGs
    → S3 raw NDJSON + JSON metadata events
    → bronze Parquet + compacted metadata
    → dbt / Spark Thrift / Iceberg silver and gold tables
    → committed Evidence Parquet snapshots
    → GitHub Pages
```

Ingest runs daily, keeps raw records source-faithful, and records both current
and attempt-level metadata. Promotion selects complete intervals, and failed
extracts still refresh operational health reporting. See the
[architecture guide](docs/ARCHITECTURE.md) for the full runtime and trigger
details.

## Start here

The quickest code-only check is:

```bash
uv sync --all-groups
make test
```

The production-oriented data path uses AWS S3 for raw and Parquet storage and
the AWS Glue catalog for Iceberg. Configure the private AWS environment files
and follow the [AWS S3 + Glue workflow](docs/DEVELOPMENT.md#aws-s3--glue-workflow)
to run the supported ingestion and transform commands.

The MinIO/Spark Compose stack is an optional local fixture harness, not the
primary data path. [Development](docs/DEVELOPMENT.md) covers AWS configuration,
Airflow, manual backfills, local smoke testing, and the full command reference.

## Common tasks

| Goal | Command |
| --- | --- |
| Run tests | `make test` |
| Run all project quality checks | `make pre-commit` |
| Start AWS-configured Airflow | `make airflow-up-aws` |
| Start Spark Thrift with the Glue catalog | `make spark-up-aws` |
| Build lakehouse models | `make dbt-lakehouse-gold LAKEHOUSE_ENV_FILE=lakehouse/.env.aws` |
| Export Evidence snapshots | `make export-evidence-snapshots` |

## Repository map

| Path | Purpose |
| --- | --- |
| `airflow/` | Astro project, generated DAGs, and extractors |
| `contracts/lakehouse/` | Versioned Parquet and Iceberg contracts |
| `dbt/` | Canonical bronze, silver, and gold models |
| `lakehouse/` | Spark Thrift Compose runtime and optional MinIO harness |
| `reports/` | Evidence site, snapshots, and PipeFlow architecture board |
| `docs/` | Architecture, operations, and deployment guides |

## Documentation

- [Development](docs/DEVELOPMENT.md) — AWS configuration, commands, Airflow, and dbt workflows
- [Architecture](docs/ARCHITECTURE.md) — runtime paths, assets, and orchestration
- [Data model](docs/DATA_MODEL.md) — raw data, contracts, table grains, and snapshots
- [Edge cases](docs/EDGE_CASES.md) — retry, failure, and extraction behavior
- [Deployment](docs/DEPLOY.md) — GitHub Pages and supported runtime deployment paths
- [Evidence reports](reports/README.md) — report development and snapshot regeneration

## License

MIT — see [LICENSE](LICENSE).
