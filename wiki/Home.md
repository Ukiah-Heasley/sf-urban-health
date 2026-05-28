# sf-urban-health

An end-to-end civic-data platform: DataSF SODA APIs → S3 raw lake →
Snowflake warehouse → dbt transformations → Plotly Dash dashboard, all
orchestrated by Apache Airflow.

Built as a hands-on exercise in production-grade data engineering for a
public-data domain — three SF civic datasets (housing permits, eviction
notices, public-safety incidents) wired all the way from API to BI.

## Stack

![Airflow](https://img.shields.io/badge/Airflow-3.0-017CEE) ![dbt](https://img.shields.io/badge/dbt-1.9-orange) ![Snowflake](https://img.shields.io/badge/Snowflake-warehouse-29b5e8) ![Python](https://img.shields.io/badge/Python-3.11-blue) ![Ruff](https://img.shields.io/badge/lint-ruff-261230) ![SQLFluff](https://img.shields.io/badge/lint-sqlfluff-25c2a0)

apache-airflow 3.0 · dbt-snowflake 1.9 · Snowflake · AWS S3 · Plotly Dash · Polars · Astro CLI

## Where to start

| If you want to… | Go to |
|---|---|
| Clone and run the pipeline | [README (repo)](https://github.com/Ukiah-Heasley/sf-urban-health#quickstart) |
| Understand the architecture | [[Architecture]] |
| See sources, marts, and grain | [[Data-Model]] |
| Read the design rationale | [[Design-Decisions]] |
| Walk through edge cases | [[Edge-Cases]] |
| See the dashboard | [[Dashboard]] |
| Set up dev tooling | [[Developer-Setup]] |

## What's in scope (Phase 1)

A vertical slice — three civic datasets wired through the same pipeline
shape so that the *plumbing* (orchestration, observability, lineage,
dashboarding) is what's being demonstrated, not the breadth of sources:

- **Building permits** — `data.sfgov.org/i98e-djp9`
- **Eviction notices** — `data.sfgov.org/5cei-gny5`
- **Public-safety incidents** — `data.sfgov.org/wg3w-h783`

Plus an Airflow-observability project under `dbt/models/metadata/` that
treats the pipeline's own DAG-run history as a first-class data source.

## Quickstart

See the repo [README](https://github.com/Ukiah-Heasley/sf-urban-health#quickstart)
for clone-and-run steps. See [[Developer-Setup]] for the lint stack, pre-commit hooks,
and dashboard tooling.

## Links

- **Source code:** [github.com/Ukiah-Heasley/sf-urban-health](https://github.com/Ukiah-Heasley/sf-urban-health)
- **Live dashboard:** decision pending — see [docs/DEPLOY.md](https://github.com/Ukiah-Heasley/sf-urban-health/blob/main/docs/DEPLOY.md)
- **License:** [MIT](https://github.com/Ukiah-Heasley/sf-urban-health/blob/main/LICENSE)
