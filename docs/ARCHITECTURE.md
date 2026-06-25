# Architecture

The checked-in code has a raw S3 ingestion path and retained Snowflake-backed
transformation and consumer paths.

## Raw ingestion

```text
DataSF SODA API
    │  paginated HTTPS; half-open Airflow data interval
    ▼
airflow/include/scripts/soda_ingest.py
    │  streaming compact NDJSON
    ▼
S3 raw/{dataset}/data_interval_start=.../data_interval_end=.../records.ndjson
    │
    ▼
Airflow ingest-complete asset
```

The three generated ingest DAGs are:

| DAG | Schedule | Tasks |
| --- | --- | --- |
| `ingest_permits` | `0 6 * * *` | `extract_permits_to_raw -> ingest_complete` |
| `ingest_evictions` | `0 6 * * *` | `extract_evictions_to_raw -> ingest_complete` |
| `ingest_incidents` | `0 6 * * *` | `extract_incidents_to_raw -> ingest_complete` |

`dag_factory.py` builds all three from `DatasetConfig` and `DagConfig` values.
Each successful extract task pushes raw location, counts, timing, interval, and
observed maximum source timestamp through XCom. Empty intervals return null raw
paths and still emit the asset.

The retained SQL files under `airflow/include/sql/` are not referenced by the
current ingest DAG factory. In particular, raw S3 objects are not currently
copied into Snowflake by these DAGs.

## Snowflake transformation path

```text
permits ingest asset   ─┐
evictions ingest asset ─┼─> transform_all
incidents ingest asset ─┘      │
                               ├─> dbt deps
Existing Snowflake RAW sources ├─> dbt run --target prod
                               └─> dbt test --target prod
```

The dbt project expects `SF_URBAN_HEALTH.RAW` source tables populated outside
the current raw ingest DAG. It builds:

```text
RAW sources -> staging views -> intermediate views -> mart tables
```

`dbt/` is the canonical project. `make sync-dbt` mirrors it into
`airflow/include/dbt/` for the Astro Docker build.

## Pipeline metadata path

```text
Airflow REST API
    -> ingest_pipeline_metadata (07:00 UTC)
    -> Snowflake METADATA.AIRFLOW_DAG_RUNS / AIRFLOW_TASK_INSTANCES
    -> dbt observability marts
```

The collector enriches extract task instances with selected XCom values before
upserting them. The checked-in collector asks for `max_watermark`, while the
current extract DAG publishes `max_loaded_at`; the maximum timestamp therefore
does not currently populate that Snowflake field.

## Consumers

- Plotly Dash reads Snowflake mart and metadata schemas into Polars frames when
  the process starts.
- Evidence reads local Parquet snapshots with DuckDB during its static build.
  The Pages workflow first attempts a Snowflake-to-Parquet export and otherwise
  uses the checked-in sample snapshot.

These paths are separate processes; neither dashboard is part of an ingest DAG.
