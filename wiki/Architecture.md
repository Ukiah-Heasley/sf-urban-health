# Architecture

```
DataSF SODA API
   │  (paginated REST, watermark = data_loaded_at)
   ▼
airflow/include/scripts/soda_ingest.py
   │  (NDJSON, one record per line)
   ▼
S3: s3://$AWS_S3_BUCKET/raw/<dataset>/YYYY/MM/DD/<dataset>.json
   │  (durable raw layer; permanent)
   ▼
Snowflake RAW.<DATASET>  ◄── COPY INTO via SQLExecuteQueryOperator
   │  (loading target, not source-of-truth)
   ▼
dbt staging  →  intermediate  →  marts
   │  (views → views → tables)
   ▼
SF_URBAN_HEALTH.MARTS.* ── Plotly Dash dashboard
SF_URBAN_HEALTH.METADATA.*  (observability marts)
```

## DAGs

Five DAGs orchestrate the platform, all defined under `airflow/dags/`:

| DAG | Schedule | What it does |
|---|---|---|
| `ingest_permits` | `0 6 * * *` | DataSF → S3 → `RAW.PERMITS` → `METADATA.INGEST_WATERMARKS` |
| `ingest_evictions` | `0 6 * * *` | DataSF → S3 → `RAW.EVICTIONS` → `METADATA.INGEST_WATERMARKS` |
| `ingest_incidents` | `0 6 * * *` | DataSF → S3 → `RAW.INCIDENTS` → `METADATA.INGEST_WATERMARKS` |
| `transform_all` | `0 7 * * *` | After ingests, runs `dbt build` against all three datasets |
| `ingest_pipeline_metadata` | `0 7 * * *` | Pulls Airflow REST API → `METADATA.AIRFLOW_DAG_RUNS` / `AIRFLOW_TASK_INSTANCES` for the observability marts |

The three ingest DAGs share a factory (`airflow/dags/dag_factory.py`) so
adding a fourth dataset is a single `DagConfig(...)` declaration — see
[[Design-Decisions]] for why.

## Layers

### S3 (durable raw)

Newline-delimited JSON, one object per line. Path partitioning matches
the run date so a backfill is just `aws s3 sync` of the relevant
prefixes. **S3 is the source of truth — never re-hit the DataSF API to
reprocess.**

### Snowflake RAW

`COPY INTO ... FROM @S3_STAGE/...` with `STRIP_OUTER_ARRAY = FALSE` so
the NDJSON shape from S3 maps cleanly onto Snowflake `VARIANT` rows.
Idempotent on the stage path, so overlapping loads are safe.

### dbt staging

One view per source. Cleans typing (`payload:field::type`),
`coalesce`s nulls, and enforces a stable PK + `_loaded_at` shape. A
small `normalize_neighborhood` macro collapses DataSF's null / empty /
"unknown" neighborhood spellings into `'Unknown'` and `initcap`s the
rest — every neighborhood-grain mart must use it.

### dbt intermediate

Per-record derived models (`int_permit_timelines`, `int_incident_timelines`):
one row per permit / incident with computed lifecycle timings, structural-
change classification, and time-of-day buckets consumed by the marts.

### dbt marts

| Mart | Grain | Purpose |
|---|---|---|
| `mart_housing_production` | `(filed_month, neighborhood, supervisor_district, use_transition)` | Net-units / cost-per-unit BI |
| `mart_evictions` | `(filed_month, neighborhood, supervisor_district, eviction_type)` | Eviction trend & top-neighborhood views |
| `mart_public_safety` | `(incident_month, neighborhood, supervisor_district, police_district, incident_category)` | Incident & resolution-rate analysis |
| `mart_permit_pipeline` | `(neighborhood, supervisor_district, lifecycle_stage, age_bucket)` | Pipeline backlog snapshot |
| `mart_pipeline_health` | `(run_date, dag_id)` | DAG-run success rate & duration |
| `mart_dbt_test_health` | `(run_date, run_id, test_name)` | Per-test pass/fail trend |
| `mart_data_trust` | `(dataset_name)` | Composite trust score per source |

Marts materialize as **tables** for fast dashboard reads; staging and
intermediate stay as **views** so they're always fresh.

## Observability

The Airflow REST client (`airflow/include/scripts/airflow_rest_client.py`)
runs daily (07:00 UTC) and writes DAG-run + task-instance metadata into
`METADATA.AIRFLOW_DAG_RUNS` and `METADATA.AIRFLOW_TASK_INSTANCES`. dbt
treats these as sources, builds the four observability marts above,
and surfaces them in the Pipeline Health, Eng Health, and Data Trust
dashboard pages — closing the loop on "is the pipeline healthy?".

The `elementary` dbt package is also installed for test-result tracking.
