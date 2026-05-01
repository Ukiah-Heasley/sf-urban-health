# TODO

## Performance
- [ ] Materialize `stg_permits` as a table instead of a view — currently re-runs full dedup window function across all of `RAW.PERMITS` on every downstream query. Change `config(materialized='table')` in `dbt/models/staging/stg_permits.sql` and update CLAUDE.md.

## Airflow / DAG
- [ ] Consolidate the three dbt BashOperator tasks (`dbt_deps`, `run_dbt_staging`, `run_dbt_marts`) in `dag_factory.py` into a single task — reduces DAG complexity and overhead. Could be one BashOperator chaining the commands, or switch to [Astronomer Cosmos](https://github.com/astronomer/astronomer-cosmos) for native dbt-as-tasks support.

## MUNI Dashboard
- [ ] Fix route path rendering on map — pull static SFMTA stops and route line geometries from DataSF into S3/Snowflake (same pipeline pattern as permits/incidents) and query from there instead of approximating via the 511 `trippatterns` endpoint (stop-to-stop straight lines look bad at zoom 15).

## Observability
- [ ] Persist Airflow task logs to S3 — currently stored only in Docker containers and lost on `astro dev stop`. Set `AIRFLOW__LOGGING__REMOTE_LOGGING=True` and point at existing S3 bucket.
