# TODO

## Performance
- [ ] Migrate staging models from views to incremental materialization — currently views are fine but will become expensive as raw tables grow. Revisit when query performance degrades.
- [ ] Investigate streaming S3 writes per page-batch (avoid full in-memory accumulation in `soda_ingest.run()`) if large datasets cause OOM or API timeout issues during backfill — at 5–10 KB/record, datasets with 500k+ records can approach 2 GB of RAM in a single run.

## Airflow / DAG
- [x] Consolidate dbt tasks — moved to `transform_all.py` shared DAG with `ExternalTaskSensor` fan-in; dbt now runs once per day across all datasets instead of once per ingest DAG.

## MUNI Dashboard
- [ ] Fix route path rendering on map — pull static SFMTA stops and route line geometries from DataSF into S3/Snowflake (same pipeline pattern as permits/incidents) and query from there instead of approximating via the 511 `trippatterns` endpoint (stop-to-stop straight lines look bad at zoom 15).

## Observability
- [ ] Persist Airflow task logs to S3 — currently stored only in Docker containers and lost on `astro dev stop`. Set `AIRFLOW__LOGGING__REMOTE_LOGGING=True` and point at existing S3 bucket.
