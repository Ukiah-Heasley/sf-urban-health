# Deployment

## Static Evidence site

The public demo at <https://ukiah-heasley.github.io/sf-urban-health/> is built
by `.github/workflows/pages.yml` and deployed to GitHub Pages.

```text
Snowflake marts, when credentials are available
    -> reports/scripts/export_marts.py
    -> local Parquet snapshots
    -> Evidence DuckDB sources
    -> reports/build
    -> GitHub Pages
```

If Snowflake credentials are unavailable, the export script exits without
replacing data and the build uses the checked-in sample Parquet snapshot.

Local build:

```bash
uv run --group dashboard python reports/scripts/make_sample_data.py
cd reports
npm ci
npm run sources
npm run build
```

Repository Pages must use **GitHub Actions** as its source. The workflow runs on
manual dispatch, relevant pull requests, and its daily schedule.

## Plotly Dash

The live Dash application reads Snowflake at process startup.

```bash
make dashboard-dev
make dashboard-docker
docker run --env-file airflow/.env -p 8050:8050 sf-urban-health-dashboard
```

The image serves `dashboard.app:server` with Gunicorn. A deployment requires
network access to Snowflake and the Snowflake/dashboard environment variables
listed in [dashboard/README.md](../dashboard/README.md).

## Airflow

The Astro project can run locally with `make airflow-up` or be packaged through
the Astro CLI. Configure AWS and DataSF values through the deployment platform;
configure Snowflake as well if running `transform_all` or
`ingest_pipeline_metadata`.

The current ingest DAGs land raw NDJSON in S3 and stop there. Deploying the
checked-in Airflow project does not create an end-to-end S3-to-Snowflake load
because the retained Snowflake load SQL is not wired into `dag_factory.py`.

## Snowflake bootstrap

`snowflake/bootstrap.sql` creates the database, schemas, raw tables, metadata
tables, and S3 stage expected by the retained Snowflake components. It does not
connect the current raw ingest DAG to those tables.
