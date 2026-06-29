# Deployment

## Static Evidence site

The public demo at <https://ukiah-heasley.github.io/sf-urban-health/> is built
by `.github/workflows/pages.yml` and deployed to GitHub Pages.

```text
committed Parquet snapshots
    -> Evidence DuckDB sources
    -> reports/build
    -> GitHub Pages
```

Local build:

```bash
uv run python reports/scripts/make_sample_data.py
cd reports
npm ci
npm run sources
npm run build
```

Repository Pages must use **GitHub Actions** as its source. The workflow runs on
manual dispatch, relevant pull requests, and its daily schedule.

## Plotly Dash

The Dash application is a consumer shell. Live warehouse loading is disabled;
pages render empty-state layouts without credentials.

```bash
make dashboard-dev
make dashboard-docker
docker run -p 8050:8050 sf-urban-health-dashboard
```

The image serves `dashboard.app:server` with Gunicorn.

## Airflow

The Astro project can run locally with `make airflow-up` or be packaged through
the Astro CLI. Configure AWS and DataSF values through the deployment platform.

The ingest DAGs land raw NDJSON in S3 and promote bronze Parquet through
`promote_raw_to_bronze`. `build_lakehouse_gold` runs dbt inside the Astro
image after bronze promotion completes, building lakehouse silver and gold
Iceberg models. Configure `DBT_SPARK_HOST`, `DBT_SPARK_PORT`, `DBT_SPARK_SCHEMA`,
and `DBT_SPARK_USER` so Airflow tasks can reach Spark Thrift. The mirrored dbt
project is synced into `airflow/include/dbt/` by `make sync-dbt` before
`make airflow-up`.
