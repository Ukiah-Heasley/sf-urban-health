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

GitHub Pages builds from committed snapshots only. It does not query Spark,
Iceberg, or MinIO at deploy time.

Local export after building lakehouse gold:

```bash
make spark-up
make lakehouse-prepare-fixtures
make dbt-lakehouse-gold
make export-evidence-snapshots
cd reports
npm ci
npm run sources
npm run build
```

AWS Glue export after building lakehouse gold:

```bash
make spark-up-aws
make dbt-lakehouse-gold LAKEHOUSE_ENV_FILE=lakehouse/.env.aws
make export-evidence-snapshots LAKEHOUSE_ENV_FILE=lakehouse/.env.aws
cd reports
npm ci
npm run sources
npm run build
```

Fallback when Spark is unavailable:

```bash
uv run python reports/scripts/make_sample_data.py
cd reports
npm ci
npm run sources
npm run build
```

`make export-evidence-snapshots` writes:

- `housing_production.parquet`
- `permit_pipeline.parquet`
- `evictions.parquet`
- `public_safety.parquet`
- `pipeline_health.parquet`
- `data_trust.parquet`

Each filename matches its source gold Iceberg table. `pipeline_health` and
`data_trust` are built from compacted lakehouse metadata before export.

Repository Pages must use **GitHub Actions** as its source. The workflow runs on
main pushes that touch report files, manual dispatch, relevant pull requests,
and its daily schedule.

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
