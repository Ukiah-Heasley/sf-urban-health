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

The deployed site also serves the standalone interactive PipeFlow architecture
whiteboard at `/architecture/sf-urban-health.pipeflow.html`. Evidence copies
`reports/static/` to the Pages artifact unchanged; keep the corresponding
`sf-urban-health.pipeflow.json` sidecar there as the reviewable, editable board
source. Regenerate both files with PipeFlow's **Download HTML Bundle** action
when the architecture diagram changes.

AWS Glue export after building lakehouse gold (with existing AWS bronze and
metadata Parquet):

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

For an optional local fixture run, use `make spark-up`,
`make lakehouse-prepare-fixtures`, and `make dbt-lakehouse-gold` before the
same snapshot export and Evidence build commands. The local stack is a
development harness; Pages always deploys committed snapshot files.

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

## Airflow runtime boundary

The Astro project can run locally with `make airflow-up` or be packaged through
the Astro CLI. Configure AWS and DataSF values through the deployment platform.

The ingest DAGs land raw NDJSON in S3 and promote bronze Parquet through
`promote_raw_to_bronze`. `make airflow-up-aws` runs the local Astro container
stack with AWS S3 configuration. The AWS Glue transform path is currently a
host CLI proof run; Airflow does not orchestrate it. `make sync-dbt` mirrors
the canonical dbt project into `airflow/include/dbt/` before Airflow starts.
