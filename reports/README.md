# SF Urban Health Evidence Reports

This Evidence application builds the public static site at
<https://ukiah-heasley.github.io/sf-urban-health/>.

Evidence queries six committed Parquet snapshots through DuckDB:

- `housing_production`
- `permit_pipeline`
- `evictions`
- `public_safety`
- `pipeline_health`
- `data_trust`

GitHub Pages builds from committed snapshots only. It does not query Spark or
Iceberg at deploy time.

## Interactive architecture whiteboard

`static/architecture/sf-urban-health.pipeflow.html` is a standalone PipeFlow
board served by the deployed site at
`/architecture/sf-urban-health.pipeflow.html`. Its adjacent
`sf-urban-health.pipeflow.json` file is the canonical review and edit source.
Use PipeFlow's **Download HTML Bundle** action to replace both files after
editing the diagram.

## Regenerate snapshots

From the repository root, export from the selected Spark/Iceberg catalog after
lakehouse gold models build. The AWS Glue path uses the AWS lakehouse env file:

```bash
make spark-up-aws
make dbt-lakehouse-gold LAKEHOUSE_ENV_FILE=lakehouse/.env.aws
make export-evidence-snapshots LAKEHOUSE_ENV_FILE=lakehouse/.env.aws
```

`make export-evidence-snapshots` runs `airflow/include/scripts/evidence_snapshots.py`.
The report pages read `housing_production.parquet`, `permit_pipeline.parquet`,
`evictions.parquet`, `public_safety.parquet`, `pipeline_health.parquet`, and
`data_trust.parquet` from the selected Spark/Iceberg gold schema.

For the optional local fixture harness, run `make spark-up`,
`make lakehouse-prepare-fixtures`, and `make dbt-lakehouse-gold` before the
same export command. When Spark is unavailable, regenerate demo-shaped
snapshots with:

```bash
uv run python reports/scripts/make_sample_data.py
```

## Develop

```bash
cd reports
npm ci
npm run sources
npm run dev
```

The development server uses <http://localhost:3000>. A production-equivalent
local build uses `npm run build && npm run preview`.

## Layout

```text
reports/
  pages/                         Evidence pages
  static/architecture/           deployed PipeFlow board and JSON sidecar
  sources/sf_urban_health/       DuckDB connection and source queries
    data/                        committed Parquet snapshots
  scripts/make_sample_data.py    fallback deterministic demo data
```

Deployment behavior is documented in [Deployment](../docs/DEPLOY.md).
