# SF Urban Health Evidence Reports

This Evidence application builds the public static site at
<https://ukiah-heasley.github.io/sf-urban-health/>.

Evidence queries three local Parquet snapshots through DuckDB:

- `mart_housing_production`
- `mart_pipeline_health`
- `mart_data_trust`

GitHub Pages builds from committed snapshots only. It does not query Spark or
Iceberg at deploy time.

## Regenerate snapshots

From the repository root, after local lakehouse gold models are built:

```bash
make spark-up
make lakehouse-prepare-fixtures
make dbt-lakehouse-gold
make export-evidence-snapshots
```

`make export-evidence-snapshots` runs `airflow/include/scripts/evidence_snapshots.py`.
It exports `mart_housing_production.parquet` from gold Iceberg
`sf_urban_health.housing_production` and writes deterministic observability
snapshots for `mart_pipeline_health.parquet` and `mart_data_trust.parquet`.

When Spark is unavailable, regenerate demo-shaped snapshots with:

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
  sources/sf_urban_health/       DuckDB connection and source queries
    data/                        committed Parquet snapshots
  scripts/make_sample_data.py    fallback deterministic demo data
```

Deployment behavior is documented in [Deployment](../docs/DEPLOY.md).
