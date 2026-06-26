# SF Urban Health Evidence Reports

This Evidence application builds the public static site at
<https://ukiah-heasley.github.io/sf-urban-health/>.

Evidence queries three local Parquet snapshots through DuckDB:

- `mart_housing_production`
- `mart_pipeline_health`
- `mart_data_trust`

The Pages workflow builds from the committed sample snapshots only.

## Develop

From the repository root:

```bash
uv run python reports/scripts/make_sample_data.py

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
    data/                        committed sample Parquet snapshots
  scripts/make_sample_data.py    deterministic-shape synthetic data
```

Deployment behavior is documented in [Deployment](../docs/DEPLOY.md).
