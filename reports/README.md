# SF Urban Health Evidence Reports

This Evidence application builds the public static site at
<https://ukiah-heasley.github.io/sf-urban-health/>.

Evidence queries three local Parquet snapshots through DuckDB:

- `mart_housing_production`
- `mart_pipeline_health`
- `mart_data_trust`

The Pages workflow runs `scripts/export_marts.py` before the build. With a
complete Snowflake environment it replaces the snapshots from current marts.
Without those credentials the script leaves the committed sample data in place.

## Develop

From the repository root:

```bash
uv run --group dashboard python reports/scripts/make_sample_data.py

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
    data/                        committed sample/current Parquet snapshots
  scripts/export_marts.py        Snowflake mart export
  scripts/make_sample_data.py    deterministic-shape synthetic data
```

Deployment behavior is documented in [Deployment](../docs/DEPLOY.md).
