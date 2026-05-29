# SF Urban Health — Evidence reports

Static [Evidence](https://evidence.dev) site for the SF Urban Health pipeline,
published to GitHub Pages: <https://ukiah-heasley.github.io/sf-urban-health/>.

It reads the dbt marts as **parquet via DuckDB** at build time, so the site is a
nightly static snapshot with no runtime warehouse. Data is produced by
`scripts/export_marts.py` (Snowflake → parquet) in CI, or by
`scripts/make_sample_data.py` for a committed synthetic sample.

## Develop

```bash
# from the repo root: generate data (sample, or real via export_marts.py)
uv run --group dashboard python reports/scripts/make_sample_data.py

cd reports
npm ci
npm run sources   # run the DuckDB queries over the parquet
npm run dev       # http://localhost:3000   (or: npm run build && npm run preview)
```

## Layout

```
reports/
├── pages/                       # index.md (Housing), pipeline.md (Pipeline & Trust)
├── sources/sf_urban_health/     # DuckDB source: connection.yaml + one .sql per mart
│   └── data/*.parquet           # committed sample snapshot (overwritten in CI)
├── scripts/export_marts.py      # Snowflake → parquet (production)
└── scripts/make_sample_data.py  # synthetic sample → parquet (demo / fork builds)
```

Deploy + data flow: [../docs/DEPLOY.md](../docs/DEPLOY.md).
