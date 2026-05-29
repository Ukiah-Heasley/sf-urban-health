# Deployment

## Portfolio demo — static Evidence snapshot on GitHub Pages

The public, clickable demo at **<https://ukiah-heasley.github.io/sf-urban-health/>**
is a static [Evidence](https://evidence.dev) site published by the
[`pages.yml`](../.github/workflows/pages.yml) workflow. It is free, needs no
runtime warehouse, and stays close to the reference `ecommerce-analytics-dbt`
project's pattern.

### How it works

```
Snowflake MARTS / METADATA
   │  reports/scripts/export_marts.py  (nightly, in CI)
   ▼
reports/sources/sf_urban_health/data/*.parquet
   │  Evidence DuckDB source reads the parquet at build time
   ▼
reports/build/  →  GitHub Pages
```

- The Evidence project lives in [`reports/`](../reports/) (2 pages: Housing, and
  Pipeline Health & Data Trust).
- `export_marts.py` dumps each mart to parquet using the same `SNOWFLAKE_*`
  env vars as the rest of the project. **If those secrets are absent** (a fork,
  or before they're configured), the export no-ops and the **committed sample
  snapshot** in `reports/sources/sf_urban_health/data/` is used — so the site
  always builds. Regenerate the sample with
  `uv run --group dashboard python reports/scripts/make_sample_data.py`.

### One-time owner setup (to go live)

1. **Settings → Pages → Source = "GitHub Actions".**
2. Confirm the six `SNOWFLAKE_*` repo secrets exist (same ones `dbt-ci.yml`
   uses). Without them the demo still publishes, on sample data.
3. Trigger once via **Actions → pages → Run workflow** (or wait for the 09:00
   UTC schedule).

### Build locally

```bash
uv run --group dashboard python reports/scripts/make_sample_data.py   # or export_marts.py
cd reports && npm ci && npm run sources && npm run build              # output in reports/build/
npm run preview                                                      # serve the built site
```

## Live operator dashboard (Plotly Dash) — optional

The Plotly Dash app (`dashboard/`) reads live from Snowflake and is the
operator-facing view. It runs locally via `make dashboard-dev` and ships a
`dashboard/Dockerfile` (gunicorn against the exported `server`). To host it,
deploy that image to a container PaaS (Render / Fly.io / Railway / Cloud Run)
with the `SNOWFLAKE_*` env vars and a **read-only** Snowflake role (uncomment
the reader role in [`snowflake/bootstrap.sql`](../snowflake/bootstrap.sql)).
This is optional — the Evidence snapshot above is the portfolio link.

## Operator deployment for the data pipeline

The ingest + transform pipeline deploys independently of the demo:

1. **Snowflake bootstrap.** Run [`snowflake/bootstrap.sql`](../snowflake/bootstrap.sql)
   once (database, five schemas, S3 stage, RAW + METADATA tables, optional
   reader role).
2. **Managed Airflow.** Deploy `airflow/` via the Astro CLI
   (`cd airflow && astro deploy`) to Astronomer Cloud / MWAA / Composer. Set the
   Snowflake + AWS connections and env vars in the platform UI, mirroring
   `airflow/.env.example`. Define the `snowflake_default` connection there —
   do not ship `airflow_settings.yaml` with a literal password (see TODO.md H3).
3. **dbt scheduling.** The `transform_all` DAG already runs `dbt build` after the
   ingests succeed — no separate dbt Cloud account required.
4. **CI secrets.** `dbt-ci.yml` and `pages.yml` need the six `SNOWFLAKE_*` repo
   secrets (Settings → Secrets and variables → Actions). `ci.yml` needs none.

See [WIKI-SYNC.md](WIKI-SYNC.md) for publishing the wiki content.
