# Deployment

> **Status: hosting decision deferred.**
>
> The Plotly Dash app reads live from Snowflake at request time, so
> the static-site-on-GitHub-Pages pattern used by the
> `ecommerce-analytics-dbt` reference project doesn't transfer
> directly. The candidate options are listed below; pick one when
> ready.

## Components and their current state

| Component | Where it runs today | Where it would live in production |
|---|---|---|
| Airflow ingest + transform DAGs | local Astro CLI | a managed Airflow (Astronomer Cloud / MWAA / Composer) |
| dbt project | local `uv run dbt build` | dbt Cloud OR scheduled in Airflow with `BashOperator` (already wired) |
| Snowflake warehouse | live | live |
| S3 raw lake | live | live |
| Plotly Dash dashboard | local `make dashboard-dev` | TBD — see below |

The DAGs and the dbt project already work in production today (modulo
the deferred Airflow / dbt fixes in `TODO.md`). The open question is
specifically how to host the live-reading Dash app.

## Dashboard hosting candidates

### Option A — Container PaaS

Deploy `dashboard/Dockerfile` to Render.com / Fly.io / Railway / Cloud
Run with the Snowflake env vars. The Dockerfile already runs gunicorn
against the WSGI handle exported by `dashboard/app.py`.

- ✅ True live data (no staleness budget).
- ✅ Smallest deviation from how it runs locally.
- ❌ Always-on infra cost (~$5–25/mo depending on tier).
- ❌ Needs a public-facing read role on Snowflake — implies a separate
  warehouse + read-only role to bound the blast radius if the connection
  string leaks.

### Option B — Static snapshot

A nightly Airflow task dumps each mart to `reports/data/*.parquet` and a
small static viewer (Evidence.dev, Observable, or even hand-rolled
Plotly + a JSON file) deploys to GitHub Pages.

- ✅ Free hosting, no Snowflake at runtime, portfolio-friendly URL.
- ✅ Survives without Snowflake credentials, makes the repo more
  approachable for reviewers who don't want to spin up a warehouse.
- ❌ Up to 24 h stale.
- ❌ Loses interactive cross-tab filtering (can be partially preserved
  with client-side filtering on a small Parquet file).

### Option C — Hybrid

Keep the Dash app for the operator-facing prod view (Option A) **plus**
a read-only static snapshot (Option B) on Pages for the portfolio link.

- ✅ Best of both worlds.
- ❌ Two deploy pipelines to maintain.

### Option D — Screenshots only

Record GIFs / PNGs of the local Dash app and embed them in the README.

- ✅ Zero ongoing cost or operational surface.
- ❌ Reviewers can't click around themselves.

## Why we're not choosing right now

Phase 1 prioritized correctness of the data pipeline. Hosting the
dashboard is a separate evaluation (cost, role-design on the Snowflake
side, portfolio narrative) that's worth doing deliberately rather than
defaulting to whichever option is closest at hand. This file is the
parking lot.

## Operator deployment for the data pipeline

Even with the dashboard hosting deferred, the data pipeline can deploy
today:

1. **Snowflake bootstrap.** Run `snowflake/bootstrap.sql` once to create
   the warehouse, database, schemas (`RAW`, `METADATA`, `MARTS`,
   `dbt_*` for dev / prod), and a read-only role for the dashboard
   when chosen.
2. **Astronomer Cloud (or other managed Airflow).** Deploy the `airflow/`
   directory via the Astro CLI: `cd airflow && astro deploy`. Set the
   Snowflake + AWS connection IDs and env vars in the Astro UI, mirroring
   `airflow/.env.example`.
3. **dbt scheduling.** The `transform_all` DAG already runs `dbt build`
   after the ingests succeed. No separate dbt Cloud account required.
4. **CI secrets.** The `dbt-ci.yml` workflow needs the six
   `SNOWFLAKE_*` repo secrets (Settings → Secrets and variables →
   Actions). The `ci.yml` workflow runs without warehouse access.

See [WIKI-SYNC.md](WIKI-SYNC.md) for publishing the wiki content.
