# Dashboard

A Plotly Dash multi-page app under `dashboard/`. Reads dbt marts from
Snowflake into in-memory Polars DataFrames at boot and serves all
callbacks from memory.

## Pages

| Route | Module | Source mart | Audience |
|---|---|---|---|
| `/` | `dashboard/pages/housing.py` | `mart_housing_production` | Analyst |
| `/incidents` | `dashboard/pages/incidents.py` | `mart_public_safety` | Analyst |
| `/evictions` | `dashboard/pages/evictions.py` | `mart_evictions` | Analyst |
| `/pipeline` | `dashboard/pages/pipeline_health.py` | `mart_pipeline_health`, `mart_dbt_test_health` | Analyst |
| `/engineer` | `dashboard/pages/engineer_health.py` | `mart_pipeline_health`, `mart_dbt_test_health` | Engineer |
| `/data-trust` | `dashboard/pages/data_trust.py` | `mart_data_trust`, `mart_pipeline_health`, `mart_dbt_test_health` | Analyst |

## Components

Pure helpers under `dashboard/components/`:

- `figures.py` — registers two Plotly templates (`light`, `dark`) used
  by every page.
- `kpi.py` — KPI-card factory.
- `theme_utils.py` — small color utilities.
- One `_figures.py` per page (housing, incidents, evictions, pipeline,
  engineer, trust) — the page-specific Plotly figure builders.

## Data layer

Under `dashboard/data/`:

- `snowflake.py` — thin client that reads creds from env vars (no fallback
  strings; missing-var → `RuntimeError`).
- `cache.py` — module-level `_load(...)` calls for each mart, wrapped in
  try/except so the dashboard imports cleanly even without a live
  Snowflake (returns empty DataFrames; pages handle gracefully).
- `transforms.py` + `*_transforms.py` — Polars helpers for each page.

## Run locally

```bash
make dashboard-dev
```

This loads `airflow/.env`, installs the `dashboard` dependency group via
`uv`, and runs `python -m dashboard.app` on `http://localhost:8050`.

Set `DASH_DEBUG=1` to enable the Dash dev-tools panel (off by default —
see [[Edge-Cases]]).

## Run in Docker

```bash
make dashboard-docker
docker run --env-file airflow/.env -p 8050:8050 sf-urban-health-dashboard
```

The Dockerfile uses `gunicorn` to serve the `server` WSGI handle exported
from `dashboard/app.py`.

## Config

| Var | Default | Purpose |
|---|---|---|
| `SNOWFLAKE_ACCOUNT` | required | warehouse account |
| `SNOWFLAKE_USER` / `SNOWFLAKE_PASSWORD` | required | auth |
| `SNOWFLAKE_WAREHOUSE` | required | compute |
| `SNOWFLAKE_ROLE` | `SYSADMIN` | role |
| `SNOWFLAKE_DATABASE` | `SF_URBAN_HEALTH` | database |
| `SNOWFLAKE_SCHEMA` | `prod` | default schema (per-mart schemas below override this) |
| `DASHBOARD_MART_SCHEMA` | `MARTS` | schema holding the dbt marts |
| `DASHBOARD_METADATA_SCHEMA` | `METADATA` | schema holding observability marts |
| `DASHBOARD_MAX_ROWS` | `200000` | startup safety guard — fails loudly if a mart balloons |
| `DASH_DEBUG` | unset | set to `1` for dev-tools |

## Hosting

The dashboard is **not** currently deployed. Decision deferred — see
[docs/DEPLOY.md](https://github.com/Ukiah-Heasley/sf-urban-health/blob/main/docs/DEPLOY.md)
for the candidate hosting options.
