# Dashboard

Plotly Dash app that reads `mart_housing_production` from Snowflake into an
in-memory Polars DataFrame and serves it to a browser.

## Run locally

From the repo root:

```bash
make dashboard-dev          # http://localhost:8050
```

This loads `airflow/.env`, installs the `dashboard` dependency group via uv,
and runs `python -m dashboard.app`.

## Run in Docker

```bash
make dashboard-docker
docker run --env-file airflow/.env -p 8050:8050 sf-urban-health-dashboard
```

## Config

All credentials come from `airflow/.env`. The dashboard adds one optional
variable:

| Var | Default | Purpose |
| --- | --- | --- |
| `DASHBOARD_MART_SCHEMA` | `MARTS` | Schema holding the dbt marts |
| `DASHBOARD_MAX_ROWS` | `100000` | Safety guard at startup load |

## Layout

```
dashboard/
├── app.py            # Dash entrypoint; exposes `server` for gunicorn
├── data/
│   ├── snowflake.py  # connector + query_arrow()
│   └── cache.py      # MART singleton, loaded once at import
├── pages/            # (future) multi-page routes
└── components/       # (future) reusable Dash components
```
