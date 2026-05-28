# Dashboard

Plotly Dash app that reads the dbt marts from Snowflake into in-memory
Polars DataFrames at boot and serves six pages from memory.

## Run locally

From the repo root:

```bash
make dashboard-dev          # http://localhost:8050
```

This loads `airflow/.env`, installs the `dashboard` dependency group via uv,
and runs `python -m dashboard.app`. Set `DASH_DEBUG=1` if you want the
interactive traceback / dev tools (off by default).

## Run in Docker

```bash
make dashboard-docker
docker run --env-file airflow/.env -p 8050:8050 sf-urban-health-dashboard
```

## Config

All credentials come from `airflow/.env`. The dashboard adds these optional
variables on top:

| Var | Default | Purpose |
| --- | --- | --- |
| `DASHBOARD_MART_SCHEMA`     | `MARTS`    | Schema holding the dbt marts |
| `DASHBOARD_METADATA_SCHEMA` | `METADATA` | Schema holding the observability marts |
| `DASHBOARD_MAX_ROWS`        | `200000`   | Safety guard at startup load |
| `DASH_DEBUG`                | unset      | Set to `1` to enable Dash dev tools |

## Layout

```
dashboard/
├── app.py            # Dash entrypoint; exposes `server` for gunicorn
├── data/             # Snowflake client, in-memory mart cache, transform helpers
├── pages/            # 6 multi-page routes (housing, incidents, evictions, pipeline, engineer, data trust)
├── components/       # Plotly figure builders, KPI cards, theme utilities
└── assets/           # Static SVG + theme CSS served by Dash
```
