# Dashboard

The Plotly Dash application keeps six interactive pages as a consumer shell.
Live warehouse loading is disabled during the lakehouse rebuild; startup cache
calls fail closed to empty Polars frames.

## Run locally

```bash
make dashboard-dev
```

The app listens on <http://localhost:8050>. `DASH_DEBUG=1` enables Dash
development tools; debug mode is off by default.

## Run in Docker

```bash
make dashboard-docker
docker run -p 8050:8050 sf-urban-health-dashboard
```

Gunicorn serves the `server` object exported by `dashboard.app`.

## Data loading

`dashboard/data/cache.py` attempts to load one module-level Polars frame per
mart-shaped table. Connection or query failures produce warning logs and empty
frames so the app can still import. A successful query returning more than
`DASHBOARD_MAX_ROWS` fails startup rather than silently loading an unexpectedly
large frame.

## Pages

| Route | Intended data |
| --- | --- |
| `/` | housing production |
| `/incidents` | public safety |
| `/evictions` | evictions |
| `/pipeline` | pipeline and dbt test health |
| `/engineer` | pipeline summary and engineering health |
| `/data-trust` | trust, freshness, and test health |

## Configuration

| Variable | Default | Requirement |
| --- | --- | --- |
| `DASHBOARD_MAX_ROWS` | `200000` | per-mart startup limit |
| `DASH_DEBUG` | unset | set to `1` for development tools |

## Layout

```text
dashboard/
  app.py          Dash entry point and WSGI server
  data/           startup cache and Polars transforms
  pages/          six route modules
  components/     figure builders, KPI cards, theme helpers
  assets/         Dash-served CSS and SVG
```
