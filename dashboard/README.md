# Dashboard

The Plotly Dash application loads eight Snowflake marts into Polars DataFrames
at process startup and serves six interactive pages from memory.

## Run locally

```bash
make dashboard-dev
```

The app listens on <http://localhost:8050>. `DASH_DEBUG=1` enables Dash
development tools; debug mode is off by default.

## Run in Docker

```bash
make dashboard-docker
docker run --env-file airflow/.env -p 8050:8050 sf-urban-health-dashboard
```

Gunicorn serves the `server` object exported by `dashboard.app`.

## Data loading

`dashboard/data/snowflake.py` executes Snowflake queries and returns Arrow
tables. `dashboard/data/cache.py` converts results to Polars, lowercases column
names, and stores one module-level frame per mart.

Connection or query failures produce warning logs and empty frames so the app
can still import. A successful query returning more than
`DASHBOARD_MAX_ROWS` fails startup rather than silently loading an unexpectedly
large mart.

## Pages

| Route | Data |
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
| `SNOWFLAKE_ACCOUNT` | none | required |
| `SNOWFLAKE_USER` | none | required |
| `SNOWFLAKE_PASSWORD` | none | required |
| `SNOWFLAKE_WAREHOUSE` | none | required |
| `SNOWFLAKE_ROLE` | `SYSADMIN` | optional |
| `SNOWFLAKE_DATABASE` | `SF_URBAN_HEALTH` | optional |
| `SNOWFLAKE_SCHEMA` | `prod` | optional connection default |
| `DASHBOARD_MART_SCHEMA` | `MARTS` | analytical marts |
| `DASHBOARD_METADATA_SCHEMA` | `METADATA` | observability marts |
| `DASHBOARD_MAX_ROWS` | `200000` | per-mart startup limit |
| `DASH_DEBUG` | unset | set to `1` for development tools |

## Layout

```text
dashboard/
  app.py          Dash entry point and WSGI server
  data/           Snowflake client, startup cache, Polars transforms
  pages/          six route modules
  components/     figure builders, KPI cards, theme helpers
  assets/         Dash-served CSS and SVG
```
