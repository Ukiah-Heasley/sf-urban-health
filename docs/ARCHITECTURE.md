# Architecture

The full architecture write-up lives in the wiki:

**→ [wiki/Architecture.md](../wiki/Architecture.md)**

This file exists so links from `README.md` to `docs/ARCHITECTURE.md`
stay stable; the wiki page is the working copy.

## TL;DR

```
DataSF SODA API → S3 (NDJSON) → Snowflake RAW → dbt → MARTS / METADATA → Plotly Dash
                       │                                    │
                       └─── orchestrated by 5 Airflow DAGs ─┘
```

- **5 DAGs:** 3 ingest (permits, evictions, incidents) + 1 transform_all + 1 ingest_pipeline_metadata.
- **Watermark-driven incremental** loads; `METADATA.INGEST_WATERMARKS` is the checkpoint of record.
- **dbt:** views (staging, intermediate) → tables (marts), with a separate observability project under `dbt/models/metadata/`.
- **Dashboard:** Plotly Dash, six pages, in-memory Polars caching at boot.

See the wiki for the layered diagram, mart grain table, and the
observability story.
