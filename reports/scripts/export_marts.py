"""Export Snowflake marts to parquet for the Evidence static site.

Run from the repo root (so the ``dashboard`` package is importable):

    uv run --group dashboard python reports/scripts/export_marts.py

Reuses dashboard/data/snowflake.py for the connection (same SNOWFLAKE_* env
vars as the rest of the project). Writes one parquet file per mart into
reports/sources/sf_urban_health/data/, which the Evidence DuckDB source reads
at build time. The scheduled Pages workflow runs this, then `npm run build`.

With no warehouse handy, use reports/scripts/make_sample_data.py instead — it
writes the same files with a small synthetic snapshot so the site still builds.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import polars as pl

from dashboard.data.snowflake import query_arrow

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("export_marts")

_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "SF_URBAN_HEALTH")
_MARTS = os.environ.get("DASHBOARD_MART_SCHEMA", "MARTS")
_META = os.environ.get("DASHBOARD_METADATA_SCHEMA", "METADATA")

OUT_DIR = Path(__file__).resolve().parent.parent / "sources" / "sf_urban_health" / "data"

# mart name -> fully-qualified Snowflake source. Marts in MARTS, observability in METADATA.
MARTS: dict[str, str] = {
    "mart_housing_production": f"{_DATABASE}.{_MARTS}.mart_housing_production",
    "mart_permit_pipeline": f"{_DATABASE}.{_MARTS}.mart_permit_pipeline",
    "mart_public_safety": f"{_DATABASE}.{_MARTS}.mart_public_safety",
    "mart_evictions": f"{_DATABASE}.{_MARTS}.mart_evictions",
    "mart_pipeline_health": f"{_DATABASE}.{_META}.mart_pipeline_health",
    "mart_pipeline_summary": f"{_DATABASE}.{_META}.mart_pipeline_summary",
    "mart_dbt_test_health": f"{_DATABASE}.{_META}.mart_dbt_test_health",
    "mart_data_trust": f"{_DATABASE}.{_META}.mart_data_trust",
}


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    exported = 0
    for name, fqn in MARTS.items():
        try:
            table = query_arrow(f"SELECT * FROM {fqn}")
        except Exception as exc:  # noqa: BLE001 — one bad mart shouldn't sink the export
            logger.warning("skipping %s: %s", name, exc)
            continue
        df = pl.from_arrow(table)
        df = df.rename({c: c.lower() for c in df.columns})  # Snowflake returns upper-case
        out = OUT_DIR / f"{name}.parquet"
        df.write_parquet(out)
        logger.info("wrote %s (%d rows) -> %s", name, df.height, out)
        exported += 1
    logger.info("exported %d/%d marts", exported, len(MARTS))


if __name__ == "__main__":
    main()
