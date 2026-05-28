"""In-memory cache of the housing production mart.

The mart is small by design (monthly grain × ~40 neighborhoods × ~11
districts × ~13 years of history ≈ a few thousand rows). We load it
once at app startup into a Polars DataFrame and serve all callbacks
from memory.

MAX_ROWS is a safety guard: if the mart unexpectedly balloons (e.g. a
broken join in dbt), we'd rather fail loudly at boot than silently
chew through memory.
"""

from __future__ import annotations

import logging
import os

import polars as pl

from dashboard.data.snowflake import query_arrow

logger = logging.getLogger(__name__)

MAX_ROWS = int(os.environ.get("DASHBOARD_MAX_ROWS", "200000"))

_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "SF_URBAN_HEALTH")
_SCHEMA   = os.environ.get("DASHBOARD_MART_SCHEMA", "MARTS")
_META     = os.environ.get("DASHBOARD_METADATA_SCHEMA", "METADATA")

_MART_FQN             = f"{_DATABASE}.{_SCHEMA}.mart_housing_production"
_PIPELINE_FQN         = f"{_DATABASE}.{_SCHEMA}.mart_permit_pipeline"
_SAFETY_FQN           = f"{_DATABASE}.{_SCHEMA}.mart_public_safety"
_EVICTIONS_FQN        = f"{_DATABASE}.{_SCHEMA}.mart_evictions"
# Pipeline observability marts live in the METADATA schema
_PIPELINE_HEALTH_FQN  = f"{_DATABASE}.{_META}.mart_pipeline_health"
_DBT_TEST_HEALTH_FQN  = f"{_DATABASE}.{_META}.mart_dbt_test_health"
_PIPELINE_SUMMARY_FQN = f"{_DATABASE}.{_META}.mart_pipeline_summary"
_DATA_TRUST_FQN       = f"{_DATABASE}.{_META}.mart_data_trust"


def _load(fqn: str) -> pl.DataFrame:
    try:
        table = query_arrow(f"SELECT * FROM {fqn}")
    except Exception as exc:
        logger.warning("could not load %s: %s", fqn, exc)
        return pl.DataFrame()
    if table is None:
        return pl.DataFrame()
    if table.num_rows > MAX_ROWS:
        raise RuntimeError(
            f"{fqn} returned {table.num_rows:,} rows, exceeding "
            f"DASHBOARD_MAX_ROWS={MAX_ROWS:,}. Investigate the mart "
            "before raising the limit."
        )
    df = pl.from_arrow(table)
    return df.rename({c: c.lower() for c in df.columns})


MART: pl.DataFrame             = _load(_MART_FQN)
PIPELINE: pl.DataFrame         = _load(_PIPELINE_FQN)
SAFETY_MART: pl.DataFrame      = _load(_SAFETY_FQN)
EVICTIONS: pl.DataFrame        = _load(_EVICTIONS_FQN)
PIPELINE_HEALTH: pl.DataFrame  = _load(_PIPELINE_HEALTH_FQN)
DBT_TEST_HEALTH: pl.DataFrame  = _load(_DBT_TEST_HEALTH_FQN)
PIPELINE_SUMMARY: pl.DataFrame = _load(_PIPELINE_SUMMARY_FQN)
DATA_TRUST: pl.DataFrame       = _load(_DATA_TRUST_FQN)
