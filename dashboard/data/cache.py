"""In-memory cache of mart-shaped tables for the dashboard shell.

Live warehouse loading is disabled during the lakehouse rebuild. Module-level
frames start empty so pages render their empty-state layouts without credentials.
"""

from __future__ import annotations

import polars as pl

MART: pl.DataFrame = pl.DataFrame()
PIPELINE: pl.DataFrame = pl.DataFrame()
SAFETY_MART: pl.DataFrame = pl.DataFrame()
EVICTIONS: pl.DataFrame = pl.DataFrame()
PIPELINE_HEALTH: pl.DataFrame = pl.DataFrame()
DBT_TEST_HEALTH: pl.DataFrame = pl.DataFrame()
PIPELINE_SUMMARY: pl.DataFrame = pl.DataFrame()
DATA_TRUST: pl.DataFrame = pl.DataFrame()
