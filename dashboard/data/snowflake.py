"""Thin Snowflake client for the dashboard.

Reads credentials from the same environment variables already used by
Airflow and dbt (see airflow/.env.example), so no new secrets to manage.
Returns Arrow tables — the zero-copy bridge into Polars.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

import pyarrow as pa
import snowflake.connector


def _required(var: str) -> str:
    value = os.environ.get(var)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {var}. "
            "Source airflow/.env or set it in your shell."
        )
    return value


@contextmanager
def connect() -> Iterator[snowflake.connector.SnowflakeConnection]:
    conn = snowflake.connector.connect(
        account=_required("SNOWFLAKE_ACCOUNT"),
        user=_required("SNOWFLAKE_USER"),
        password=_required("SNOWFLAKE_PASSWORD"),
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "SF_URBAN_HEALTH"),
        warehouse=_required("SNOWFLAKE_WAREHOUSE"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "prod"),
    )
    try:
        yield conn
    finally:
        conn.close()


def query_arrow(sql: str) -> pa.Table:
    """Run a query and return the result as a PyArrow table."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetch_arrow_all()
