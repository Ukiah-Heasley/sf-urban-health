"""Daily ingestion of SF building permits into the warehouse.

Extract:   DataSF SODA API (data_loaded_at lookback)
Load:      S3 raw layer -> Snowflake RAW.PERMITS via COPY INTO
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG  # noqa: F401 – required for Airflow 3 DAG file discovery
from dag_factory import DagConfig, make_ingest_dag
from scripts.permits import PERMITS_CONFIG

dag = make_ingest_dag(DagConfig(
    dataset=PERMITS_CONFIG,
    snowflake_table="RAW.PERMITS",
    schedule="0 6 * * *",
    start_date=datetime(2026, 3, 24),
    tags=["sf-civic", "permits", "daily"],
))
