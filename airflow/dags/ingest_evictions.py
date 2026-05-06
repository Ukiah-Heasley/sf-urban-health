"""Daily ingestion of SF eviction notices into the warehouse.

Extract:   DataSF SODA API (data_loaded_at lookback — captures new records and updates)
Load:      S3 raw layer -> Snowflake RAW.EVICTIONS via COPY INTO
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG  # noqa: F401 – required for Airflow 3 DAG file discovery
from dag_factory import DagConfig, make_ingest_dag
from scripts.evictions import EVICTIONS_CONFIG

dag = make_ingest_dag(DagConfig(
    dataset=EVICTIONS_CONFIG,
    snowflake_table="RAW.EVICTIONS",
    schedule="0 6 * * *",
    start_date=datetime(2026, 5, 1),
    tags=["sf-civic", "evictions", "daily"],
))
