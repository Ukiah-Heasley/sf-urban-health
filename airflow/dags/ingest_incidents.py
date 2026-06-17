"""Daily ingestion of SF Police Department incident reports into the warehouse.

Extract:   DataSF SODA API (data_loaded_at lookback)
Load:      S3 raw layer -> Snowflake RAW.INCIDENTS via COPY INTO
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG  # noqa: F401 - required for Airflow 3 DAG file discovery
from dag_factory import DagConfig, make_ingest_dag
from scripts.incident_reports import INCIDENTS_CONFIG

dag = make_ingest_dag(DagConfig(
    dataset=INCIDENTS_CONFIG,
    snowflake_table="RAW.INCIDENTS",
    schedule="0 6 * * *",
    start_date=datetime(2026, 4, 30),
    tags=["sf-civic", "incidents", "daily"],
))
