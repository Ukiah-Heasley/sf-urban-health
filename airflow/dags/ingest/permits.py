"""Daily raw ingestion of SF building permits.

Input:    DataSF SODA API, queried by Airflow data interval.
Output:   Raw NDJSON in s3://$AWS_S3_BUCKET/raw/permits/... plus ingest asset.
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG  # noqa: F401 - required for Airflow 3 DAG file discovery
from _shared.dag_factory import DagConfig, make_ingest_dag
from scripts.permits import PERMITS_CONFIG

dag = make_ingest_dag(DagConfig(
    dataset=PERMITS_CONFIG,
    schedule="0 6 * * *",
    start_date=datetime(2026, 3, 24),
    tags=["sf-civic", "permits", "daily"],
))
