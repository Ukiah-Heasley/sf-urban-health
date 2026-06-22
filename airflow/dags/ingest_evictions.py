"""Daily raw ingestion of SF eviction notices.

Input:    DataSF SODA API, queried by Airflow data interval.
Output:   Raw NDJSON in s3://$AWS_S3_BUCKET/raw/evictions/... plus ingest asset.
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG  # noqa: F401 - required for Airflow 3 DAG file discovery
from dag_factory import DagConfig, make_ingest_dag
from scripts.evictions import EVICTIONS_CONFIG

dag = make_ingest_dag(DagConfig(
    dataset=EVICTIONS_CONFIG,
    schedule="0 6 * * *",
    start_date=datetime(2026, 5, 1),
    tags=["sf-civic", "evictions", "daily"],
))
