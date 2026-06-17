"""Shared Airflow assets emitted by ingestion and consumed by dbt transforms."""
from __future__ import annotations

from airflow.sdk import Asset

PERMITS_INGEST_ASSET = Asset("sf-urban-health://ingest/permits")
EVICTIONS_INGEST_ASSET = Asset("sf-urban-health://ingest/evictions")
INCIDENTS_INGEST_ASSET = Asset("sf-urban-health://ingest/incidents")

INGEST_ASSETS = {
    "permits": PERMITS_INGEST_ASSET,
    "evictions": EVICTIONS_INGEST_ASSET,
    "incidents": INCIDENTS_INGEST_ASSET,
}


def ingest_asset_for(dataset_name: str) -> Asset:
    return INGEST_ASSETS[dataset_name]
