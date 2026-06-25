"""Shared Airflow assets emitted by ingestion and consumed by dbt transforms."""
from __future__ import annotations

from airflow.sdk import Asset

PERMITS_INGEST_ASSET = Asset("sf-urban-health://ingest/permits")
EVICTIONS_INGEST_ASSET = Asset("sf-urban-health://ingest/evictions")
INCIDENTS_INGEST_ASSET = Asset("sf-urban-health://ingest/incidents")

PERMITS_BRONZE_ASSET = Asset("sf-urban-health://lakehouse/bronze/permits")
EVICTIONS_BRONZE_ASSET = Asset("sf-urban-health://lakehouse/bronze/evictions")
INCIDENTS_BRONZE_ASSET = Asset("sf-urban-health://lakehouse/bronze/incidents")

INGEST_ASSETS = {
    "permits": PERMITS_INGEST_ASSET,
    "evictions": EVICTIONS_INGEST_ASSET,
    "incidents": INCIDENTS_INGEST_ASSET,
}

BRONZE_ASSETS = {
    "permits": PERMITS_BRONZE_ASSET,
    "evictions": EVICTIONS_BRONZE_ASSET,
    "incidents": INCIDENTS_BRONZE_ASSET,
}

LAKEHOUSE_TRANSFORM_ASSET = Asset("sf-urban-health://lakehouse/transform_complete")


def ingest_asset_for(dataset_name: str) -> Asset:
    return INGEST_ASSETS[dataset_name]


def bronze_asset_for(dataset_name: str) -> Asset:
    return BRONZE_ASSETS[dataset_name]
