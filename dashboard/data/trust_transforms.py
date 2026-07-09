"""Polars helpers for the Data Trust dashboard page."""
from __future__ import annotations

from datetime import date, timedelta

import polars as pl

_DAG_TO_DATASET = {
    "ingest_permits":   "Permits",
    "ingest_evictions": "Evictions",
    "ingest_incidents": "Incidents",
}

_DATASET_MODELS = {
    "Permits":   "stg_permits",
    "Evictions": "stg_evictions",
    "Incidents": "stg_incidents",
}

DATASET_ORDER = ["Permits", "Evictions", "Incidents"]


def freshness_grid(pipeline_health: pl.DataFrame, days: int = 14) -> pl.DataFrame:
    """14-day × dataset grid for the freshness calendar heatmap.

    Returns columns: dataset_name, run_date, success_rate_pct (0-100 or null).
    """
    if pipeline_health.is_empty():
        return pl.DataFrame()

    cutoff = date.today() - timedelta(days=days - 1)
    recent = pipeline_health.filter(pl.col("run_date") >= cutoff)

    mapped = recent.with_columns(
        pl.col("dag_id").replace(_DAG_TO_DATASET).alias("dataset_name")
    ).filter(pl.col("dataset_name").is_in(DATASET_ORDER))

    return (
        mapped
        .select(["dataset_name", "run_date", "success_rate_pct"])
        .sort(["dataset_name", "run_date"])
    )


def staging_test_trend(test_health: pl.DataFrame, days: int = 30) -> pl.DataFrame:
    """Daily pass rate per staging model over the last *days* days."""
    if test_health.is_empty():
        return pl.DataFrame()

    cutoff = date.today() - timedelta(days=days - 1)
    staging_models = list(_DATASET_MODELS.values())

    filtered = test_health.filter(
        (pl.col("run_date") >= cutoff)
        & pl.col("model_name").is_in(staging_models)
    )
    if filtered.is_empty():
        return pl.DataFrame()

    return (
        filtered
        .group_by(["run_date", "model_name"])
        .agg(
            (pl.col("is_passing").cast(pl.Int32).sum() * 100.0 / pl.len())
            .round(1)
            .alias("pass_rate_pct")
        )
        .with_columns(
            pl.col("model_name")
            .replace({v: k for k, v in _DATASET_MODELS.items()})
            .alias("dataset_name")
        )
        .sort("run_date")
    )


def recent_test_failures(test_health: pl.DataFrame, days: int = 7, limit: int = 20) -> pl.DataFrame:
    """Recent failures across staging models, enriched with dataset name."""
    if test_health.is_empty():
        return pl.DataFrame()

    cutoff = date.today() - timedelta(days=days)
    staging_models = list(_DATASET_MODELS.values())

    failed = test_health.filter(
        (~pl.col("is_passing"))
        & (pl.col("run_date") >= cutoff)
        & pl.col("model_name").is_in(staging_models)
    )
    if failed.is_empty():
        return pl.DataFrame()

    return (
        failed
        .with_columns(
            pl.col("model_name")
            .replace({v: k for k, v in _DATASET_MODELS.items()})
            .alias("dataset_name")
        )
        .sort("run_date", descending=True)
        .select(["dataset_name", "model_name", "test_name", "column_name",
                 "run_date", "failures", "pass_rate_30d"])
        .head(limit)
    )
