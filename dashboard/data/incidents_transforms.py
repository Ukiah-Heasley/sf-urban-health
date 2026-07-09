"""Polars helpers for the Public Safety Incidents dashboard page."""

from __future__ import annotations

from datetime import date

import polars as pl


def apply_incident_filters(
    mart: pl.DataFrame,
    start: date | None,
    end: date | None,
    neighborhoods: list[str] | None,
    categories: list[str] | None,
) -> pl.DataFrame:
    df = mart
    if start is not None:
        df = df.filter(pl.col("incident_month") >= start)
    if end is not None:
        df = df.filter(pl.col("incident_month") <= end)
    if neighborhoods:
        df = df.filter(pl.col("neighborhood").is_in(neighborhoods))
    if categories:
        df = df.filter(pl.col("incident_category").is_in(categories))
    return df


def incident_kpi_summary(df: pl.DataFrame) -> dict:
    if df.is_empty():
        return {"total": 0, "top_category": None, "pct_resolved": None}

    total = int(df["total_incidents"].sum())
    resolved = int(df["resolved_count"].sum())
    pct_resolved = resolved / total * 100 if total else None

    top_cat = (
        df.group_by("incident_category")
        .agg(pl.col("total_incidents").sum())
        .sort("total_incidents", descending=True)
        .head(1)["incident_category"]
        .to_list()
    )
    top_category = top_cat[0] if top_cat else None

    return {"total": total, "top_category": top_category, "pct_resolved": pct_resolved}


def monthly_incident_trend(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("incident_month")
        .agg(pl.col("total_incidents").sum())
        .sort("incident_month")
    )


def top_incident_neighborhoods(df: pl.DataFrame, n: int = 15) -> pl.DataFrame:
    return (
        df.group_by("neighborhood")
        .agg(pl.col("total_incidents").sum())
        .sort("total_incidents", descending=True)
        .head(n)
        .sort("total_incidents")  # ascending so largest bar appears at top
    )


def category_breakdown(df: pl.DataFrame, n: int = 20) -> pl.DataFrame:
    return (
        df.group_by("incident_category")
        .agg(pl.col("total_incidents").sum())
        .sort("total_incidents", descending=True)
        .head(n)
        .sort("total_incidents")
    )


def resolution_rate_by_district(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.filter(pl.col("police_district").is_not_null())
        .group_by("police_district")
        .agg(
            pl.col("total_incidents").sum().alias("total"),
            pl.col("resolved_count").sum().alias("resolved"),
        )
        .with_columns((pl.col("resolved") / pl.col("total")).alias("resolution_rate"))
        .sort("police_district")
    )
