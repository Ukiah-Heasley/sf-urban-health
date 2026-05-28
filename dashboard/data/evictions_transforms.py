"""Polars helpers for the Evictions dashboard page."""

from __future__ import annotations

from datetime import date

import polars as pl


def apply_eviction_filters(
    mart: pl.DataFrame,
    start: date | None,
    end: date | None,
    neighborhoods: list[str] | None,
) -> pl.DataFrame:
    df = mart
    if start is not None:
        df = df.filter(pl.col("filed_month") >= start)
    if end is not None:
        df = df.filter(pl.col("filed_month") <= end)
    if neighborhoods:
        df = df.filter(pl.col("neighborhood").is_in(neighborhoods))
    return df


def eviction_kpi_summary(df: pl.DataFrame) -> dict:
    if df.is_empty():
        return {"total": 0, "no_fault_pct": None, "ellis_act": 0}

    total = int(df["eviction_count"].sum())
    no_fault = int(df.filter(pl.col("eviction_type") == "no_fault")["eviction_count"].sum())
    ellis = int(df["ellis_act_count"].sum())
    no_fault_pct = no_fault / total * 100 if total else None

    return {"total": total, "no_fault_pct": no_fault_pct, "ellis_act": ellis}


def monthly_eviction_trend(
    evictions: pl.DataFrame,
    housing: pl.DataFrame,
) -> pl.DataFrame:
    """Returns a combined monthly frame with eviction_count and net_units_added."""
    ev = (
        evictions.group_by("filed_month")
        .agg(pl.col("eviction_count").sum())
        .sort("filed_month")
    )
    hu = (
        housing.group_by("filed_month")
        .agg(pl.col("net_units_added").sum())
        .sort("filed_month")
    )
    return ev.join(hu, on="filed_month", how="outer_coalesce").sort("filed_month")


def eviction_type_trend(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by(["filed_month", "eviction_type"])
        .agg(pl.col("eviction_count").sum())
        .sort(["filed_month", "eviction_type"])
    )


def top_eviction_neighborhoods(df: pl.DataFrame, n: int = 15) -> pl.DataFrame:
    return (
        df.group_by("neighborhood")
        .agg(pl.col("eviction_count").sum())
        .sort("eviction_count", descending=True)
        .head(n)
        .sort("eviction_count")
    )
