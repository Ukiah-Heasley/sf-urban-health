"""Polars helpers for the dashboard callback.

The MART singleton in cache.py is the source of truth. These helpers
slice it (apply_filters), build period-over-period KPIs, and add a
rolling-average column for the trend chart.
"""

from __future__ import annotations

from datetime import date

import polars as pl


def apply_filters(
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


def trailing_window(df: pl.DataFrame, end: date, months: int) -> pl.DataFrame:
    """Rows in [end - months, end]. Inclusive on both sides."""
    start_expr = pl.lit(end).cast(pl.Date).dt.offset_by(f"-{months}mo")
    return df.filter(
        (pl.col("filed_month") > start_expr) & (pl.col("filed_month") <= end)
    )


def kpi_summary(
    df_current: pl.DataFrame,
    df_prior: pl.DataFrame,
) -> dict[str, tuple[float | None, float | None]]:
    """Return {metric: (current_value, yoy_delta_pct)} for the 4 KPIs."""

    def agg(d: pl.DataFrame) -> dict[str, float | None]:
        if d.is_empty():
            return {
                "net_units": 0.0,
                "permits_filed": 0.0,
                "project_cost": 0.0,
                "median_days": None,
            }
        return {
            "net_units": float(d["net_units_added"].sum() or 0),
            "permits_filed": float(d["permits_filed"].sum() or 0),
            "project_cost": float(d["total_project_cost"].sum() or 0),
            "median_days": (
                float(d["median_days_to_issue"].drop_nulls().median())
                if d["median_days_to_issue"].drop_nulls().len() > 0
                else None
            ),
        }

    cur, prior = agg(df_current), agg(df_prior)

    def delta(c: float | None, p: float | None) -> float | None:
        if c is None or p is None or p == 0:
            return None
        return (c - p) / p * 100

    return {
        k: (cur[k], delta(cur[k], prior[k]))
        for k in ("net_units", "permits_filed", "project_cost", "median_days")
    }


def monthly_net_units(df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate to month level (sums across neighborhoods/districts)."""
    return (
        df.group_by("filed_month")
        .agg(pl.col("net_units_added").sum())
        .sort("filed_month")
    )


def with_rolling_avg(
    monthly: pl.DataFrame, value_col: str, window: int = 12
) -> pl.DataFrame:
    return monthly.sort("filed_month").with_columns(
        pl.col(value_col)
        .rolling_mean(window_size=window, min_periods=1)
        .alias(f"{value_col}_rolling")
    )


def use_transition_monthly(df: pl.DataFrame) -> pl.DataFrame:
    """Monthly net units broken down by use_transition category."""
    return (
        df.filter(pl.col("use_transition").is_not_null())
        .group_by(["filed_month", "use_transition"])
        .agg(pl.col("net_units_added").sum())
        .sort(["filed_month", "use_transition"])
    )


def cost_per_unit_neighborhoods(df: pl.DataFrame, n: int = 15) -> pl.DataFrame:
    """Top N neighborhoods by new-residential volume, with avg cost per unit."""
    return (
        df.filter(pl.col("use_transition") == "new_residential")
        .filter(pl.col("avg_cost_per_unit").is_not_null())
        .group_by("neighborhood")
        .agg(
            pl.col("net_units_added").sum().alias("total_units"),
            (
                (pl.col("avg_cost_per_unit") * pl.col("permits_filed")).sum()
                / pl.col("permits_filed").sum()
            ).alias("avg_cost_per_unit"),
        )
        .filter(pl.col("total_units") > 0)
        .sort("total_units", descending=True)
        .head(n)
        .sort("avg_cost_per_unit")
    )


def pipeline_by_age(df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate pipeline by lifecycle_stage and age_bucket."""
    return (
        df.group_by(["lifecycle_stage", "age_bucket"])
        .agg(pl.col("permit_count").sum())
        .sort(["lifecycle_stage", "age_bucket"])
    )
