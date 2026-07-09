"""Transform helpers for the pipeline health dashboard page."""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl


def _last_n_days(df: pl.DataFrame, date_col: str, n: int) -> pl.DataFrame:
    cutoff = date.today() - timedelta(days=n)
    return df.filter(pl.col(date_col) >= cutoff)


def pipeline_kpis(pipeline: pl.DataFrame, tests: pl.DataFrame, days: int = 7) -> dict:
    if pipeline.is_empty():
        return {
            "success_rate_pct": None,
            "avg_duration_seconds": None,
            "total_records_ingested": None,
            "test_pass_rate_pct": None,
        }

    recent = _last_n_days(pipeline, "run_date", days)
    total = recent["total_runs"].sum() or 0
    success = recent["successful_runs"].sum() or 0
    success_rate = round(success * 100.0 / total, 1) if total else None

    avg_dur = recent["avg_duration_seconds"].mean()
    avg_dur = round(avg_dur, 0) if avg_dur is not None else None

    records = recent["total_records_ingested"].sum()
    records = int(records) if records is not None else 0

    test_pass_rate = None
    if not tests.is_empty():
        recent_tests = _last_n_days(tests, "run_date", days)
        if not recent_tests.is_empty():
            passing = recent_tests.filter(pl.col("is_passing"))["test_name"].n_unique()
            total_tests = recent_tests["test_name"].n_unique()
            test_pass_rate = (
                round(passing * 100.0 / total_tests, 1) if total_tests else None
            )

    return {
        "success_rate_pct": success_rate,
        "avg_duration_seconds": avg_dur,
        "total_records_ingested": records,
        "test_pass_rate_pct": test_pass_rate,
    }


def dag_run_timeline(pipeline: pl.DataFrame) -> pl.DataFrame:
    """Rows: run_date × dag_id with success/failed counts for stacked bar."""
    if pipeline.is_empty():
        return pl.DataFrame()
    return pipeline.select(
        ["run_date", "dag_id", "successful_runs", "failed_runs"]
    ).sort("run_date")


def dag_avg_duration(pipeline: pl.DataFrame) -> pl.DataFrame:
    """One row per dag_id with avg duration — for horizontal bar."""
    if pipeline.is_empty():
        return pl.DataFrame()
    return (
        pipeline.group_by("dag_id")
        .agg(pl.col("avg_duration_seconds").mean().alias("avg_duration_seconds"))
        .sort("avg_duration_seconds", descending=True)
    )


def test_pass_rate_trend(tests: pl.DataFrame) -> pl.DataFrame:
    """Daily pass rate per model — for line chart."""
    if tests.is_empty():
        return pl.DataFrame()
    return (
        tests.group_by(["run_date", "model_name"])
        .agg(
            (pl.col("is_passing").cast(pl.Int32).sum() * 100.0 / pl.len())
            .round(1)
            .alias("pass_rate_pct")
        )
        .sort("run_date")
    )


def failing_tests(tests: pl.DataFrame, limit: int = 20) -> pl.DataFrame:
    """Most recently failed tests — for the failures table."""
    if tests.is_empty():
        return pl.DataFrame()
    failed = tests.filter(~pl.col("is_passing"))
    if failed.is_empty():
        return pl.DataFrame()
    return (
        failed.sort("run_date", descending=True)
        .select(
            [
                "run_date",
                "test_name",
                "model_name",
                "column_name",
                "test_type",
                "failures",
                "pass_rate_30d",
            ]
        )
        .head(limit)
    )
