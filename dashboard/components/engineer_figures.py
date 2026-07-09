"""Plotly figure builders for the Engineer Health dashboard page."""

from __future__ import annotations

from datetime import date, timedelta

import plotly.graph_objects as go
import polars as pl

from dashboard.components import theme_utils as tu


def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    """Convert a 6-digit hex color to an rgba() string Plotly accepts."""
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


_DAG_LABELS = {
    "ingest_permits": "Permits",
    "ingest_evictions": "Evictions",
    "ingest_incidents": "Incidents",
}
_INGEST_DAGS = list(_DAG_LABELS.keys())


def _empty(msg: str, height: int = 380, template: str = "terminal_amber") -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        **tu.base_layout(template),
        height=height,
        annotations=[
            dict(
                text=msg,
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.5,
                showarrow=False,
                font=dict(size=14, color=tu.empty_color(template)),
            )
        ],
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )
    return fig


def dag_success_rate_line(
    pipeline_health: pl.DataFrame,
    days: int = 30,
    template: str = "terminal_amber",
) -> go.Figure:
    """Line chart of daily success rate per ingest DAG over the last *days* days."""
    if pipeline_health.is_empty():
        return _empty("No pipeline run data available", template=template)

    cutoff = date.today() - timedelta(days=days - 1)
    data = pipeline_health.filter(
        (pl.col("run_date") >= cutoff) & pl.col("dag_id").is_in(_INGEST_DAGS)
    ).sort("run_date")
    if data.is_empty():
        return _empty("No data in the selected window", template=template)

    pal = tu.palette(template)
    fig = go.Figure()
    for i, dag_id in enumerate(_INGEST_DAGS):
        subset = data.filter(pl.col("dag_id") == dag_id)
        if subset.is_empty():
            continue
        # Mark failure days with a visible dot
        failure_mask = subset["success_rate_pct"] < 100
        fig.add_trace(
            go.Scatter(
                x=subset["run_date"].to_list(),
                y=subset["success_rate_pct"].to_list(),
                name=_DAG_LABELS[dag_id],
                mode="lines+markers",
                line=dict(color=pal[i], width=2.5),
                marker=dict(
                    size=[6 if f else 0 for f in failure_mask.to_list()],
                    color=pal[i],
                    symbol="circle",
                ),
                hovertemplate=f"{_DAG_LABELS[dag_id]}<br>%{{x}}<br>%{{y:.0f}}%<extra></extra>",
            )
        )

    fig.update_layout(
        **tu.base_layout(template),
        title=f"DAG Success Rate — Last {days} Days",
        height=280,
        yaxis=dict(
            title="Success %", range=[-5, 110], gridcolor=tu.grid_color(template)
        ),
        xaxis=dict(title=None, gridcolor=tu.grid_color(template)),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def runtime_trend_line(
    pipeline_health: pl.DataFrame,
    days: int = 30,
    template: str = "terminal_amber",
) -> go.Figure:
    """Line chart of avg runtime per ingest DAG over the last *days* days."""
    if pipeline_health.is_empty():
        return _empty("No runtime data available", template=template)

    cutoff = date.today() - timedelta(days=days - 1)
    data = pipeline_health.filter(
        (pl.col("run_date") >= cutoff)
        & pl.col("dag_id").is_in(_INGEST_DAGS)
        & pl.col("avg_duration_seconds").is_not_null()
    ).sort("run_date")
    if data.is_empty():
        return _empty("No runtime data in the selected window", template=template)

    pal = tu.palette(template)
    fig = go.Figure()
    for i, dag_id in enumerate(_INGEST_DAGS):
        subset = data.filter(pl.col("dag_id") == dag_id)
        if subset.is_empty():
            continue
        minutes = (subset["avg_duration_seconds"] / 60).round(2).to_list()
        fig.add_trace(
            go.Scatter(
                x=subset["run_date"].to_list(),
                y=minutes,
                name=_DAG_LABELS[dag_id],
                mode="lines",
                line=dict(color=pal[i], width=2.5),
                fill="tozeroy" if i == 0 else None,
                fillcolor=pal[i] + "18",
                hovertemplate=f"{_DAG_LABELS[dag_id]}<br>%{{x}}<br>%{{y:.1f}} min<extra></extra>",
            )
        )

    fig.update_layout(
        **tu.base_layout(template),
        title=f"Avg Runtime Trend — Last {days} Days",
        height=280,
        yaxis=dict(title="Minutes", gridcolor=tu.grid_color(template)),
        xaxis=dict(title=None, gridcolor=tu.grid_color(template)),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def test_volume_bar(
    test_health: pl.DataFrame,
    days: int = 14,
    template: str = "terminal_amber",
) -> go.Figure:
    """Stacked bar of daily pass/fail test counts."""
    if test_health.is_empty():
        return _empty("No test data available", template=template)

    cutoff = date.today() - timedelta(days=days - 1)
    daily = (
        test_health.filter(pl.col("run_date") >= cutoff)
        .group_by("run_date")
        .agg(
            pl.col("is_passing").cast(pl.Int32).sum().alias("passed"),
            (pl.col("is_passing") == False).cast(pl.Int32).sum().alias("failed"),  # noqa: E712
        )
        .sort("run_date")
    )
    if daily.is_empty():
        return _empty("No test data in the selected window", template=template)

    fig = go.Figure()
    fig.add_bar(
        x=daily["run_date"].to_list(),
        y=daily["passed"].to_list(),
        name="Passed",
        marker_color=tu.ok_color(template),
        marker_opacity=0.55,
        marker_line_color=tu.ok_color(template),
        marker_line_width=1,
        hovertemplate="%{x}<br>%{y} passed<extra></extra>",
    )
    fig.add_bar(
        x=daily["run_date"].to_list(),
        y=daily["failed"].to_list(),
        name="Failed",
        marker_color=tu.bad_color(template),
        marker_opacity=0.55,
        marker_line_color=tu.bad_color(template),
        marker_line_width=1,
        hovertemplate="%{x}<br>%{y} failed<extra></extra>",
    )
    fig.update_layout(
        **tu.base_layout(template),
        title=f"Daily Test Results — Last {days} Days",
        height=280,
        barmode="stack",
        xaxis=dict(title=None, gridcolor=tu.grid_color(template)),
        yaxis=dict(title="Tests", gridcolor=tu.grid_color(template)),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def test_pass_rate_table(
    test_health: pl.DataFrame,
    template: str = "terminal_amber",
) -> go.Figure:
    """Table: latest 7d pass rate per (model, test)."""
    if test_health.is_empty():
        return _empty("No test data available", 260, template)

    # Take latest run_date row per (test_name, model_name)
    latest = (
        test_health.sort("run_date", descending=True)
        .group_by(["model_name", "test_name"])
        .first()
        .sort(["model_name", "test_name"])
        .select(["model_name", "test_name", "pass_rate_7d", "last_failure_at"])
    )

    def fmt_fail(v) -> str:
        if v is None:
            return "—"
        return str(v)[:10] if hasattr(v, "__str__") else "—"

    def rate_color(r: float | None) -> str:
        if r is None:
            return tu.empty_color(template)
        if r >= 95:
            return tu.ok_color(template)
        if r >= 80:
            return tu.warn_color(template)
        return tu.bad_color(template)

    rates = latest["pass_rate_7d"].to_list()
    hdr = tu.table_header(template)
    cells = tu.table_cells(template)

    fig = go.Figure(
        go.Table(
            columnwidth=[2, 2, 1, 1.5],
            header=dict(
                values=["Model", "Test", "7d Rate", "Last Failure"],
                fill_color=hdr["fill_color"],
                font=dict(color=hdr["font_color"], size=11),
                align="left",
                line_color=hdr["line_color"],
            ),
            cells=dict(
                values=[
                    latest["model_name"].to_list(),
                    latest["test_name"].to_list(),
                    [f"{r:.1f}%" if r is not None else "—" for r in rates],
                    [fmt_fail(v) for v in latest["last_failure_at"].to_list()],
                ],
                fill_color=[
                    [cells["fill_color"]] * len(latest),
                    [cells["fill_color"]] * len(latest),
                    [_hex_to_rgba(rate_color(r), 0.25) for r in rates],
                    [cells["fill_color"]] * len(latest),
                ],
                font=dict(color=cells["font_color"], size=10),
                align="left",
                line_color=cells["line_color"],
            ),
        )
    )
    fig.update_layout(
        **tu.base_layout(template, margin=dict(l=10, r=10, t=40, b=10)),
        title="Pass Rate by Model",
        height=max(220, 60 + len(latest) * 28),
    )
    return fig
