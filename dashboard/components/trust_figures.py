"""Plotly figure builders for the Data Trust dashboard page."""
from __future__ import annotations

import plotly.graph_objects as go
import polars as pl

from dashboard.components import theme_utils as tu
from dashboard.data.trust_transforms import DATASET_ORDER


def _empty(msg: str, height: int = 300, template: str = "terminal_amber") -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        **tu.base_layout(template), height=height,
        annotations=[dict(
            text=msg, xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=14, color=tu.empty_color(template)),
        )],
        xaxis=dict(visible=False), yaxis=dict(visible=False),
    )
    return fig


def score_donut(
    score: int,
    status: str,
    template: str = "terminal_amber",
) -> go.Figure:
    """Small doughnut chart showing the trust score (0-100)."""
    if status == "trusted":
        color = tu.ok_color(template)
    elif status == "degraded":
        color = tu.warn_color(template)
    else:
        color = tu.bad_color(template)

    bg = "#E0D8C4" if template == "cal_light" else "#2A1A00"

    fig = go.Figure(go.Pie(
        values=[score, 100 - score],
        hole=0.72,
        marker_colors=[color, bg],
        textinfo="none",
        hoverinfo="skip",
        showlegend=False,
        sort=False,
    ))
    fig.update_layout(
        **tu.base_layout(template, margin=dict(l=0, r=0, t=0, b=0)),
        height=130, width=130,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
    )
    return fig


def freshness_calendar(
    grid_df: pl.DataFrame,
    template: str = "terminal_amber",
) -> go.Figure:
    """Heatmap — rows = dataset, columns = date, color = success_rate_pct."""
    if grid_df.is_empty():
        return _empty("No freshness data available", 220, template)

    datasets = DATASET_ORDER
    dates = sorted(grid_df["run_date"].unique().to_list())
    if not dates:
        return _empty("No freshness data available", 220, template)

    # Build z matrix: rows=datasets, cols=dates
    z = []
    for ds in datasets:
        row = []
        ds_data = grid_df.filter(pl.col("dataset_name") == ds)
        date_map = {
            r["run_date"]: r["success_rate_pct"]
            for r in ds_data.iter_rows(named=True)
        }
        for d in dates:
            row.append(date_map.get(d, None))
        z.append(row)

    x_labels = [str(d)[5:] for d in dates]  # MM-DD

    if template == "cal_light":
        colorscale = [[0, "#fecaca"], [0.5, "#fde68a"], [1, "#bbf7d0"]]
    else:
        colorscale = [[0, "#3d0a0a"], [0.5, "#3d2600"], [1, "#0a3d1e"]]

    fig = go.Figure(go.Heatmap(
        z=z, x=x_labels, y=datasets,
        colorscale=colorscale,
        zmin=0, zmax=100,
        showscale=False,
        hovertemplate="%{y}<br>%{x}<br>%{z:.0f}% success<extra></extra>",
        xgap=3, ygap=3,
    ))
    fig.update_layout(
        **tu.base_layout(template, margin=dict(l=80, r=10, t=40, b=40)),
        title="Load Freshness — Last 14 Days",
        height=220,
        xaxis=dict(side="bottom", tickfont=dict(size=10)),
        yaxis=dict(tickfont=dict(size=11)),
    )
    return fig


def test_trend_line(
    trend_df: pl.DataFrame,
    template: str = "terminal_amber",
) -> go.Figure:
    """Line chart of 7d rolling test pass rate per dataset."""
    if trend_df.is_empty():
        return _empty("No test trend data available", template=template)

    pal = tu.palette(template)
    datasets = [d for d in DATASET_ORDER if d in trend_df["dataset_name"].unique().to_list()]
    fig = go.Figure()
    for i, ds in enumerate(datasets):
        subset = trend_df.filter(pl.col("dataset_name") == ds).sort("run_date")
        fig.add_trace(go.Scatter(
            x=subset["run_date"].to_list(),
            y=subset["pass_rate_pct"].to_list(),
            name=ds, mode="lines",
            line=dict(color=pal[i], width=2.5),
            hovertemplate=f"{ds}<br>%{{x}}<br>%{{y:.1f}}% pass<extra></extra>",
        ))

    fig.update_layout(
        **tu.base_layout(template),
        title="Test Pass Rate — Last 30 Days",
        height=230,
        yaxis=dict(
            title="Pass %", range=[74, 102],
            gridcolor=tu.grid_color(template),
        ),
        xaxis=dict(title=None),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def failure_table(
    failures_df: pl.DataFrame,
    template: str = "terminal_amber",
) -> go.Figure:
    """Table of recent test failures across datasets."""
    if failures_df.is_empty():
        return _empty("No recent test failures ✓", 160, template)

    hdr = tu.table_header(template)
    cells = tu.table_cells(template)

    fig = go.Figure(go.Table(
        columnwidth=[1.2, 1.5, 1.5, 1.5, 1.2, 0.8],
        header=dict(
            values=["Dataset", "Model", "Test", "Column", "Failed At", "Rows"],
            fill_color=hdr["fill_color"],
            font=dict(color=hdr["font_color"], size=11),
            align="left", line_color=hdr["line_color"],
        ),
        cells=dict(
            values=[
                failures_df["dataset_name"].to_list(),
                failures_df["model_name"].to_list(),
                failures_df["test_name"].to_list(),
                [c or "—" for c in failures_df["column_name"].to_list()],
                failures_df["run_date"].cast(pl.Utf8).to_list(),
                failures_df["failures"].to_list(),
            ],
            fill_color=cells["fill_color"],
            font=dict(color=cells["font_color"], size=10),
            align="left", line_color=cells["line_color"],
        ),
    ))
    fig.update_layout(
        **tu.base_layout(template, margin=dict(l=10, r=10, t=40, b=10)),
        title="Recent Test Failures — Last 7 Days",
        height=max(180, 70 + len(failures_df) * 30),
    )
    return fig
