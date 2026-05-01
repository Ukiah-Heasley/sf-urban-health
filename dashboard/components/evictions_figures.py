"""Chart builders for the Evictions dashboard page."""

from __future__ import annotations

import plotly.graph_objects as go
import polars as pl

from dashboard.data.evictions_transforms import (
    eviction_type_trend,
    monthly_eviction_trend,
    top_eviction_neighborhoods,
)

_PALETTE = {"no_fault": "#C0392B", "at_fault": "#E67E22"}


def _empty(msg: str, height: int = 400) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(text=msg, xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
    fig.update_layout(height=height, xaxis_visible=False, yaxis_visible=False)
    return fig


def evictions_vs_units(evictions: pl.DataFrame, housing: pl.DataFrame) -> go.Figure:
    """Dual-axis line chart: monthly eviction count vs net units added."""
    if evictions.is_empty():
        return _empty("No eviction data available")

    data = monthly_eviction_trend(evictions, housing)

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=data["filed_month"].to_list(),
        y=data["eviction_count"].to_list(),
        name="Eviction notices",
        line={"color": "#C0392B", "width": 2},
        yaxis="y1",
    ))

    if "net_units_added" in data.columns:
        fig.add_trace(go.Scatter(
            x=data["filed_month"].to_list(),
            y=data["net_units_added"].to_list(),
            name="Net units added",
            line={"color": "#2471A3", "width": 2, "dash": "dot"},
            yaxis="y2",
        ))

    fig.update_layout(
        title="Eviction Notices vs. New Housing Units — Monthly",
        xaxis={"title": "Month"},
        yaxis={"title": "Eviction notices", "titlefont": {"color": "#C0392B"}},
        yaxis2={
            "title": "Net units added",
            "titlefont": {"color": "#2471A3"},
            "overlaying": "y",
            "side": "right",
        },
        legend={"orientation": "h", "y": -0.15},
        height=420,
        hovermode="x unified",
    )
    return fig


def eviction_type_breakdown(df: pl.DataFrame) -> go.Figure:
    """Stacked area chart of at-fault vs no-fault evictions over time."""
    if df.is_empty():
        return _empty("No eviction data available")

    data = eviction_type_trend(df)
    fig = go.Figure()

    for etype in ["at_fault", "no_fault"]:
        subset = data.filter(pl.col("eviction_type") == etype)
        label = "At-fault" if etype == "at_fault" else "No-fault"
        fig.add_trace(go.Scatter(
            x=subset["filed_month"].to_list(),
            y=subset["eviction_count"].to_list(),
            name=label,
            mode="lines",
            stackgroup="one",
            fillcolor=_PALETTE[etype],
            line={"color": _PALETTE[etype]},
        ))

    fig.update_layout(
        title="At-Fault vs No-Fault Evictions — Monthly",
        xaxis={"title": "Month"},
        yaxis={"title": "Eviction notices"},
        legend={"orientation": "h", "y": -0.15},
        height=380,
        hovermode="x unified",
    )
    return fig


def top_neighborhoods_bar(df: pl.DataFrame) -> go.Figure:
    """Horizontal bar of top neighborhoods by eviction count."""
    if df.is_empty():
        return _empty("No eviction data available", 350)

    data = top_eviction_neighborhoods(df)
    fig = go.Figure(go.Bar(
        x=data["eviction_count"].to_list(),
        y=data["neighborhood"].to_list(),
        orientation="h",
        marker_color="#C0392B",
    ))
    fig.update_layout(
        title="Top Neighborhoods by Eviction Notices",
        xaxis={"title": "Total notices"},
        yaxis={"title": ""},
        height=420,
        margin={"l": 160},
    )
    return fig
