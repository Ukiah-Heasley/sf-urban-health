"""Chart builders for the Evictions dashboard page."""
from __future__ import annotations

import plotly.graph_objects as go
import polars as pl

from dashboard.components import theme_utils as tu
from dashboard.data.evictions_transforms import (
    eviction_type_trend,
    monthly_eviction_trend,
    top_eviction_neighborhoods,
)


def _empty(msg: str, height: int = 400, template: str = "terminal_amber") -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        **tu.base_layout(template),
        height=height,
        annotations=[dict(
            text=msg, xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=14, color=tu.empty_color(template)),
        )],
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )
    return fig


def evictions_vs_units(
    evictions: pl.DataFrame,
    housing: pl.DataFrame,
    template: str = "terminal_amber",
) -> go.Figure:
    """Dual-axis line chart: monthly eviction count vs net units added."""
    if evictions.is_empty():
        return _empty("No eviction data available", template=template)

    data = monthly_eviction_trend(evictions, housing)
    pal = tu.palette(template)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=data["filed_month"].to_list(), y=data["eviction_count"].to_list(),
        name="Eviction notices",
        line={"color": pal[4], "width": 2}, yaxis="y1",
        hovertemplate="%{x|%b %Y}<br>%{y:,.0f} notices<extra></extra>",
    ))
    if "net_units_added" in data.columns:
        fig.add_trace(go.Scatter(
            x=data["filed_month"].to_list(), y=data["net_units_added"].to_list(),
            name="Net units added",
            line={"color": pal[0], "width": 2, "dash": "dot"}, yaxis="y2",
            hovertemplate="%{x|%b %Y}<br>%{y:,.0f} units<extra></extra>",
        ))

    grid = tu.grid_color(template)
    fig.update_layout(
        **tu.base_layout(template),
        title="Eviction notices vs. new housing units — monthly",
        height=420, hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis=dict(title="Eviction notices", titlefont=dict(color=pal[4]),
                   gridcolor=grid, linecolor=grid),
        yaxis2=dict(title="Net units added", titlefont=dict(color=pal[0]),
                    overlaying="y", side="right", gridcolor=grid, linecolor=grid),
    )
    return fig


def eviction_type_breakdown(df: pl.DataFrame, template: str = "terminal_amber") -> go.Figure:
    """Stacked area chart of at-fault vs no-fault evictions over time."""
    if df.is_empty():
        return _empty("No eviction data available", template=template)

    data = eviction_type_trend(df)
    pal = tu.palette(template)
    type_colors = {"at_fault": pal[4], "no_fault": pal[1]}

    fig = go.Figure()
    for etype in ["at_fault", "no_fault"]:
        subset = data.filter(pl.col("eviction_type") == etype)
        label = "At-fault" if etype == "at_fault" else "No-fault"
        fig.add_trace(go.Scatter(
            x=subset["filed_month"].to_list(), y=subset["eviction_count"].to_list(),
            name=label, mode="lines", stackgroup="one",
            fillcolor=type_colors[etype], line={"color": type_colors[etype]},
            hovertemplate=f"{label}<br>%{{x|%b %Y}}<br>%{{y:,.0f}} notices<extra></extra>",
        ))

    fig.update_layout(
        **tu.base_layout(template, margin=dict(l=60, r=20, t=80, b=40)),
        title="At-fault vs no-fault evictions — monthly",
        height=380, hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis_title="Eviction notices", xaxis_title=None,
    )
    return fig


def top_neighborhoods_bar(df: pl.DataFrame, template: str = "terminal_amber") -> go.Figure:
    """Horizontal bar of top neighborhoods by eviction count."""
    if df.is_empty():
        return _empty("No eviction data available", 350, template)

    data = top_eviction_neighborhoods(df)
    fig = go.Figure(go.Bar(
        x=data["eviction_count"].to_list(), y=data["neighborhood"].to_list(),
        orientation="h", marker_color=tu.palette(template)[4],
        hovertemplate="%{y}<br>%{x:,.0f} notices<extra></extra>",
    ))
    fig.update_layout(
        **tu.base_layout(template, margin=dict(l=160, r=20, t=50, b=40)),
        title="Top neighborhoods by eviction notices",
        height=420, xaxis_title="Total notices", yaxis_title=None,
    )
    return fig
