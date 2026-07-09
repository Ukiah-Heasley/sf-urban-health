"""Plotly figure builders for the Public Safety Incidents page."""

from __future__ import annotations

import plotly.graph_objects as go
import polars as pl

from dashboard.components import theme_utils as tu
from dashboard.data.incidents_transforms import (
    category_breakdown,
    monthly_incident_trend,
    resolution_rate_by_district,
    top_incident_neighborhoods,
)


def _empty(message: str, height: int, template: str = "terminal_amber") -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        **tu.base_layout(template),
        height=height,
        annotations=[
            dict(
                text=message,
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


def incident_trend(df: pl.DataFrame, template: str = "terminal_amber") -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 350, template)

    monthly = monthly_incident_trend(df)
    fig = go.Figure(
        go.Bar(
            x=monthly["incident_month"],
            y=monthly["total_incidents"],
            marker_color=tu.palette(template)[1],
            hovertemplate="%{x|%b %Y}<br>%{y:,.0f} incidents<extra></extra>",
        )
    )
    fig.update_layout(
        **tu.base_layout(template),
        title="Incidents by month",
        height=350,
        bargap=0.15,
        yaxis_title="Incidents",
        xaxis_title=None,
    )
    return fig


def top_neighborhoods(
    df: pl.DataFrame, n: int = 15, template: str = "terminal_amber"
) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 400, template)

    top = top_incident_neighborhoods(df, n)
    fig = go.Figure(
        go.Bar(
            x=top["total_incidents"],
            y=top["neighborhood"],
            orientation="h",
            marker_color=tu.palette(template)[0],
            hovertemplate="%{y}<br>%{x:,.0f} incidents<extra></extra>",
        )
    )
    fig.update_layout(
        **tu.base_layout(template, margin=dict(l=180, r=20, t=50, b=40)),
        title=f"Top {n} neighborhoods by incidents",
        height=400,
        xaxis_title="Incidents",
        yaxis_title=None,
    )
    return fig


def category_bar(
    df: pl.DataFrame, n: int = 20, template: str = "terminal_amber"
) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 400, template)

    cats = category_breakdown(df, n)
    fig = go.Figure(
        go.Bar(
            x=cats["total_incidents"],
            y=cats["incident_category"],
            orientation="h",
            marker_color=tu.palette(template)[2],
            hovertemplate="%{y}<br>%{x:,.0f} incidents<extra></extra>",
        )
    )
    fig.update_layout(
        **tu.base_layout(template, margin=dict(l=200, r=20, t=50, b=40)),
        title=f"Top {n} incident categories",
        height=400,
        xaxis_title="Incidents",
        yaxis_title=None,
    )
    return fig


def resolution_by_district(
    df: pl.DataFrame, template: str = "terminal_amber"
) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 350, template)

    by_dist = resolution_rate_by_district(df)
    fig = go.Figure(
        go.Bar(
            x=by_dist["police_district"],
            y=by_dist["resolution_rate"],
            marker_color=tu.ok_color(template),
            hovertemplate="%{x}<br>%{y:.1%} resolved<extra></extra>",
        )
    )
    fig.update_layout(
        **tu.base_layout(template),
        title="Resolution rate by police district",
        height=350,
        xaxis_title="Police district",
        yaxis_title="Resolved / total",
        yaxis=dict(
            tickformat=".0%",
            range=[0, 1],
            gridcolor=tu.grid_color(template),
            linecolor=tu.grid_color(template),
        ),
        xaxis=dict(type="category"),
    )
    return fig
