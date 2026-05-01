"""Plotly figure builders for the Public Safety Incidents page.

Each function takes a Polars DataFrame already filtered by the page's
date range / neighborhood / category selection and returns a styled Figure.
No Dash imports here — pure data → Figure transforms.
"""

from __future__ import annotations

import plotly.graph_objects as go
import polars as pl

from dashboard.data.incidents_transforms import (
    category_breakdown,
    monthly_incident_trend,
    resolution_rate_by_district,
    top_incident_neighborhoods,
)

ACCENT = "#ff6b35"
ACCENT_LIGHT = "#ffb300"
NEUTRAL = "#664d00"
PALETTE = ["#ff6b35", "#ffb300", "#ffe066", "#4caf66", "#42a5f5"]

_BASE_LAYOUT = dict(
    template="terminal_amber",
    margin=dict(l=75, r=20, t=50, b=40),
    font=dict(family="'Share Tech Mono', monospace", size=11),
    title_font=dict(size=13, color="#886600"),
)


def _empty(message: str, height: int) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        **_BASE_LAYOUT,
        height=height,
        annotations=[
            dict(
                text=message,
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.5,
                showarrow=False,
                font=dict(size=14, color=NEUTRAL),
            )
        ],
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )
    return fig


def incident_trend(df: pl.DataFrame) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 350)

    monthly = monthly_incident_trend(df)
    fig = go.Figure(
        go.Bar(
            x=monthly["incident_month"],
            y=monthly["total_incidents"],
            marker_color=ACCENT_LIGHT,
            hovertemplate="%{x|%b %Y}<br>%{y:,.0f} incidents<extra></extra>",
        )
    )
    fig.update_layout(
        **_BASE_LAYOUT,
        title="Incidents by month",
        height=350,
        bargap=0.15,
        yaxis_title="Incidents",
        xaxis_title=None,
    )
    return fig


def top_neighborhoods(df: pl.DataFrame, n: int = 15) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 400)

    top = top_incident_neighborhoods(df, n)
    layout = {**_BASE_LAYOUT, "margin": dict(l=180, r=20, t=50, b=40)}
    fig = go.Figure(
        go.Bar(
            x=top["total_incidents"],
            y=top["neighborhood"],
            orientation="h",
            marker_color=ACCENT,
            hovertemplate="%{y}<br>%{x:,.0f} incidents<extra></extra>",
        )
    )
    fig.update_layout(
        **layout,
        title=f"Top {n} neighborhoods by incidents",
        height=400,
        xaxis_title="Incidents",
        yaxis_title=None,
    )
    return fig


def category_bar(df: pl.DataFrame, n: int = 20) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 400)

    cats = category_breakdown(df, n)
    layout = {**_BASE_LAYOUT, "margin": dict(l=200, r=20, t=50, b=40)}
    fig = go.Figure(
        go.Bar(
            x=cats["total_incidents"],
            y=cats["incident_category"],
            orientation="h",
            marker_color=PALETTE[2],
            hovertemplate="%{y}<br>%{x:,.0f} incidents<extra></extra>",
        )
    )
    fig.update_layout(
        **layout,
        title=f"Top {n} incident categories",
        height=400,
        xaxis_title="Incidents",
        yaxis_title=None,
    )
    return fig


def resolution_by_district(df: pl.DataFrame) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 350)

    by_dist = resolution_rate_by_district(df)
    fig = go.Figure(
        go.Bar(
            x=by_dist["police_district"],
            y=by_dist["resolution_rate"],
            marker_color=PALETTE[3],
            hovertemplate="%{x}<br>%{y:.1%} resolved<extra></extra>",
        )
    )
    fig.update_layout(
        **_BASE_LAYOUT,
        title="Resolution rate by police district",
        height=350,
        xaxis_title="Police district",
        yaxis_title="Resolved / total",
        yaxis=dict(tickformat=".0%", range=[0, 1], gridcolor="#1f1800", linecolor="#2a1f00"),
        xaxis=dict(type="category"),
    )
    return fig
