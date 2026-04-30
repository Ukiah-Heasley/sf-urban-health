"""Plotly figure builders.

Each function takes a Polars DataFrame already filtered by the global
date range / neighborhood selection and returns a styled Figure. No
Dash imports here — these are pure data → Figure transforms, easy to
inspect in a REPL or unit-test.
"""

from __future__ import annotations

import plotly.graph_objects as go
import polars as pl

from dashboard.data.transforms import (
    cost_per_unit_neighborhoods,
    monthly_net_units,
    pipeline_by_age,
    use_transition_monthly,
    with_rolling_avg,
)

ACCENT = "#2C7873"
ACCENT_LIGHT = "#6FB3B8"
NEUTRAL = "#9DA9A0"
PALETTE = ["#2C7873", "#6FB3B8", "#C8D5B9", "#F2A65A", "#772E25"]

_TRANSITION_COLORS = {
    "new_residential": "#2C7873",
    "unit_addition": "#6FB3B8",
    "commercial_to_residential": "#C8D5B9",
    "sfr_to_multifamily": "#F2A65A",
    "demolition": "#772E25",
    "renovation_same_use": "#9DA9A0",
    "other": "#D3D3D3",
}

_AGE_COLORS = {
    "<90d": "#C8D5B9",
    "90-180d": "#F2A65A",
    "180-365d": "#D46027",
    ">365d": "#772E25",
}

_BASE_LAYOUT = dict(
    template="plotly_white",
    margin=dict(l=60, r=20, t=50, b=40),
    font=dict(family="system-ui, -apple-system, sans-serif", size=12),
    title_font=dict(size=15, family="system-ui, sans-serif"),
    hoverlabel=dict(bgcolor="white", font_size=12),
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


def trend_net_units(df: pl.DataFrame) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 350)

    monthly = with_rolling_avg(monthly_net_units(df), "net_units_added", window=12)

    fig = go.Figure()
    fig.add_bar(
        x=monthly["filed_month"],
        y=monthly["net_units_added"],
        name="Monthly",
        marker_color=ACCENT_LIGHT,
        hovertemplate="%{x|%b %Y}<br>%{y:,.0f} net units<extra></extra>",
    )
    fig.add_scatter(
        x=monthly["filed_month"],
        y=monthly["net_units_added_rolling"],
        name="12-month avg",
        mode="lines",
        line=dict(color=ACCENT, width=3),
        hovertemplate="%{x|%b %Y}<br>%{y:,.0f} avg<extra></extra>",
    )
    fig.update_layout(
        **_BASE_LAYOUT,
        title="Net new units permitted, by month",
        height=350,
        bargap=0.15,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis_title="Net new units",
        xaxis_title=None,
    )
    return fig


def top_neighborhoods(df: pl.DataFrame, n: int = 15) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 400)

    top = (
        df.group_by("neighborhood")
        .agg(pl.col("net_units_added").sum().alias("net_units"))
        .sort("net_units", descending=True)
        .head(n)
        .sort("net_units")  # ascending for horizontal bar (largest at top)
    )

    fig = go.Figure(
        go.Bar(
            x=top["net_units"],
            y=top["neighborhood"],
            orientation="h",
            marker_color=ACCENT,
            hovertemplate="%{y}<br>%{x:,.0f} net units<extra></extra>",
        )
    )
    layout = {**_BASE_LAYOUT, "margin": dict(l=180, r=20, t=50, b=40)}
    fig.update_layout(
        **layout,
        title=f"Top {n} neighborhoods by net new units",
        height=400,
        xaxis_title="Net new units",
        yaxis_title=None,
    )
    return fig


def district_breakdown(df: pl.DataFrame) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 400)

    by_district = (
        df.filter(pl.col("supervisor_district").is_not_null())
        .group_by("supervisor_district")
        .agg(pl.col("net_units_added").sum().alias("net_units"))
        .sort("supervisor_district")
    )

    fig = go.Figure(
        go.Bar(
            x=by_district["supervisor_district"].cast(pl.Utf8),
            y=by_district["net_units"],
            marker_color=ACCENT,
            hovertemplate="District %{x}<br>%{y:,.0f} net units<extra></extra>",
        )
    )
    fig.update_layout(
        **_BASE_LAYOUT,
        title="Supervisor districts by net new units",
        height=400,
        xaxis_title="Supervisor district",
        yaxis_title="Net new units",
        xaxis=dict(type="category"),
    )
    return fig


def median_days_to_issue_trend(df: pl.DataFrame) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 350)

    # Weight each row's median by permits_filed when collapsing across
    # neighborhoods/districts in the same month — simple and defensible.
    monthly = (
        df.filter(pl.col("median_days_to_issue").is_not_null())
        .with_columns(
            (pl.col("median_days_to_issue") * pl.col("permits_filed")).alias("_w"),
        )
        .group_by("filed_month")
        .agg(
            (pl.col("_w").sum() / pl.col("permits_filed").sum()).alias("median_days"),
        )
        .sort("filed_month")
    )

    fig = go.Figure(
        go.Scatter(
            x=monthly["filed_month"],
            y=monthly["median_days"],
            mode="lines",
            line=dict(color=PALETTE[3], width=2.5),
            hovertemplate="%{x|%b %Y}<br>%{y:.0f} days<extra></extra>",
        )
    )
    fig.update_layout(
        **_BASE_LAYOUT,
        title="Median days from filing to issuance",
        height=350,
        yaxis_title="Days",
        xaxis_title=None,
    )
    return fig


def completion_rate_by_district(df: pl.DataFrame) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 350)

    by_district = (
        df.filter(pl.col("supervisor_district").is_not_null())
        .group_by("supervisor_district")
        .agg(
            pl.col("permits_completed").sum().alias("completed"),
            pl.col("permits_filed").sum().alias("filed"),
        )
        .with_columns(
            (pl.col("completed") / pl.col("filed")).alias("rate"),
        )
        .sort("supervisor_district")
    )

    fig = go.Figure(
        go.Bar(
            x=by_district["supervisor_district"].cast(pl.Utf8),
            y=by_district["rate"],
            marker_color=PALETTE[2],
            hovertemplate="District %{x}<br>%{y:.1%} completed<extra></extra>",
        )
    )
    fig.update_layout(
        **_BASE_LAYOUT,
        title="Completion rate by supervisor district",
        height=350,
        xaxis_title="Supervisor district",
        yaxis_title="Completed / filed",
        yaxis=dict(tickformat=".0%", range=[0, 1]),
        xaxis=dict(type="category"),
    )
    return fig


def use_transition_breakdown(df: pl.DataFrame) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 350)

    monthly = use_transition_monthly(df)
    if monthly.is_empty():
        return _empty("No transition data available", 350)

    transitions = sorted(monthly["use_transition"].drop_nulls().unique().to_list())
    fig = go.Figure()
    for transition in transitions:
        subset = monthly.filter(pl.col("use_transition") == transition)
        label = transition.replace("_", " ").title()
        fig.add_bar(
            x=subset["filed_month"],
            y=subset["net_units_added"],
            name=label,
            marker_color=_TRANSITION_COLORS.get(transition, NEUTRAL),
            hovertemplate=f"{label}<br>%{{x|%b %Y}}<br>%{{y:,.0f}} net units<extra></extra>",
        )
    fig.update_layout(
        **_BASE_LAYOUT,
        title="Net new units by permit type, by month",
        height=350,
        barmode="stack",
        bargap=0.1,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis_title="Net new units",
        xaxis_title=None,
    )
    return fig


def cost_per_unit_by_neighborhood(df: pl.DataFrame, n: int = 15) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 400)

    data = cost_per_unit_neighborhoods(df, n=n)
    if data.is_empty():
        return _empty("No cost data available", 400)

    layout = {**_BASE_LAYOUT, "margin": dict(l=180, r=20, t=50, b=40)}
    fig = go.Figure(
        go.Bar(
            x=data["avg_cost_per_unit"],
            y=data["neighborhood"],
            orientation="h",
            marker_color=PALETTE[3],
            hovertemplate="%{y}<br>$%{x:,.0f} per unit<extra></extra>",
        )
    )
    fig.update_layout(
        **layout,
        title=f"Avg cost per new unit — top {n} neighborhoods (new construction only)",
        height=400,
        xaxis_title="Avg cost per net unit ($)",
        yaxis_title=None,
        xaxis=dict(tickformat="$,.0f"),
    )
    return fig


def pipeline_backlog(df: pl.DataFrame) -> go.Figure:
    if df.is_empty():
        return _empty("No pipeline data available", 350)

    data = pipeline_by_age(df)
    if data.is_empty():
        return _empty("No in-flight permits", 350)

    age_order = ["<90d", "90-180d", "180-365d", ">365d"]
    fig = go.Figure()
    for bucket in age_order:
        subset = data.filter(pl.col("age_bucket") == bucket)
        if subset.is_empty():
            continue
        fig.add_bar(
            x=subset["lifecycle_stage"].cast(pl.Utf8),
            y=subset["permit_count"],
            name=bucket,
            marker_color=_AGE_COLORS[bucket],
            hovertemplate=f"{bucket}<br>%{{x}}<br>%{{y:,.0f}} permits<extra></extra>",
        )
    fig.update_layout(
        **_BASE_LAYOUT,
        title="In-flight residential permits by stage and age",
        height=350,
        barmode="group",
        bargap=0.2,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis_title="Permits",
        xaxis_title=None,
        xaxis=dict(type="category"),
    )
    return fig
