"""Plotly figure builders for the Housing Production page.

Each function takes a Polars DataFrame already filtered by the global
date range / neighborhood selection and returns a styled Figure. No
Dash imports here — these are pure data → Figure transforms.
"""

from __future__ import annotations

import plotly.graph_objects as go
import plotly.io as pio
import polars as pl

from dashboard.components import theme_utils as tu
from dashboard.data.transforms import (
    cost_per_unit_neighborhoods,
    monthly_net_units,
    pipeline_by_age,
    use_transition_monthly,
    with_rolling_avg,
)

# ── Register Plotly templates ──────────────────────────────────────────────────
pio.templates["terminal_amber"] = go.layout.Template(
    layout=go.Layout(
        paper_bgcolor="#0f0c00",
        plot_bgcolor="#0f0c00",
        font=dict(family="'Share Tech Mono', monospace", color="#664d00", size=11),
        xaxis=dict(gridcolor="#1f1800", linecolor="#2a1f00", tickcolor="#664d00"),
        yaxis=dict(gridcolor="#1f1800", linecolor="#2a1f00", tickcolor="#664d00"),
        hoverlabel=dict(bgcolor="#1a1200", font_size=12, bordercolor="#ffb300"),
    )
)

pio.templates["cal_light"] = go.layout.Template(
    layout=go.Layout(
        paper_bgcolor="#F8F5EE",
        plot_bgcolor="#FFFFFF",
        font=dict(
            family="-apple-system, 'Segoe UI', Roboto, sans-serif",
            color="#4A3F28",
            size=11,
        ),
        xaxis=dict(gridcolor="#E0D8C4", linecolor="#E0D8C4", tickcolor="#9A8B6E"),
        yaxis=dict(gridcolor="#E0D8C4", linecolor="#E0D8C4", tickcolor="#9A8B6E"),
        hoverlabel=dict(bgcolor="#FFFFFF", font_size=12, bordercolor="#003262"),
    )
)

_TRANSITION_COLORS_DARK = {
    "new_residential": "#ffb300",
    "unit_addition": "#ffe066",
    "commercial_to_residential": "#ff6b35",
    "sfr_to_multifamily": "#4caf66",
    "demolition": "#ef5350",
    "renovation_same_use": "#3a2a00",
    "other": "#664d00",
}
_TRANSITION_COLORS_LIGHT = {
    "new_residential": "#C4820A",
    "unit_addition": "#FDB515",
    "commercial_to_residential": "#D9661F",
    "sfr_to_multifamily": "#16a34a",
    "demolition": "#dc2626",
    "renovation_same_use": "#9A8B6E",
    "other": "#E0D8C4",
}
_AGE_COLORS_DARK = {
    "<90d": "#ffe066",
    "90-180d": "#ffb300",
    "180-365d": "#ff6b35",
    ">365d": "#ef5350",
}
_AGE_COLORS_LIGHT = {
    "<90d": "#FDB515",
    "90-180d": "#C4820A",
    "180-365d": "#D9661F",
    ">365d": "#dc2626",
}


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


def trend_net_units(df: pl.DataFrame, template: str = "terminal_amber") -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 350, template)

    monthly = with_rolling_avg(monthly_net_units(df), "net_units_added", window=12)
    pal = tu.palette(template)

    fig = go.Figure()
    fig.add_bar(
        x=monthly["filed_month"],
        y=monthly["net_units_added"],
        name="Monthly",
        marker_color=pal[1],
        hovertemplate="%{x|%b %Y}<br>%{y:,.0f} net units<extra></extra>",
    )
    fig.add_scatter(
        x=monthly["filed_month"],
        y=monthly["net_units_added_rolling"],
        name="12-month avg",
        mode="lines",
        line=dict(color=pal[0], width=3),
        hovertemplate="%{x|%b %Y}<br>%{y:,.0f} avg<extra></extra>",
    )
    fig.update_layout(
        **tu.base_layout(template, margin=dict(l=60, r=20, t=80, b=40)),
        title="Net new units permitted, by month",
        height=350,
        bargap=0.15,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis_title="Net new units",
        xaxis_title=None,
    )
    return fig


def top_neighborhoods(
    df: pl.DataFrame, n: int = 15, template: str = "terminal_amber"
) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 400, template)

    top = (
        df.group_by("neighborhood")
        .agg(pl.col("net_units_added").sum().alias("net_units"))
        .sort("net_units", descending=True)
        .head(n)
        .sort("net_units")
    )
    pal = tu.palette(template)
    fig = go.Figure(
        go.Bar(
            x=top["net_units"],
            y=top["neighborhood"],
            orientation="h",
            marker_color=pal[0],
            hovertemplate="%{y}<br>%{x:,.0f} net units<extra></extra>",
        )
    )
    fig.update_layout(
        **tu.base_layout(template, margin=dict(l=180, r=20, t=50, b=40)),
        title=f"Top {n} neighborhoods by net new units",
        height=400,
        xaxis_title="Net new units",
        yaxis_title=None,
    )
    return fig


def district_breakdown(df: pl.DataFrame, template: str = "terminal_amber") -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 400, template)

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
            marker_color=tu.palette(template)[0],
            hovertemplate="District %{x}<br>%{y:,.0f} net units<extra></extra>",
        )
    )
    fig.update_layout(
        **tu.base_layout(template),
        title="Supervisor districts by net new units",
        height=400,
        xaxis_title="Supervisor district",
        yaxis_title="Net new units",
        xaxis=dict(type="category"),
    )
    return fig


def median_days_to_issue_trend(
    df: pl.DataFrame, template: str = "terminal_amber"
) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 350, template)

    monthly = (
        df.filter(pl.col("median_days_to_issue").is_not_null())
        .with_columns(
            (pl.col("median_days_to_issue") * pl.col("permits_filed")).alias("_w")
        )
        .group_by("filed_month")
        .agg((pl.col("_w").sum() / pl.col("permits_filed").sum()).alias("median_days"))
        .sort("filed_month")
    )
    fig = go.Figure(
        go.Scatter(
            x=monthly["filed_month"],
            y=monthly["median_days"],
            mode="lines",
            line=dict(color=tu.bad_color(template), width=2.5),
            hovertemplate="%{x|%b %Y}<br>%{y:.0f} days<extra></extra>",
        )
    )
    fig.update_layout(
        **tu.base_layout(template),
        title="Median days from filing to issuance",
        height=350,
        yaxis_title="Days",
        xaxis_title=None,
    )
    return fig


def completion_rate_by_district(
    df: pl.DataFrame, template: str = "terminal_amber"
) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 350, template)

    by_district = (
        df.filter(pl.col("supervisor_district").is_not_null())
        .group_by("supervisor_district")
        .agg(
            pl.col("permits_completed").sum().alias("completed"),
            pl.col("permits_filed").sum().alias("filed"),
        )
        .with_columns((pl.col("completed") / pl.col("filed")).alias("rate"))
        .sort("supervisor_district")
    )
    fig = go.Figure(
        go.Bar(
            x=by_district["supervisor_district"].cast(pl.Utf8),
            y=by_district["rate"],
            marker_color=tu.ok_color(template),
            hovertemplate="District %{x}<br>%{y:.1%} completed<extra></extra>",
        )
    )
    fig.update_layout(
        **tu.base_layout(template),
        title="Completion rate by supervisor district",
        height=350,
        xaxis_title="Supervisor district",
        yaxis_title="Completed / filed",
        yaxis=dict(tickformat=".0%", range=[0, 1], gridcolor=tu.grid_color(template)),
        xaxis=dict(type="category"),
    )
    return fig


def use_transition_breakdown(
    df: pl.DataFrame, template: str = "terminal_amber"
) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 350, template)

    monthly = use_transition_monthly(df)
    if monthly.is_empty():
        return _empty("No transition data available", 350, template)

    color_map = (
        _TRANSITION_COLORS_LIGHT if template == "cal_light" else _TRANSITION_COLORS_DARK
    )
    neutral = tu.empty_color(template)
    transitions = sorted(monthly["use_transition"].drop_nulls().unique().to_list())
    fig = go.Figure()
    for transition in transitions:
        subset = monthly.filter(pl.col("use_transition") == transition)
        label = transition.replace("_", " ").title()
        fig.add_bar(
            x=subset["filed_month"],
            y=subset["net_units_added"],
            name=label,
            marker_color=color_map.get(transition, neutral),
            hovertemplate=f"{label}<br>%{{x|%b %Y}}<br>%{{y:,.0f}} net units<extra></extra>",
        )
    fig.update_layout(
        **tu.base_layout(template, margin=dict(l=60, r=20, t=80, b=40)),
        title="Net new units by permit type, by month",
        height=350,
        barmode="stack",
        bargap=0.1,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis_title="Net new units",
        xaxis_title=None,
    )
    return fig


def cost_per_unit_by_neighborhood(
    df: pl.DataFrame, n: int = 15, template: str = "terminal_amber"
) -> go.Figure:
    if df.is_empty():
        return _empty("No data in the selected window", 400, template)

    data = cost_per_unit_neighborhoods(df, n=n)
    if data.is_empty():
        return _empty("No cost data available", 400, template)

    fig = go.Figure(
        go.Bar(
            x=data["avg_cost_per_unit"],
            y=data["neighborhood"],
            orientation="h",
            marker_color=tu.ok_color(template),
            hovertemplate="%{y}<br>$%{x:,.0f} per unit<extra></extra>",
        )
    )
    fig.update_layout(
        **tu.base_layout(template, margin=dict(l=180, r=20, t=50, b=40)),
        title=f"Avg cost per new unit — top {n} neighborhoods (new construction only)",
        height=400,
        xaxis_title="Avg cost per net unit ($)",
        yaxis_title=None,
        xaxis=dict(tickformat="$,.0f"),
    )
    return fig


def pipeline_backlog(df: pl.DataFrame, template: str = "terminal_amber") -> go.Figure:
    if df.is_empty():
        return _empty("No pipeline data available", 350, template)

    data = pipeline_by_age(df)
    if data.is_empty():
        return _empty("No in-flight permits", 350, template)

    age_map = _AGE_COLORS_LIGHT if template == "cal_light" else _AGE_COLORS_DARK
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
            marker_color=age_map[bucket],
            hovertemplate=f"{bucket}<br>%{{x}}<br>%{{y:,.0f}} permits<extra></extra>",
        )
    fig.update_layout(
        **tu.base_layout(template, margin=dict(l=60, r=20, t=80, b=40)),
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
