"""Evictions page — /evictions"""

from __future__ import annotations

from datetime import date, datetime

import polars as pl
import dash_bootstrap_components as dbc
from dash import Input, Output, callback, dcc, html

from dashboard.components import evictions_figures as efig
from dashboard.components.kpi import build_kpi_card
from dashboard.data.cache import EVICTIONS, MART
from dashboard.data.evictions_transforms import apply_eviction_filters, eviction_kpi_summary

ACCENT = "#ef5350"

_NEIGHBORHOODS = (
    sorted(n for n in EVICTIONS["neighborhood"].unique().to_list() if n is not None)
    if not EVICTIONS.is_empty()
    else []
)
_MIN_DATE: date = EVICTIONS["filed_month"].min() if not EVICTIONS.is_empty() else date.today()
_MAX_DATE: date = EVICTIONS["filed_month"].max() if not EVICTIONS.is_empty() else date.today()


def _filter_bar() -> dbc.Row:
    return dbc.Row(
        [
            dbc.Col(
                [
                    dbc.Label("Date range", className="fw-semibold mb-1"),
                    dcc.DatePickerRange(
                        id="ev-date-range",
                        min_date_allowed=_MIN_DATE,
                        max_date_allowed=_MAX_DATE,
                        start_date=_MIN_DATE,
                        end_date=_MAX_DATE,
                        display_format="MMM YYYY",
                    ),
                ],
                md=4,
            ),
            dbc.Col(
                [
                    dbc.Label("Neighborhoods", className="fw-semibold mb-1"),
                    dcc.Dropdown(
                        id="ev-neighborhoods",
                        options=[{"label": n, "value": n} for n in _NEIGHBORHOODS],
                        multi=True,
                        placeholder="All neighborhoods",
                    ),
                ],
                md=4,
            ),
        ],
        className="g-3 mb-4",
    )


layout = dbc.Container(
    [
        html.Div(
            [
                html.H2("SF Eviction Notices", className="mb-0"),
                html.Div(
                    f"SF Rent Board filings, {_MIN_DATE:%b %Y}–{_MAX_DATE:%b %Y}",
                    className="text-muted",
                ),
            ],
            className="py-4",
        ),
        _filter_bar(),
        dbc.Row(id="ev-kpi-row", className="g-3 mb-4"),
        dbc.Card(dbc.CardBody(dcc.Graph(id="ev-fig-vs-units")), className="mb-4 shadow-sm"),
        dbc.Card(dbc.CardBody(dcc.Graph(id="ev-fig-type-trend")), className="mb-4 shadow-sm"),
        dbc.Card(dbc.CardBody(dcc.Graph(id="ev-fig-neighborhoods")), className="mb-5 shadow-sm"),
    ],
    fluid=True,
    style={"maxWidth": "1400px"},
)


def _parse(d: str | None) -> date | None:
    if not d:
        return None
    return datetime.fromisoformat(d[:10]).date()


def _fmt_int(v: float | None) -> str:
    return "—" if v is None else f"{v:,.0f}"


def _fmt_pct(v: float | None) -> str:
    return "—" if v is None else f"{v:.1f}%"


@callback(
    Output("ev-kpi-row", "children"),
    Output("ev-fig-vs-units", "figure"),
    Output("ev-fig-type-trend", "figure"),
    Output("ev-fig-neighborhoods", "figure"),
    Input("ev-date-range", "start_date"),
    Input("ev-date-range", "end_date"),
    Input("ev-neighborhoods", "value"),
    Input("theme-store", "data"),
)
def refresh(start_date, end_date, neighborhoods, theme):
    template = "cal_light" if theme == "light" else "terminal_amber"
    start = _parse(start_date) or _MIN_DATE
    end = _parse(end_date) or _MAX_DATE

    filtered = apply_eviction_filters(EVICTIONS, start, end, neighborhoods)
    housing_filtered = (
        MART.filter(
            (pl.col("filed_month") >= start) & (pl.col("filed_month") <= end)
        )
        if not MART.is_empty()
        else MART
    )
    kpis = eviction_kpi_summary(filtered)

    cards = [
        dbc.Col(
            build_kpi_card("Total notices", _fmt_int(kpis["total"]), None, ACCENT),
            md=4,
        ),
        dbc.Col(
            build_kpi_card("No-fault share", _fmt_pct(kpis["no_fault_pct"]), None, "#ff6b35"),
            md=4,
        ),
        dbc.Col(
            build_kpi_card("Ellis Act notices", _fmt_int(kpis["ellis_act"]), None, "#ffb300"),
            md=4,
        ),
    ]

    return (
        cards,
        efig.evictions_vs_units(filtered, housing_filtered, template=template),
        efig.eviction_type_breakdown(filtered, template=template),
        efig.top_neighborhoods_bar(filtered, template=template),
    )
