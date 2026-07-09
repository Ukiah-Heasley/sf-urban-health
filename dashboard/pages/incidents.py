"""Public Safety Incidents page — /incidents"""

from __future__ import annotations

from datetime import date, datetime

import dash_bootstrap_components as dbc
from dash import Input, Output, callback, dcc, html

from dashboard.components import incidents_figures as ifig
from dashboard.components.kpi import build_kpi_card
from dashboard.data.cache import SAFETY_MART
from dashboard.data.incidents_transforms import (
    apply_incident_filters,
    incident_kpi_summary,
)

ACCENT = "#ff6b35"

_NEIGHBORHOODS = (
    sorted(n for n in SAFETY_MART["neighborhood"].unique().to_list() if n is not None)
    if not SAFETY_MART.is_empty()
    else []
)
_CATEGORIES = (
    sorted(c for c in SAFETY_MART["incident_category"].unique().to_list() if c is not None)
    if not SAFETY_MART.is_empty()
    else []
)
_MIN_DATE: date = SAFETY_MART["incident_month"].min() if not SAFETY_MART.is_empty() else date.today()
_MAX_DATE: date = SAFETY_MART["incident_month"].max() if not SAFETY_MART.is_empty() else date.today()


def _filter_bar() -> dbc.Row:
    return dbc.Row(
        [
            dbc.Col(
                [
                    dbc.Label("Date range", className="fw-semibold mb-1"),
                    dcc.DatePickerRange(
                        id="i-date-range",
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
                        id="i-neighborhoods",
                        options=[{"label": n, "value": n} for n in _NEIGHBORHOODS],
                        multi=True,
                        placeholder="All neighborhoods",
                    ),
                ],
                md=4,
            ),
            dbc.Col(
                [
                    dbc.Label("Incident category", className="fw-semibold mb-1"),
                    dcc.Dropdown(
                        id="i-categories",
                        options=[{"label": c, "value": c} for c in _CATEGORIES],
                        multi=True,
                        placeholder="All categories",
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
                html.H2("SF Public Safety Incidents", className="mb-0"),
                html.Div(
                    f"SFPD incident reports, {_MIN_DATE:%b %Y}–{_MAX_DATE:%b %Y}",
                    className="text-muted",
                ),
            ],
            className="py-4",
        ),
        _filter_bar(),
        # KPI strip
        dbc.Row(id="i-kpi-row", className="g-3 mb-4"),
        # Section 1: trend
        dbc.Card(dbc.CardBody(dcc.Graph(id="i-fig-trend")), className="mb-4 shadow-sm"),
        # Section 2: geography + categories
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(dcc.Graph(id="i-fig-top-neighborhoods")),
                        className="shadow-sm h-100",
                    ),
                    md=6,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(dcc.Graph(id="i-fig-categories")),
                        className="shadow-sm h-100",
                    ),
                    md=6,
                ),
            ],
            className="g-3 mb-4",
        ),
        # Section 3: resolution rates
        dbc.Card(
            dbc.CardBody(dcc.Graph(id="i-fig-resolution")), className="mb-5 shadow-sm"
        ),
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
    Output("i-kpi-row", "children"),
    Output("i-fig-trend", "figure"),
    Output("i-fig-top-neighborhoods", "figure"),
    Output("i-fig-categories", "figure"),
    Output("i-fig-resolution", "figure"),
    Input("i-date-range", "start_date"),
    Input("i-date-range", "end_date"),
    Input("i-neighborhoods", "value"),
    Input("i-categories", "value"),
    Input("theme-store", "data"),
)
def refresh(start_date, end_date, neighborhoods, categories, theme):
    template = "cal_light" if theme == "light" else "terminal_amber"
    start = _parse(start_date) or _MIN_DATE
    end = _parse(end_date) or _MAX_DATE

    filtered = apply_incident_filters(SAFETY_MART, start, end, neighborhoods, categories)
    kpis = incident_kpi_summary(filtered)

    cards = [
        dbc.Col(
            build_kpi_card("Total incidents", _fmt_int(kpis["total"]), None, ACCENT),
            md=4,
        ),
        dbc.Col(
            build_kpi_card("Top category", kpis["top_category"] or "—", None, "#ffb300"),
            md=4,
        ),
        dbc.Col(
            build_kpi_card("% resolved", _fmt_pct(kpis["pct_resolved"]), None, "#4caf66"),
            md=4,
        ),
    ]

    return (
        cards,
        ifig.incident_trend(filtered, template=template),
        ifig.top_neighborhoods(filtered, template=template),
        ifig.category_bar(filtered, template=template),
        ifig.resolution_by_district(filtered, template=template),
    )
