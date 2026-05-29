"""Housing Production page — /"""

from __future__ import annotations

from datetime import date, datetime

import dash_bootstrap_components as dbc
import polars as pl
from dash import Input, Output, callback, dcc, html

from dashboard.components import figures as fig
from dashboard.components.kpi import build_kpi_card
from dashboard.data.cache import MART, PIPELINE
from dashboard.data.transforms import (
    apply_filters,
    kpi_summary,
    trailing_window,
)

ACCENT = "#ffb300"

# Marts load empty when Snowflake is unavailable (see data/cache.py); guard
# every module-level column access so the app still imports/boots, matching
# the evictions and incidents pages.
_NEIGHBORHOODS = (
    sorted(n for n in MART["neighborhood"].unique().to_list() if n is not None)
    if not MART.is_empty()
    else []
)
_MIN_DATE: date = MART["filed_month"].min() if not MART.is_empty() else date.today()
_MAX_DATE: date = MART["filed_month"].max() if not MART.is_empty() else date.today()


def _filter_bar() -> dbc.Row:
    return dbc.Row(
        [
            dbc.Col(
                [
                    dbc.Label("Date range", className="fw-semibold mb-1"),
                    dcc.DatePickerRange(
                        id="h-date-range",
                        min_date_allowed=_MIN_DATE,
                        max_date_allowed=_MAX_DATE,
                        start_date=_MIN_DATE,
                        end_date=_MAX_DATE,
                        display_format="MMM YYYY",
                    ),
                ],
                md=6,
            ),
            dbc.Col(
                [
                    dbc.Label("Neighborhoods", className="fw-semibold mb-1"),
                    dcc.Dropdown(
                        id="h-neighborhoods",
                        options=[{"label": n, "value": n} for n in _NEIGHBORHOODS],
                        multi=True,
                        placeholder="All neighborhoods",
                    ),
                ],
                md=6,
            ),
        ],
        className="g-3 mb-4",
    )


layout = dbc.Container(
    [
        html.Div(
            [
                html.H2("SF Housing Production", className="mb-0"),
                html.Div(
                    "Permits filed with the City of San Francisco, "
                    f"{_MIN_DATE:%b %Y}–{_MAX_DATE:%b %Y}",
                    className="text-muted",
                ),
            ],
            className="py-4",
        ),
        _filter_bar(),
        # KPI strip
        dbc.Row(id="h-kpi-row", className="g-3 mb-4"),
        # Section 1: trend
        dbc.Card(dbc.CardBody(dcc.Graph(id="h-fig-trend")), className="mb-4 shadow-sm"),
        # Section 2: geography
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(dcc.Graph(id="h-fig-top-neighborhoods")),
                        className="shadow-sm h-100",
                    ),
                    md=6,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(dcc.Graph(id="h-fig-districts")),
                        className="shadow-sm h-100",
                    ),
                    md=6,
                ),
            ],
            className="g-3 mb-4",
        ),
        # Section 3: process efficiency
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(dcc.Graph(id="h-fig-days-to-issue")),
                        className="shadow-sm h-100",
                    ),
                    md=6,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(dcc.Graph(id="h-fig-completion-rate")),
                        className="shadow-sm h-100",
                    ),
                    md=6,
                ),
            ],
            className="g-3 mb-4",
        ),
        # Section 4: housing mix over time
        dbc.Card(
            dbc.CardBody(dcc.Graph(id="h-fig-use-transition")), className="mb-4 shadow-sm"
        ),
        # Section 5: cost efficiency + pipeline backlog
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(dcc.Graph(id="h-fig-cost-per-unit")),
                        className="shadow-sm h-100",
                    ),
                    md=6,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(dcc.Graph(id="h-fig-pipeline")),
                        className="shadow-sm h-100",
                    ),
                    md=6,
                ),
            ],
            className="g-3 mb-5",
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


def _fmt_money(v: float | None) -> str:
    if v is None:
        return "—"
    if abs(v) >= 1e9:
        return f"${v / 1e9:.2f}B"
    if abs(v) >= 1e6:
        return f"${v / 1e6:.1f}M"
    return f"${v:,.0f}"


def _fmt_days(v: float | None) -> str:
    return "—" if v is None else f"{v:.0f} d"


def _shift_months(d: date, months: int) -> date:
    m = d.month - 1 + months
    year = d.year + m // 12
    month = m % 12 + 1
    day = min(d.day, 28)
    return date(year, month, day)


@callback(
    Output("h-kpi-row", "children"),
    Output("h-fig-trend", "figure"),
    Output("h-fig-top-neighborhoods", "figure"),
    Output("h-fig-districts", "figure"),
    Output("h-fig-days-to-issue", "figure"),
    Output("h-fig-completion-rate", "figure"),
    Output("h-fig-use-transition", "figure"),
    Output("h-fig-cost-per-unit", "figure"),
    Output("h-fig-pipeline", "figure"),
    Input("h-date-range", "start_date"),
    Input("h-date-range", "end_date"),
    Input("h-neighborhoods", "value"),
    Input("theme-store", "data"),
)
def refresh(start_date, end_date, neighborhoods, theme):
    template = "cal_light" if theme == "light" else "terminal_amber"
    start = _parse(start_date) or _MIN_DATE
    end = _parse(end_date) or _MAX_DATE

    filtered = apply_filters(MART, start, end, neighborhoods)

    # KPI deltas: last 12 months in the selected window vs. the 12 months
    # before that. We respect the neighborhood filter but ignore the date
    # filter when computing the prior window — otherwise the comparison
    # baseline would shrink as the user narrows the date range.
    nbhd_only = apply_filters(MART, None, None, neighborhoods)
    cur_window = trailing_window(nbhd_only, end, 12)
    prior_window = trailing_window(nbhd_only, _shift_months(end, -12), 12)

    kpis = kpi_summary(cur_window, prior_window)

    # Pipeline snapshot — filter by neighborhood but not date (it's a current snapshot).
    _pipeline_has_data = "age_bucket" in PIPELINE.columns
    pipeline_filtered = (
        PIPELINE.filter(pl.col("neighborhood").is_in(neighborhoods))
        if (_pipeline_has_data and neighborhoods)
        else PIPELINE
    )
    stalled = (
        int(
            pipeline_filtered.filter(
                pl.col("age_bucket").is_in(["180-365d", ">365d"])
            )["permit_count"].sum()
            or 0
        )
        if _pipeline_has_data
        else 0
    )

    cards = [
        dbc.Col(
            build_kpi_card(
                "Net new units",
                _fmt_int(kpis["net_units"][0]),
                kpis["net_units"][1],
                ACCENT,
            ),
            width=True,
        ),
        dbc.Col(
            build_kpi_card(
                "Permits filed",
                _fmt_int(kpis["permits_filed"][0]),
                kpis["permits_filed"][1],
                "#ffe066",
            ),
            width=True,
        ),
        dbc.Col(
            build_kpi_card(
                "Total project cost",
                _fmt_money(kpis["project_cost"][0]),
                kpis["project_cost"][1],
                "#ff6b35",
            ),
            width=True,
        ),
        dbc.Col(
            build_kpi_card(
                "Median days to issue",
                _fmt_days(kpis["median_days"][0]),
                kpis["median_days"][1],
                "#ef5350",
                higher_is_better=False,
            ),
            width=True,
        ),
        dbc.Col(
            build_kpi_card(
                "Stalled permits",
                _fmt_int(stalled),
                None,
                "#664d00",
                higher_is_better=False,
            ),
            width=True,
        ),
    ]

    return (
        cards,
        fig.trend_net_units(filtered, template=template),
        fig.top_neighborhoods(filtered, n=15, template=template),
        fig.district_breakdown(filtered, template=template),
        fig.median_days_to_issue_trend(filtered, template=template),
        fig.completion_rate_by_district(filtered, template=template),
        fig.use_transition_breakdown(filtered, template=template),
        fig.cost_per_unit_by_neighborhood(filtered, template=template),
        fig.pipeline_backlog(pipeline_filtered, template=template),
    )
