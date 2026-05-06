"""Pipeline Health page — /pipeline"""
from __future__ import annotations

from datetime import date, timedelta

import dash_bootstrap_components as dbc
from dash import Input, Output, callback, dcc, html

from dashboard.components import pipeline_figures as pfig
from dashboard.components.kpi import build_kpi_card
from dashboard.data.cache import DBT_TEST_HEALTH, PIPELINE_HEALTH
from dashboard.data.pipeline_transforms import pipeline_kpis

ACCENT = "#42a5f5"

_DEFAULT_DAYS = 30


def _days_selector() -> dbc.Row:
    return dbc.Row(
        dbc.Col(
            [
                dbc.Label("Lookback window", className="fw-semibold mb-1"),
                dcc.Dropdown(
                    id="pl-days",
                    options=[
                        {"label": "Last 7 days", "value": 7},
                        {"label": "Last 30 days", "value": 30},
                        {"label": "Last 90 days", "value": 90},
                    ],
                    value=_DEFAULT_DAYS,
                    clearable=False,
                    style={"width": "200px"},
                ),
            ],
            md=4,
        ),
        className="g-3 mb-4",
    )


layout = dbc.Container(
    [
        html.Div(
            [
                html.H2("Pipeline Health", className="mb-0"),
                html.Div(
                    "Airflow DAG runs + dbt test results",
                    className="text-muted",
                ),
            ],
            className="py-4",
        ),
        _days_selector(),
        dbc.Row(id="pl-kpi-row", className="g-3 mb-4"),
        dbc.Card(dbc.CardBody(dcc.Graph(id="pl-fig-timeline")), className="mb-4 shadow-sm"),
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(dbc.CardBody(dcc.Graph(id="pl-fig-duration")), className="shadow-sm"),
                    md=5,
                ),
                dbc.Col(
                    dbc.Card(dbc.CardBody(dcc.Graph(id="pl-fig-test-trend")), className="shadow-sm"),
                    md=7,
                ),
            ],
            className="mb-4 g-3",
        ),
        dbc.Card(dbc.CardBody(dcc.Graph(id="pl-fig-failing-tests")), className="mb-5 shadow-sm"),
    ],
    fluid=True,
    style={"maxWidth": "1400px"},
)


def _fmt_pct(v: float | None) -> str:
    return "—" if v is None else f"{v:.1f}%"


def _fmt_dur(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{v:.0f}s" if v < 3600 else f"{v / 60:.0f}m"


def _fmt_int(v: int | None) -> str:
    return "—" if v is None else f"{v:,}"


@callback(
    Output("pl-kpi-row", "children"),
    Output("pl-fig-timeline", "figure"),
    Output("pl-fig-duration", "figure"),
    Output("pl-fig-test-trend", "figure"),
    Output("pl-fig-failing-tests", "figure"),
    Input("pl-days", "value"),
    Input("theme-store", "data"),
)
def refresh(days, theme):
    template = "cal_light" if theme == "light" else "terminal_amber"
    days = days or _DEFAULT_DAYS
    cutoff = date.today() - timedelta(days=days)

    pipeline = (
        PIPELINE_HEALTH.filter(PIPELINE_HEALTH["run_date"] >= cutoff)
        if not PIPELINE_HEALTH.is_empty()
        else PIPELINE_HEALTH
    )
    tests = (
        DBT_TEST_HEALTH.filter(DBT_TEST_HEALTH["run_date"] >= cutoff)
        if not DBT_TEST_HEALTH.is_empty()
        else DBT_TEST_HEALTH
    )

    kpis = pipeline_kpis(pipeline, tests, days=days)

    cards = [
        dbc.Col(build_kpi_card("Success rate", _fmt_pct(kpis["success_rate_pct"]), None, ACCENT), md=3),
        dbc.Col(build_kpi_card("Avg run duration", _fmt_dur(kpis["avg_duration_seconds"]), None, "#ffb300", higher_is_better=False), md=3),
        dbc.Col(build_kpi_card("dbt test pass rate", _fmt_pct(kpis["test_pass_rate_pct"]), None, "#4caf66"), md=3),
        dbc.Col(build_kpi_card("Records ingested", _fmt_int(kpis["total_records_ingested"]), None, "#ab47bc"), md=3),
    ]

    return (
        cards,
        pfig.dag_timeline_bar(pipeline, template=template),
        pfig.task_duration_bar(pipeline, template=template),
        pfig.test_pass_rate_line(tests, template=template),
        pfig.failing_tests_table(tests, template=template),
    )
