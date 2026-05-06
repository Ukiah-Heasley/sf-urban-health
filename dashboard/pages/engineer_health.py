"""Engineer Health Dashboard — /engineer
Mirrors the engineer_health.html preview: DAG success rate, runtime trends,
and test volume/pass rate, connected to mart_pipeline_summary and
mart_pipeline_health / mart_dbt_test_health.
"""
from __future__ import annotations

from datetime import date, timedelta

import dash_bootstrap_components as dbc
import polars as pl
from dash import Input, Output, callback, dcc, html

from dashboard.components import engineer_figures as efig
from dashboard.components.kpi import build_kpi_card
from dashboard.data.cache import DBT_TEST_HEALTH, PIPELINE_HEALTH, PIPELINE_SUMMARY

_ACCENT_DARK  = "#42a5f5"
_ACCENT_LIGHT = "#003262"


def _fmt_pct(v: float | None) -> str:
    return "—" if v is None else f"{v:.1f}%"


def _fmt_dur(v: float | None) -> str:
    if v is None:
        return "—"
    m, s = divmod(int(v), 60)
    return f"{m}m {s:02d}s"


def _fmt_int(v: int | None) -> str:
    return "—" if v is None else f"{v:,}"


def _delta_sign(v: float | None) -> str:
    if v is None:
        return "—"
    arrow = "↑" if v > 0 else "↓"
    return f"{arrow} {abs(v):.1f}% vs prior 7 days"


layout = dbc.Container(
    [
        html.Div(
            [
                html.Div(
                    [
                        html.H2("Pipeline Health Dashboard", className="mb-0"),
                        dbc.Badge(
                            "Engineering",
                            color="primary",
                            className="ms-2 align-middle",
                            style={"verticalAlign": "middle"},
                        ),
                    ],
                    className="d-flex align-items-center",
                ),
                html.Div(
                    "DAG success rates, runtime trends, and dbt validation results  ·  "
                    "Sources: mart_pipeline_summary · mart_pipeline_health · mart_dbt_test_health",
                    className="text-muted mt-1",
                    style={"fontSize": "0.8rem"},
                ),
            ],
            className="py-4",
        ),

        # ── KPI strip ──────────────────────────────────────────────────────────
        dbc.Row(id="eng-kpi-row", className="g-3 mb-4"),

        # ── Per-DAG status cards ───────────────────────────────────────────────
        html.Div(id="eng-dag-status-row", className="mb-4"),

        # ── Trend charts ──────────────────────────────────────────────────────
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(dbc.CardBody(dcc.Graph(id="eng-fig-success-rate")), className="shadow-sm h-100"),
                    md=6,
                ),
                dbc.Col(
                    dbc.Card(dbc.CardBody(dcc.Graph(id="eng-fig-runtime")), className="shadow-sm h-100"),
                    md=6,
                ),
            ],
            className="g-3 mb-4",
        ),

        # ── Test health ───────────────────────────────────────────────────────
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(dbc.CardBody(dcc.Graph(id="eng-fig-test-bar")), className="shadow-sm h-100"),
                    md=7,
                ),
                dbc.Col(
                    dbc.Card(dbc.CardBody(dcc.Graph(id="eng-fig-test-table")), className="shadow-sm h-100"),
                    md=5,
                ),
            ],
            className="g-3 mb-5",
        ),
    ],
    fluid=True,
    style={"maxWidth": "1400px"},
)


@callback(
    Output("eng-kpi-row",         "children"),
    Output("eng-dag-status-row",  "children"),
    Output("eng-fig-success-rate","figure"),
    Output("eng-fig-runtime",     "figure"),
    Output("eng-fig-test-bar",    "figure"),
    Output("eng-fig-test-table",  "figure"),
    Input("theme-store", "data"),
)
def refresh(theme: str):
    template = "cal_light" if theme == "light" else "terminal_amber"
    accent   = _ACCENT_LIGHT if theme == "light" else _ACCENT_DARK

    # ── Aggregate KPIs from PIPELINE_SUMMARY ──────────────────────────────────
    if not PIPELINE_SUMMARY.is_empty():
        s = PIPELINE_SUMMARY
        success_7d   = round(s["success_rate_7d"].mean(), 1)     if "success_rate_7d"            in s.columns else None
        failures_7d  = int(s["failures_7d"].sum())                if "failures_7d"                in s.columns else None
        avg_dur      = s["avg_duration_7d_seconds"].mean()        if "avg_duration_7d_seconds"    in s.columns else None
        dur_chg      = s["duration_change_pct"].mean()            if "duration_change_pct"        in s.columns else None
        records_7d   = int(s["total_records_7d"].sum())           if "total_records_7d"           in s.columns else None
    else:
        success_7d = failures_7d = avg_dur = dur_chg = records_7d = None

    kpi_cards = [
        dbc.Col(build_kpi_card("7-Day Success Rate", _fmt_pct(success_7d), None, accent), md=3),
        dbc.Col(build_kpi_card("Failures (7d)", str(failures_7d) if failures_7d is not None else "—", None,
                               "#ffb300" if theme == "dark" else "#C4820A", higher_is_better=False), md=3),
        dbc.Col(build_kpi_card("Avg Runtime (7d)", _fmt_dur(avg_dur),
                               dur_chg, "#ff6b35" if theme == "dark" else "#D9661F",
                               higher_is_better=False), md=3),
        dbc.Col(build_kpi_card("Records Ingested (7d)", _fmt_int(records_7d), None,
                               "#4caf66" if theme == "dark" else "#16a34a"), md=3),
    ]

    # ── Per-DAG status cards ───────────────────────────────────────────────────
    dag_status_row = _build_dag_status_cards(theme)

    # ── Trend figures ─────────────────────────────────────────────────────────
    ph_30 = (
        PIPELINE_HEALTH.filter(pl.col("run_date") >= date.today() - timedelta(days=29))
        if not PIPELINE_HEALTH.is_empty() else PIPELINE_HEALTH
    )
    th_14 = (
        DBT_TEST_HEALTH.filter(pl.col("run_date") >= date.today() - timedelta(days=13))
        if not DBT_TEST_HEALTH.is_empty() else DBT_TEST_HEALTH
    )

    return (
        kpi_cards,
        dag_status_row,
        efig.dag_success_rate_line(ph_30, days=30, template=template),
        efig.runtime_trend_line(ph_30, days=30, template=template),
        efig.test_volume_bar(th_14, days=14, template=template),
        efig.test_pass_rate_table(DBT_TEST_HEALTH, template=template),
    )


def _build_dag_status_cards(theme: str) -> dbc.Row:
    """Build per-DAG inline status cards from PIPELINE_SUMMARY."""
    _DAG_ORDER  = ["ingest_permits", "ingest_evictions", "ingest_incidents"]
    _DAG_LABELS = {"ingest_permits": "Permits", "ingest_evictions": "Evictions", "ingest_incidents": "Incidents"}

    if PIPELINE_SUMMARY.is_empty():
        return dbc.Row()

    cols = []
    for dag_id in _DAG_ORDER:
        row = PIPELINE_SUMMARY.filter(pl.col("dag_id") == dag_id)
        if row.is_empty():
            continue
        r = row.row(0, named=True)
        status   = r.get("last_run_status", "healthy")
        rate     = r.get("success_rate_7d") or 0.0
        dur_sec  = r.get("avg_duration_7d_seconds")
        records  = r.get("total_records_7d")
        last_dt  = r.get("last_run_date")

        if status == "healthy":
            border_style = "3px solid #16a34a" if theme == "light" else "3px solid #4caf66"
            badge_color  = "success"
        elif status == "degraded":
            border_style = "3px solid #C4820A" if theme == "light" else "3px solid #ffb300"
            badge_color  = "warning"
        else:
            border_style = "3px solid #dc2626" if theme == "light" else "3px solid #ef5350"
            badge_color  = "danger"

        dur_str  = _fmt_dur(dur_sec)
        rec_str  = _fmt_int(records)
        last_str = str(last_dt)[:10] if last_dt else "—"

        cols.append(dbc.Col(
            dbc.Card(
                dbc.CardBody(
                    [
                        html.Div(
                            [
                                html.Strong(_DAG_LABELS.get(dag_id, dag_id)),
                                dbc.Badge(status.title(), color=badge_color, className="ms-2"),
                            ],
                            className="d-flex align-items-center mb-1",
                        ),
                        html.Div(
                            f"Last run: {last_str} · {dur_str} · {rec_str} records",
                            className="text-muted",
                            style={"fontSize": "0.75rem"},
                        ),
                        html.Div(
                            f"{rate:.1f}% 7-day success rate",
                            style={"fontSize": "1.1rem", "fontWeight": 700, "marginTop": "4px"},
                        ),
                    ]
                ),
                style={"borderLeft": border_style},
                className="shadow-sm h-100",
            ),
            md=4,
        ))

    return dbc.Row(cols, className="g-3")
