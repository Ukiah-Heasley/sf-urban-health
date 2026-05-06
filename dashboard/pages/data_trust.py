"""Data Quality & Freshness Dashboard — /data-trust
Analyst-facing view: per-dataset trust scores, freshness calendar,
and test pass rate trends. Connected to mart_data_trust and
mart_dbt_test_health / mart_pipeline_health.
"""
from __future__ import annotations

import dash_bootstrap_components as dbc
import polars as pl
from dash import Input, Output, callback, dcc, html

from dashboard.components import trust_figures as tfig
from dashboard.components import theme_utils as tu
from dashboard.data.cache import DATA_TRUST, DBT_TEST_HEALTH, PIPELINE_HEALTH
from dashboard.data.trust_transforms import (
    freshness_grid,
    recent_test_failures,
    staging_test_trend,
)

layout = dbc.Container(
    [
        html.Div(
            [
                html.Div(
                    [
                        html.H2("Data Quality & Freshness", className="mb-0"),
                        dbc.Badge(
                            "For Analysts & Business Users",
                            color="info",
                            className="ms-2 align-middle",
                            style={"verticalAlign": "middle"},
                        ),
                    ],
                    className="d-flex align-items-center",
                ),
                html.Div(
                    "Per-dataset trust scores, freshness status, and dbt test health  ·  "
                    "Sources: mart_data_trust · mart_dbt_test_health",
                    className="text-muted mt-1",
                    style={"fontSize": "0.8rem"},
                ),
            ],
            className="py-4",
        ),

        # ── Trust cards ───────────────────────────────────────────────────────
        html.Div(id="dt-trust-cards", className="mb-4"),

        # ── Freshness calendar + test trend ───────────────────────────────────
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(dbc.CardBody(dcc.Graph(id="dt-fig-calendar")), className="shadow-sm h-100"),
                    md=6,
                ),
                dbc.Col(
                    dbc.Card(dbc.CardBody(dcc.Graph(id="dt-fig-test-trend")), className="shadow-sm h-100"),
                    md=6,
                ),
            ],
            className="g-3 mb-4",
        ),

        # ── Failure table ─────────────────────────────────────────────────────
        dbc.Card(
            dbc.CardBody(dcc.Graph(id="dt-fig-failures")),
            className="mb-5 shadow-sm",
        ),
    ],
    fluid=True,
    style={"maxWidth": "1400px"},
)


def _status_color(status: str, theme: str) -> str:
    light = theme == "light"
    if status == "trusted":
        return "#16a34a" if light else "#4caf66"
    if status == "degraded":
        return "#C4820A" if light else "#ffb300"
    return "#dc2626" if light else "#ef5350"


def _freshness_badge(status: str) -> tuple[str, str]:
    """Returns (label, color) for dbc.Badge."""
    if status == "fresh":
        return "● Fresh", "success"
    if status == "stale":
        return "⚠ Stale", "warning"
    return "✗ Critical", "danger"


@callback(
    Output("dt-trust-cards",    "children"),
    Output("dt-fig-calendar",   "figure"),
    Output("dt-fig-test-trend", "figure"),
    Output("dt-fig-failures",   "figure"),
    Input("theme-store", "data"),
)
def refresh(theme: str):
    template = "cal_light" if theme == "light" else "terminal_amber"

    trust_cards = _build_trust_cards(theme, template)

    grid     = freshness_grid(PIPELINE_HEALTH)
    trend    = staging_test_trend(DBT_TEST_HEALTH)
    failures = recent_test_failures(DBT_TEST_HEALTH)

    return (
        trust_cards,
        tfig.freshness_calendar(grid, template=template),
        tfig.test_trend_line(trend, template=template),
        tfig.failure_table(failures, template=template),
    )


def _build_trust_cards(theme: str, template: str) -> dbc.Row:
    """Build one card per dataset row from DATA_TRUST."""
    if DATA_TRUST.is_empty():
        return dbc.Row(dbc.Col(
            dbc.Alert("mart_data_trust not yet loaded. Run make dbt-build.", color="warning"),
        ))

    _DATASET_ORDER = ["Permits", "Evictions", "Incidents"]
    cols = []

    for ds_name in _DATASET_ORDER:
        row = DATA_TRUST.filter(pl.col("dataset_name") == ds_name)
        if row.is_empty():
            continue
        r = row.row(0, named=True)

        score    = int(r.get("trust_score") or 0)
        status   = r.get("trust_status", "untrusted")
        fresh_s  = r.get("freshness_status", "critical")
        pass_rt  = r.get("test_pass_rate_7d")
        total_t  = int(r.get("total_tests_7d") or 0)
        failed_t = int(r.get("failed_tests_7d") or 0)
        records  = r.get("last_loaded_date")
        dag_id   = r.get("dag_id", "")

        score_color = _status_color(status, theme)
        fresh_label, fresh_badge_color = _freshness_badge(fresh_s)
        last_date_str = str(records)[:10] if records else "—"

        # Score ring (small donut)
        ring_fig = tfig.score_donut(score, status, template=template)

        # Card left-border color
        border = f"4px solid {score_color}"

        card = dbc.Col(
            dbc.Card(
                [
                    # Top color bar
                    html.Div(style={"height": "4px", "background": score_color}),
                    dbc.CardBody(
                        [
                            # Header: name + badge
                            html.Div(
                                [
                                    html.H5(ds_name, className="mb-0"),
                                    dbc.Badge(
                                        {"trusted": "✓ Trusted", "degraded": "⚠ Degraded"}.get(status, "✗ Untrusted"),
                                        color={"trusted": "success", "degraded": "warning"}.get(status, "danger"),
                                        className="ms-2",
                                    ),
                                ],
                                className="d-flex align-items-center mb-1",
                            ),
                            html.Div(dag_id, className="text-muted mb-3", style={"fontSize": "0.75rem"}),

                            # Score ring + metrics
                            html.Div(
                                [
                                    # Ring (centered)
                                    html.Div(
                                        [
                                            dcc.Graph(
                                                figure=ring_fig,
                                                config={"displayModeBar": False},
                                                style={"height": "130px", "width": "130px"},
                                            ),
                                            html.Div(
                                                str(score),
                                                style={
                                                    "position": "absolute", "top": "50%", "left": "50%",
                                                    "transform": "translate(-50%, -50%)",
                                                    "fontSize": "2rem", "fontWeight": 800,
                                                    "color": score_color, "lineHeight": 1,
                                                    "textAlign": "center",
                                                    "pointerEvents": "none",
                                                },
                                            ),
                                            html.Div(
                                                "trust score",
                                                style={
                                                    "position": "absolute", "top": "62%", "left": "50%",
                                                    "transform": "translateX(-50%)",
                                                    "fontSize": "0.6rem", "color": "#9A8B6E",
                                                    "textTransform": "uppercase", "letterSpacing": "0.05em",
                                                    "pointerEvents": "none",
                                                },
                                            ),
                                        ],
                                        style={"position": "relative", "width": "130px", "flexShrink": 0},
                                    ),
                                    # Metric grid
                                    html.Div(
                                        [
                                            html.Div(
                                                [
                                                    html.Div("Freshness", className="text-muted", style={"fontSize": "0.65rem", "textTransform": "uppercase", "letterSpacing": "0.07em"}),
                                                    dbc.Badge(fresh_label, color=fresh_badge_color, className="mt-1"),
                                                    html.Div(f"Last loaded {last_date_str}", className="text-muted mt-1", style={"fontSize": "0.7rem"}),
                                                ],
                                                style={"flex": 1},
                                            ),
                                        ],
                                        style={"flex": 1, "paddingLeft": "16px"},
                                    ),
                                ],
                                className="d-flex align-items-center",
                            ),
                        ]
                    ),
                    # Footer: test pass rate
                    html.Div(
                        [
                            html.Span("Test pass rate (7d)", className="text-muted", style={"fontSize": "0.75rem"}),
                            html.Span(
                                f"{pass_rt:.1f}%  ({failed_t} failures / {total_t} runs)" if pass_rt is not None else "—",
                                style={"fontWeight": 700, "color": score_color, "marginLeft": "auto"},
                            ),
                        ],
                        className="d-flex align-items-center px-3 py-2",
                        style={"borderTop": "1px solid var(--bs-border-color)"},
                    ),
                ],
                className="shadow-sm h-100",
                style={"overflow": "hidden"},
            ),
            md=4,
        )
        cols.append(card)

    return dbc.Row(cols, className="g-3")
