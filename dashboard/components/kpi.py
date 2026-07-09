"""KPI card builder."""

from __future__ import annotations

import dash_bootstrap_components as dbc
from dash import html

POSITIVE = "#4caf66"
NEGATIVE = "#ef5350"
MUTED = "#664d00"


def build_kpi_card(
    label: str,
    value: str,
    delta_pct: float | None,
    accent: str,
    *,
    higher_is_better: bool = True,
) -> dbc.Card:
    """One headline metric with a YoY delta arrow.

    `higher_is_better=False` flips the color logic for "median days to
    issue" — fewer days is good.
    """
    if delta_pct is None:
        delta_node = html.Span(
            "— vs. prior 12mo", style={"color": MUTED, "fontSize": "0.85rem"}
        )
    else:
        up = delta_pct > 0
        good = up if higher_is_better else not up
        arrow = "▲" if up else "▼"
        color = POSITIVE if good else NEGATIVE
        delta_node = html.Span(
            f"{arrow} {abs(delta_pct):.1f}% vs. prior 12mo",
            style={"color": color, "fontSize": "0.85rem", "fontWeight": 600},
        )

    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    label,
                    style={
                        "color": MUTED,
                        "fontFamily": "inherit",
                        "fontSize": "0.65rem",
                        "textTransform": "uppercase",
                        "letterSpacing": "0.15em",
                    },
                ),
                html.Div(
                    value,
                    style={
                        "fontSize": "1.8rem",
                        "fontWeight": 700,
                        "color": accent,
                        "lineHeight": 1.1,
                        "margin": "0.25rem 0",
                        "textShadow": f"0 0 10px {accent}66",
                    },
                ),
                delta_node,
            ]
        ),
        className="",
        style={
            "background": "#0f0c00",
            "border": "1px solid #2a1f00",
            "height": "100%",
        },
    )
