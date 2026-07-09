"""SF Urban Health dashboard — multi-page shell."""

from __future__ import annotations

import os

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, State, callback, clientside_callback, dcc, html

import dashboard.components.figures  # registers Plotly templates for all pages  # noqa: F401
from dashboard.pages import (
    data_trust,
    engineer_health,
    evictions,
    housing,
    incidents,
    pipeline_health,
)

app = dash.Dash(
    __name__,
    title="SF Urban Health",
    external_stylesheets=[dbc.themes.FLATLY],
    suppress_callback_exceptions=True,
)
server = app.server  # WSGI handle for gunicorn

# ── Golden Gate Bridge SVG brand icon ─────────────────────────────────────────
_BRIDGE_ICON = html.Div(
    html.Img(src="/assets/bridge.svg", width=24, height=20, alt="SF"),
    style={
        "width": "36px",
        "height": "36px",
        "borderRadius": "8px",
        "background": "#FDB515",
        "display": "flex",
        "alignItems": "center",
        "justifyContent": "center",
        "flexShrink": 0,
        "boxShadow": "0 2px 6px rgba(253,181,21,0.4)",
    },
)

_NAVBAR = dbc.Navbar(
    dbc.Container(
        [
            # Brand
            dbc.NavbarBrand(
                html.Div(
                    [
                        _BRIDGE_ICON,
                        html.Div(
                            [
                                html.Div(
                                    "SF Urban Health",
                                    style={
                                        "fontWeight": 700,
                                        "fontSize": "13px",
                                        "letterSpacing": "0.02em",
                                    },
                                ),
                                html.Div(
                                    "Data Platform",
                                    style={"fontSize": "11px", "opacity": 0.7},
                                ),
                            ],
                            style={"marginLeft": "10px"},
                        ),
                    ],
                    style={"display": "flex", "alignItems": "center"},
                ),
                href="/",
            ),
            dbc.NavbarToggler(id="navbar-toggler"),
            dbc.Collapse(
                dbc.Nav(
                    [
                        dbc.NavItem(dbc.NavLink("Housing", href="/")),
                        dbc.NavItem(dbc.NavLink("Incidents", href="/incidents")),
                        dbc.NavItem(dbc.NavLink("Evictions", href="/evictions")),
                        dbc.NavItem(dbc.NavLink("Pipeline Health", href="/pipeline")),
                        dbc.NavItem(dbc.NavLink("Eng Health", href="/engineer")),
                        dbc.NavItem(dbc.NavLink("Data Trust", href="/data-trust")),
                        # ── Theme toggle ──
                        dbc.NavItem(
                            dbc.Button(
                                "☀ Light",
                                id="theme-btn",
                                size="sm",
                                outline=True,
                                color="light",
                                className="ms-3",
                                style={"fontSize": "12px", "padding": "3px 10px"},
                            ),
                            className="d-flex align-items-center",
                        ),
                    ],
                    navbar=True,
                    className="ms-auto",
                ),
                id="navbar-collapse",
                navbar=True,
            ),
        ],
        fluid=True,
    ),
    color="primary",
    dark=True,
    style={"borderBottom": "3px solid #FDB515"},
)

app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        # Persists chosen theme in browser localStorage
        dcc.Store(id="theme-store", storage_type="local", data="dark"),
        # Dummy output for the clientside theme-class callback
        html.Div(id="_theme-body-class", style={"display": "none"}),
        _NAVBAR,
        html.Div(id="page-content"),
    ],
    id="app-shell",
)

# ── Apply theme class to <body> via clientside callback ────────────────────────
clientside_callback(
    """
    function(theme) {
        document.body.className = theme === 'light' ? 'light-mode' : '';
        return '';
    }
    """,
    Output("_theme-body-class", "children"),
    Input("theme-store", "data"),
)


# ── Theme toggle button ────────────────────────────────────────────────────────
@callback(
    Output("theme-store", "data"),
    Input("theme-btn", "n_clicks"),
    State("theme-store", "data"),
    prevent_initial_call=True,
)
def toggle_theme(_, current: str) -> str:
    return "light" if current == "dark" else "dark"


@callback(
    Output("theme-btn", "children"),
    Input("theme-store", "data"),
)
def update_btn_label(theme: str) -> str:
    return "☀ Light" if theme == "dark" else "🌙 Dark"


# ── Page routing ───────────────────────────────────────────────────────────────
@callback(Output("page-content", "children"), Input("url", "pathname"))
def display_page(pathname: str):
    if pathname == "/incidents":
        return incidents.layout
    if pathname == "/evictions":
        return evictions.layout
    if pathname == "/pipeline":
        return pipeline_health.layout
    if pathname == "/engineer":
        return engineer_health.layout
    if pathname == "/data-trust":
        return data_trust.layout
    return housing.layout


if __name__ == "__main__":
    # debug=True exposes the interactive traceback / dev tools, which leak
    # source lines and stack traces. Keep it opt-in via DASH_DEBUG=1.
    debug = os.environ.get("DASH_DEBUG") == "1"
    app.run(host="0.0.0.0", port="8050", debug=debug)
