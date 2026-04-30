"""SF Urban Health dashboard — multi-page shell."""

from __future__ import annotations

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, callback, dcc, html

from dashboard.pages import housing, incidents

app = dash.Dash(
    __name__,
    title="SF Urban Health",
    external_stylesheets=[dbc.themes.FLATLY],
    suppress_callback_exceptions=True,
)
server = app.server  # WSGI handle for gunicorn

_NAVBAR = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("Housing Production", href="/")),
        dbc.NavItem(dbc.NavLink("Public Safety Incidents", href="/incidents")),
    ],
    brand="SF Urban Health",
    brand_href="/",
    color="primary",
    dark=True,
    fluid=True,
)

app.layout = html.Div([
    dcc.Location(id="url", refresh=False),
    _NAVBAR,
    html.Div(id="page-content"),
])


@callback(Output("page-content", "children"), Input("url", "pathname"))
def display_page(pathname):
    if pathname == "/incidents":
        return incidents.layout
    return housing.layout


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=True)
