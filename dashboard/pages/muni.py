"""MUNI real-time departure board — /muni"""

from __future__ import annotations

from datetime import datetime, timezone

import dash
import dash_bootstrap_components as dbc
from dash import ALL, Input, Output, State, callback, ctx, dcc, html

from dashboard.components.muni_figures import (
    placeholder_map,
    route_emoji,
    transit_type,
    vehicle_map,
)
from dashboard.data.muni import (
    fetch_arrivals_parallel,
    fetch_route_path,
    fetch_vehicle_positions,
    geocode,
    nearby_stops,
)

_DEFAULT_ADDRESS = "1718 Steiner St, San Francisco, CA"
_SF_LAT, _SF_LNG = 37.7749, -122.4194

_DISTANCE_MARKS = {0.1: "0.1 mi", 0.25: "¼ mi", 0.5: "½ mi", 1.0: "1 mi"}

_BTN_BASE = {
    "border": "none",
    "borderRadius": 0,
    "fontFamily": "inherit",
    "fontSize": "0.75rem",
    "padding": "0.2rem 0.5rem",
    "marginRight": "0.3rem",
    "marginBottom": "0.3rem",
    "cursor": "pointer",
}
_BTN_RAIL   = {**_BTN_BASE, "background": "#42a5f5", "color": "#0d0d0d"}
_BTN_CABLE  = {**_BTN_BASE, "background": "#ef5350", "color": "#ffffff"}
_BTN_BUS    = {**_BTN_BASE, "background": "#ffb300", "color": "#0d0d0d"}

_TYPE_BTN_STYLE = {"MUNI Rail": _BTN_RAIL, "Cable Car": _BTN_CABLE, "MUNI Bus": _BTN_BUS}

layout = dbc.Container(
    [
        dcc.Interval(id="muni-interval", interval=60_000, n_intervals=0),
        dcc.Store(id="muni-stops-store"),
        dcc.Store(id="muni-geo-store"),
        dcc.Store(id="muni-selected-route"),
        html.Div(
            [
                html.H2("SF MUNI", className="mb-0"),
                html.Div("Real-Time Departures", className="text-muted"),
            ],
            className="py-4",
        ),
        # Settings row
        dbc.Row(
            [
                dbc.Col(
                    [
                        dbc.Label("Address"),
                        dcc.Input(
                            id="muni-address",
                            type="text",
                            value=_DEFAULT_ADDRESS,
                            debounce=True,
                            className="form-control",
                            placeholder="Street address, San Francisco",
                        ),
                        html.Div(id="muni-geo-preview", className="mt-1"),
                    ],
                    md=5,
                ),
                dbc.Col(
                    [
                        dbc.Label("Search radius"),
                        dcc.Slider(
                            id="muni-distance-slider",
                            min=0.1, max=1.0, step=None,
                            marks=_DISTANCE_MARKS,
                            value=0.25,
                            className="mt-2",
                        ),
                    ],
                    md=4,
                ),
                dbc.Col(
                    [
                        dbc.Label(" "),
                        dbc.Button(
                            "Apply",
                            id="muni-apply",
                            className="d-block w-100",
                            style={
                                "background": "#ffb300", "color": "#0d0d0d",
                                "border": "none", "borderRadius": 0,
                                "fontFamily": "inherit", "letterSpacing": "0.1em",
                            },
                        ),
                    ],
                    md=3, className="d-flex flex-column",
                ),
            ],
            className="mb-2 align-items-end",
        ),
        html.Div(id="muni-stops-list", className="mb-2"),
        # Route selector strip — populated after Apply
        html.Div(id="muni-route-selector", className="mb-3"),
        # Map + board
        dbc.Row(
            [
                dbc.Col(
                    dcc.Graph(id="muni-map", config={"displayModeBar": False}),
                    md=7,
                ),
                dbc.Col(html.Div(id="muni-board-content"), md=5),
            ],
            className="g-3",
        ),
    ],
    fluid=True,
    style={"maxWidth": "1400px"},
)


# ── Address geocode preview ────────────────────────────────────────────────────

@callback(
    Output("muni-geo-preview", "children"),
    Output("muni-geo-store", "data"),
    Input("muni-address", "value"),
)
def preview_geocode(address):
    if not address or not address.strip():
        return html.Div("Enter an address above.", className="text-muted",
                        style={"fontSize": "0.8rem"}), None

    coords = geocode(address.strip())
    if coords is None:
        return (
            html.Div("⚠ Could not find this address — check spelling.",
                     className="muni-error", style={"fontSize": "0.8rem"}),
            None,
        )

    lat, lng = coords
    return (
        html.Div(f"✓ Resolved → {lat:.5f}, {lng:.5f}",
                 style={"color": "#4caf66", "fontSize": "0.8rem"}),
        {"lat": lat, "lng": lng},
    )


# ── Apply: find nearby stops ───────────────────────────────────────────────────

@callback(
    Output("muni-stops-store", "data"),
    Output("muni-stops-list", "children"),
    Input("muni-apply", "n_clicks"),
    State("muni-address", "value"),
    State("muni-distance-slider", "value"),
    prevent_initial_call=True,
)
def apply_settings(_, address, distance_miles):
    if not address or not address.strip():
        return [], html.Div("Enter an address first.", className="text-muted")

    coords = geocode(address.strip())
    if coords is None:
        return [], html.Div("Could not geocode address.", className="muni-error")

    lat, lng = coords
    distance = distance_miles or 0.25
    stops = nearby_stops(lat, lng, radius_miles=distance)

    if not stops:
        label = _DISTANCE_MARKS.get(distance, f"{distance} mi")
        return [], html.Div(
            f"No MUNI stops found within {label}. Try a larger radius.",
            className="muni-error",
        )

    stop_labels = ", ".join(f"{s['name']} ({s['distance_ft']:,} ft)" for s in stops[:5])
    more = f" + {len(stops) - 5} more" if len(stops) > 5 else ""
    return stops, html.Div(
        f"Loaded {len(stops)} stops — {stop_labels}{more}",
        style={"color": "#664d00", "fontSize": "0.85rem"},
    )


# ── Arrival board + route button strip ────────────────────────────────────────

@callback(
    Output("muni-board-content", "children"),
    Output("muni-route-selector", "children"),
    Input("muni-stops-store", "data"),
    Input("muni-interval", "n_intervals"),
)
def refresh_board(stops, _n):
    if not stops:
        return (
            html.Div("Click Apply to load departures.", className="text-muted"),
            None,
        )

    now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
    stops_with_rows: list[tuple[str, list[dict]]] = []
    seen_routes: dict[str, str] = {}  # route → transit_type, insertion-ordered

    arrival_map = fetch_arrivals_parallel(stops)
    for stop in stops:
        rows = arrival_map.get(stop["stop_id"])
        if rows:
            stops_with_rows.append((stop["name"], rows))
            for r in rows:
                rt = r.get("route") or "?"
                if rt not in seen_routes:
                    seen_routes[rt] = transit_type(rt)

    children: list = []
    if not stops_with_rows:
        children.append(html.Div("No upcoming arrivals.", className="text-muted"))
    else:
        children.append(_build_table(stops_with_rows, now_str))

    # Route button strip
    route_buttons = _build_route_buttons(seen_routes)

    return html.Div(children), route_buttons


# ── Route selection → map ─────────────────────────────────────────────────────

@callback(
    Output("muni-selected-route", "data"),
    Input({"type": "muni-route-btn", "route": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def select_route(n_clicks_list):
    triggered = ctx.triggered_id
    if not triggered or not any(n for n in n_clicks_list if n):
        return dash.no_update
    return triggered["route"]


@callback(
    Output("muni-map", "figure"),
    Input("muni-selected-route", "data"),
    State("muni-geo-store", "data"),
)
def refresh_map(route, geo):
    lat = geo["lat"] if geo else _SF_LAT
    lng = geo["lng"] if geo else _SF_LNG

    if not route:
        return placeholder_map(lat, lng)

    vehicles = fetch_vehicle_positions(route)
    if vehicles is None:
        vehicles = []
    path = fetch_route_path(route)
    return vehicle_map(vehicles, lat, lng, route=route, path=path)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_route_buttons(seen_routes: dict[str, str]) -> html.Div | None:
    if not seen_routes:
        return None

    buttons = []
    for route, ttype in seen_routes.items():
        emoji = route_emoji(route)
        style = _TYPE_BTN_STYLE.get(ttype, _BTN_BUS)
        buttons.append(
            html.Button(
                f"{emoji} {route}",
                id={"type": "muni-route-btn", "route": route},
                n_clicks=0,
                style=style,
                title=f"Track {route} on map",
            )
        )

    return html.Div(
        [
            html.Div(
                "TRACK ON MAP",
                style={"color": "#664d00", "fontSize": "0.65rem",
                       "letterSpacing": "0.15em", "marginBottom": "0.4rem"},
            ),
            html.Div(buttons, style={"display": "flex", "flexWrap": "wrap"}),
        ]
    )


def _build_table(
    stops_with_rows: list[tuple[str, list[dict]]], now_str: str
) -> html.Div:
    rows: list = [
        html.Div(
            f"UPDATED {now_str}",
            style={"color": "#664d00", "fontSize": "0.7rem",
                   "letterSpacing": "0.1em", "marginBottom": "0.5rem"},
        )
    ]
    for stop_name, arrivals in stops_with_rows:
        rows.append(html.Div(stop_name, className="muni-stop-header"))
        for arr in arrivals:
            mins = arr["minutes"]
            mins_class = "muni-mins muni-mins-now" if mins <= 1 else "muni-mins"
            mins_text = "NOW" if mins <= 1 else str(mins)
            status_class = {
                "Delayed": "muni-status-delayed",
                "Early": "muni-status-early",
                "On Time": "muni-status-ok",
            }.get(arr["status"], "muni-status-ok")
            emoji = route_emoji(arr.get("route"))
            rows.append(
                dbc.Row(
                    [
                        dbc.Col(
                            html.Span(f"{emoji} {arr['route']}", className="muni-route"),
                            width=3,
                        ),
                        dbc.Col(arr["destination"], width=4,
                                style={"fontSize": "0.8rem"}),
                        dbc.Col(html.Span(mins_text, className=mins_class), width=2),
                        dbc.Col(
                            html.Span(arr["status"], className=status_class),
                            width=3, style={"fontSize": "0.7rem"},
                        ),
                    ],
                    className="align-items-center py-1",
                    style={"borderBottom": "1px solid #1a1200"},
                )
            )
    return html.Div(rows)
