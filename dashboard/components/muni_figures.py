"""MUNI vehicle map figure builder."""

from __future__ import annotations

import math

import plotly.graph_objects as go

# SFMTA light-rail Metro lines
_RAIL_ROUTES = {"J", "K", "L", "M", "N", "T", "KT"}

# Cable car lines (511 returns these names)
_CABLE_CAR_ROUTES = {
    "California", "Powell/Hyde", "Powell/Mason",
    "CAL", "PM", "PH", "Powell-Hyde", "Powell-Mason",
}

# Transit type → Scattermapbox marker style + legend label + emoji prefix for buttons
_TYPE_STYLES = {
    "MUNI Rail": {
        "color": "#42a5f5",
        "symbol": "square",
        "size": 14,
        "line_color": "#ffffff",
        "line_width": 2,
        "label": "MUNI Rail",
        "emoji": "🚊",
    },
    "Cable Car": {
        "color": "#ef5350",
        "symbol": "star",
        "size": 20,
        "line_color": "#ffb300",
        "line_width": 2,
        "label": "Cable Car",
        "emoji": "🚡",
    },
    "MUNI Bus": {
        "color": "#ffb300",
        "symbol": "circle",
        "size": 12,
        "line_color": "#0d0d0d",
        "line_width": 2,
        "label": "MUNI Bus",
        "emoji": "🚌",
    },
}

_NEARBY_RADIUS_MILES = 1.5


def transit_type(route: str | None) -> str:
    """Classify a route string into Rail / Cable Car / Bus."""
    r = (route or "").strip()
    if r.upper() in {x.upper() for x in _RAIL_ROUTES}:
        return "MUNI Rail"
    if r in _CABLE_CAR_ROUTES or r.upper() in {x.upper() for x in _CABLE_CAR_ROUTES}:
        return "Cable Car"
    return "MUNI Bus"


def route_emoji(route: str | None) -> str:
    return _TYPE_STYLES[transit_type(route)]["emoji"]


def _haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    r = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _user_pin_trace(lat: float, lng: float) -> go.Scattermapbox:
    return go.Scattermapbox(
        lat=[lat],
        lon=[lng],
        mode="markers",
        name="Address",
        marker=dict(
            symbol="circle",
            size=18,
            color="#ffffff",
            opacity=1.0,
            allowoverlap=True,
        ),
        text=["Searched address"],
        hoverinfo="text",
    )


def _route_path_trace(
    path: list[tuple[float, float]], color: str
) -> go.Scattermapbox:
    return go.Scattermapbox(
        lat=[p[0] for p in path],
        lon=[p[1] for p in path],
        mode="lines",
        name="Route path",
        line=dict(width=3, color=color),
        opacity=0.45,
        hoverinfo="skip",
    )


def _base_map(center_lat: float, center_lng: float, zoom: int = 15) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        mapbox=dict(
            style="carto-darkmatter",
            center=dict(lat=center_lat, lon=center_lng),
            zoom=zoom,
        ),
        paper_bgcolor="#0d0d0d",
        margin=dict(l=0, r=0, t=0, b=0),
        height=450,
    )
    return fig


def placeholder_map(center_lat: float, center_lng: float) -> go.Figure:
    """Empty dark map with user pin shown before any route is selected."""
    fig = _base_map(center_lat, center_lng, zoom=15)
    fig.add_trace(_user_pin_trace(center_lat, center_lng))
    fig.add_annotation(
        text="SELECT A ROUTE TO TRACK VEHICLES",
        xref="paper", yref="paper",
        x=0.5, y=0.05,
        showarrow=False,
        font=dict(
            family="'Share Tech Mono', monospace",
            size=13,
            color="#664d00",
        ),
    )
    fig.update_layout(
        legend=dict(
            bgcolor="#0f0c00",
            bordercolor="#2a1f00",
            font=dict(color="#ffb300", family="'Share Tech Mono', monospace", size=11),
            x=0.01, y=0.99,
            xanchor="left", yanchor="top",
        ),
    )
    return fig


def vehicle_map(
    vehicles: list[dict],
    center_lat: float,
    center_lng: float,
    route: str = "",
    path: list[tuple[float, float]] | None = None,
) -> go.Figure:
    nearby = [
        v for v in vehicles
        if _haversine_miles(center_lat, center_lng, v["lat"], v["lng"]) <= _NEARBY_RADIUS_MILES
    ]

    ttype = transit_type(route)
    style = _TYPE_STYLES[ttype]

    _legend = dict(
        bgcolor="#0f0c00",
        bordercolor="#2a1f00",
        font=dict(color="#ffb300", family="'Share Tech Mono', monospace", size=11),
        x=0.01, y=0.99,
        xanchor="left", yanchor="top",
    )

    if not nearby:
        fig = _base_map(center_lat, center_lng, zoom=15)
        if path:
            fig.add_trace(_route_path_trace(path, style["color"]))
        fig.add_trace(_user_pin_trace(center_lat, center_lng))
        fig.add_annotation(
            text=f"NO VEHICLES NEAR YOU ON {route.upper()}",
            xref="paper", yref="paper",
            x=0.5, y=0.05,
            showarrow=False,
            font=dict(family="'Share Tech Mono', monospace", size=12, color="#664d00"),
        )
        fig.update_layout(legend=_legend)
        return fig

    hover = [f"{style['emoji']} {v['route']} → {v['destination']}" for v in nearby]

    fig = go.Figure()
    if path:
        fig.add_trace(_route_path_trace(path, style["color"]))
    fig.add_trace(go.Scattermapbox(
        lat=[v["lat"] for v in nearby],
        lon=[v["lng"] for v in nearby],
        mode="markers",
        name=f"{style['emoji']} {style['label']}",
        marker=dict(
            symbol=style["symbol"],
            size=style["size"],
            color=style["color"],
            opacity=0.95,
            allowoverlap=True,
        ),
        text=hover,
        hoverinfo="text",
    ))
    fig.add_trace(_user_pin_trace(center_lat, center_lng))

    fig.update_layout(
        mapbox=dict(
            style="carto-darkmatter",
            center=dict(lat=center_lat, lon=center_lng),
            zoom=15,
        ),
        paper_bgcolor="#0d0d0d",
        plot_bgcolor="#0d0d0d",
        margin=dict(l=0, r=0, t=0, b=0),
        height=450,
        legend=_legend,
        font=dict(color="#ffb300", family="'Share Tech Mono', monospace"),
    )
    return fig
