"""Shared theme helpers consumed by every figure-builder module."""
from __future__ import annotations

# ── Per-theme palettes ─────────────────────────────────────────────────────────
# Light = Cal Bear: Cal Blue, Medalist Gold, Founders Rock, green, red, burnt orange
_CAL = ["#003262", "#C4820A", "#3B7EA1", "#16a34a", "#dc2626", "#D9661F"]
# Dark = terminal amber: existing palette preserved for existing charts
_AMBER = ["#42a5f5", "#ffb300", "#4caf66", "#ef5350", "#ab47bc", "#26c6da"]


def base_layout(template: str, **overrides) -> dict:
    """Return a base Plotly layout dict for *template*.

    Caller can override individual keys via keyword arguments.
    """
    if template == "cal_light":
        base: dict = dict(
            template="cal_light",
            margin=dict(l=75, r=20, t=50, b=40),
            font=dict(
                family="-apple-system, 'Segoe UI', Roboto, sans-serif",
                size=11,
                color="#4A3F28",
            ),
            title_font=dict(size=13, color="#003262"),
        )
    else:
        base = dict(
            template="terminal_amber",
            margin=dict(l=75, r=20, t=50, b=40),
            font=dict(family="'Share Tech Mono', monospace", size=11),
            title_font=dict(size=13, color="#886600"),
        )
    base.update(overrides)
    return base


def palette(template: str) -> list[str]:
    return _CAL if template == "cal_light" else _AMBER


def empty_color(template: str) -> str:
    return "#9A8B6E" if template == "cal_light" else "#664d00"


def grid_color(template: str) -> str:
    return "#E0D8C4" if template == "cal_light" else "#1f1800"


def table_header(template: str) -> dict:
    if template == "cal_light":
        return dict(fill_color="#F2EEE3", font_color="#003262", line_color="#E0D8C4")
    return dict(fill_color="#1a1200", font_color="#ffb300", line_color="#2a1f00")


def table_cells(template: str) -> dict:
    if template == "cal_light":
        return dict(fill_color="#FFFFFF", font_color="#4A3F28", line_color="#E0D8C4")
    return dict(fill_color="#0f0c00", font_color="#ffe066", line_color="#2a1f00")


def ok_color(template: str) -> str:
    return "#16a34a" if template == "cal_light" else "#4caf66"


def bad_color(template: str) -> str:
    return "#dc2626" if template == "cal_light" else "#ef5350"


def warn_color(template: str) -> str:
    return "#C4820A" if template == "cal_light" else "#ffb300"
