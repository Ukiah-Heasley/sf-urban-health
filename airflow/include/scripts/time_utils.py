"""Shared timestamp parsing and normalization helpers."""

from __future__ import annotations

from datetime import date, datetime, time as datetime_time, timezone


def coerce_utc_datetime(value: date | datetime | str) -> datetime:
    """Normalize date-like inputs to timezone-aware UTC datetimes.

    Inputs may come from argparse strings, Airflow pendulum datetimes, Python
    dates, or source-system timestamp strings. The output is always a
    ``datetime`` with ``timezone.utc`` attached.
    """
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, datetime_time.min, tzinfo=timezone.utc)
    elif isinstance(value, str):
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise TypeError(f"Unsupported datetime type: {type(value).__name__}")

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)
