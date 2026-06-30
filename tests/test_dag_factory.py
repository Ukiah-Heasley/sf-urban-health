"""Smoke tests for the ingest-DAG factory.

Skipped automatically when the `airflow` dependency group isn't installed
(local `make test` with only `--group dev` will auto-skip).
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

pytest.importorskip("airflow.models", reason="airflow not installed; install --group airflow")


def test_make_ingest_dag_extracts_raw_and_emits_asset():
    from airflow.timetables.interval import CronDataIntervalTimetable
    from _shared.dag_factory import DagConfig, make_ingest_dag
    from _shared.pipeline_assets import PERMITS_INGEST_ASSET
    from scripts.soda_ingest import DatasetConfig

    cfg = DagConfig(
        dataset=DatasetConfig(
            name="permits",
            dataset_id="i98e-djp9",
            date_field="data_loaded_at",
            order_field="permit_number",
            epoch=date(2013, 1, 1),
        ),
        schedule="0 6 * * *",
        start_date=datetime(2026, 5, 1),
        tags=["test"],
    )
    dag = make_ingest_dag(cfg)

    assert dag.dag_id == "ingest_permits"
    assert isinstance(dag.timetable, CronDataIntervalTimetable)
    task_ids = [t.task_id for t in dag.tasks]
    assert task_ids == [
        "extract_permits_to_raw",
        "record_permits_extract_metadata",
        "ingest_complete",
    ]

    extract = dag.get_task("extract_permits_to_raw")
    record_metadata = dag.get_task("record_permits_extract_metadata")
    complete = dag.get_task("ingest_complete")

    assert record_metadata.upstream_task_ids == {"extract_permits_to_raw"}
    assert complete.upstream_task_ids == {"record_permits_extract_metadata"}
    assert extract.downstream_task_ids == {"record_permits_extract_metadata"}
    assert record_metadata.downstream_task_ids == {"ingest_complete"}
    assert complete.outlets == [PERMITS_INGEST_ASSET]


def test_resolve_extract_window_uses_scheduled_interval_by_default():
    from _shared.dag_factory import resolve_extract_window

    start = datetime(2024, 3, 15, 6, tzinfo=timezone.utc)
    end = datetime(2024, 3, 16, 6, tzinfo=timezone.utc)

    window = resolve_extract_window(
        data_interval_start=start,
        data_interval_end=end,
        dag_run_conf={},
    )

    assert window.data_interval_start == start
    assert window.data_interval_end == end
    assert window.lookback == timedelta(0)
    assert window.effective_start == start


@pytest.mark.parametrize("load_mode", ["full", "backfill"])
def test_resolve_extract_window_manual_modes(load_mode: str):
    from _shared.dag_factory import resolve_extract_window

    scheduled_start = datetime(2024, 3, 15, 6, tzinfo=timezone.utc)
    scheduled_end = datetime(2024, 3, 16, 6, tzinfo=timezone.utc)
    window_start = datetime(2018, 1, 1, tzinfo=timezone.utc)
    window_end = datetime(2026, 6, 29, tzinfo=timezone.utc)

    window = resolve_extract_window(
        data_interval_start=scheduled_start,
        data_interval_end=scheduled_end,
        dag_run_conf={
            "load_mode": load_mode,
            "window_start": "2018-01-01T00:00:00Z",
            "window_end": "2026-06-29T00:00:00Z",
            "lookback_hours": 2,
        },
    )

    assert window.data_interval_start == window_start
    assert window.data_interval_end == window_end
    assert window.effective_start == window_start - timedelta(hours=2)


def test_resolve_extract_window_requires_both_bounds():
    from _shared.dag_factory import resolve_extract_window

    with pytest.raises(ValueError, match="supplied together"):
        resolve_extract_window(
            data_interval_start=datetime(2024, 3, 15, 6, tzinfo=timezone.utc),
            data_interval_end=datetime(2024, 3, 16, 6, tzinfo=timezone.utc),
            dag_run_conf={"load_mode": "full", "window_start": "2018-01-01T00:00:00Z"},
        )


def test_resolve_extract_window_rejects_manual_window_without_load_mode():
    from _shared.dag_factory import resolve_extract_window

    with pytest.raises(ValueError, match="load_mode is required"):
        resolve_extract_window(
            data_interval_start=datetime(2024, 3, 15, 6, tzinfo=timezone.utc),
            data_interval_end=datetime(2024, 3, 16, 6, tzinfo=timezone.utc),
            dag_run_conf={
                "window_start": "2018-01-01T00:00:00Z",
                "window_end": "2026-06-29T00:00:00Z",
            },
        )


def test_resolve_extract_window_rejects_negative_lookback():
    from _shared.dag_factory import resolve_extract_window

    with pytest.raises(ValueError, match="lookback_hours"):
        resolve_extract_window(
            data_interval_start=datetime(2024, 3, 15, 6, tzinfo=timezone.utc),
            data_interval_end=datetime(2024, 3, 16, 6, tzinfo=timezone.utc),
            dag_run_conf={
                "load_mode": "backfill",
                "window_start": "2018-01-01T00:00:00Z",
                "window_end": "2026-06-29T00:00:00Z",
                "lookback_hours": -1,
            },
        )
