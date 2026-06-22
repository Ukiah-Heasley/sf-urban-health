"""Smoke tests for the ingest-DAG factory.

Skipped automatically when the `airflow` dependency group isn't installed
(local `make test` with only `--group dev` will auto-skip).
"""
from __future__ import annotations

from datetime import date, datetime

import pytest

pytest.importorskip("airflow.models", reason="airflow not installed; install --group airflow")


def test_make_ingest_dag_extracts_raw_and_emits_asset():
    from dag_factory import DagConfig, make_ingest_dag
    from pipeline_assets import PERMITS_INGEST_ASSET
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
    task_ids = [t.task_id for t in dag.tasks]
    assert task_ids == [
        "extract_permits_to_raw",
        "ingest_complete",
    ]

    extract = dag.get_task("extract_permits_to_raw")
    complete = dag.get_task("ingest_complete")

    assert complete.upstream_task_ids == {"extract_permits_to_raw"}
    assert extract.downstream_task_ids == {"ingest_complete"}
    assert complete.outlets == [PERMITS_INGEST_ASSET]
