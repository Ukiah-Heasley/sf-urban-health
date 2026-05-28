"""Smoke tests for the ingest-DAG factory.

Skipped automatically when the `airflow` dependency group isn't installed
(local `make test` with only `--group dev` will auto-skip).
"""
from __future__ import annotations

from datetime import date, datetime

import pytest

pytest.importorskip("airflow.models", reason="airflow not installed; install --group airflow")


def test_make_ingest_dag_produces_three_tasks_in_canonical_order():
    from dag_factory import DagConfig, make_ingest_dag
    from scripts.soda_ingest import DatasetConfig

    cfg = DagConfig(
        dataset=DatasetConfig(
            name="permits",
            dataset_id="i98e-djp9",
            date_field="data_loaded_at",
            order_field="permit_number",
            epoch=date(2013, 1, 1),
        ),
        snowflake_table="RAW.PERMITS",
        schedule="0 6 * * *",
        start_date=datetime(2026, 5, 1),
        tags=["test"],
    )
    dag = make_ingest_dag(cfg)

    assert dag.dag_id == "ingest_permits"
    task_ids = [t.task_id for t in dag.tasks]
    assert task_ids == [
        "extract_permits_to_s3",
        "load_s3_to_snowflake",
        "update_watermark",
    ]

    # Canonical extract -> load -> update_watermark dependency chain.
    extract = dag.get_task("extract_permits_to_s3")
    load = dag.get_task("load_s3_to_snowflake")
    update = dag.get_task("update_watermark")
    assert load.upstream_task_ids == {"extract_permits_to_s3"}
    assert update.upstream_task_ids == {"load_s3_to_snowflake"}
    assert extract.downstream_task_ids == {"load_s3_to_snowflake"}
