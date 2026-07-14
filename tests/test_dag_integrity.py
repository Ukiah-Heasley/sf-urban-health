"""DAG-bag import test: every file under airflow/dags/ must import cleanly.

Skipped automatically when the `airflow` dependency group isn't installed.
This is a lightweight cousin of airflow/.astro/test_dag_integrity_default.py
that doesn't initialize the metadata DB — it only checks for import errors.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

pytest.importorskip(
    "airflow.models", reason="airflow not installed; install --group airflow"
)


_REPO_ROOT = Path(__file__).resolve().parent.parent
_DAGS_DIR = _REPO_ROOT / "airflow" / "dags"

_EXPECTED_DAG_IDS = {
    "ingest_permits",
    "ingest_evictions",
    "ingest_incidents",
    "promote_raw_to_bronze",
    "handle_ingest_failure_metadata",
    "build_lakehouse_gold",
}


def test_all_dags_import_cleanly(monkeypatch: pytest.MonkeyPatch):
    """Every DAG file in airflow/dags/ must parse without raising."""
    from airflow.models import DagBag

    # Provide harmless defaults for env-driven config so DAG files that read
    # env vars at parse time don't crash inside DagBag.
    monkeypatch.setenv("AWS_S3_BUCKET", "test-bucket")
    monkeypatch.setenv("DBT_TARGET", "dev")

    dag_bag = DagBag(dag_folder=str(_DAGS_DIR), include_examples=False)

    if dag_bag.import_errors:
        formatted = "\n".join(
            f"  {os.path.relpath(path, _DAGS_DIR)}: {err.strip()}"
            for path, err in dag_bag.import_errors.items()
        )
        pytest.fail(f"DAG import errors:\n{formatted}")

    assert dag_bag.dags, "DagBag is empty — no DAGs were discovered"
    assert set(dag_bag.dags) == _EXPECTED_DAG_IDS

    from _shared.pipeline_assets import (
        BRONZE_PROMOTION_ASSET,
        GOLD_TRANSFORM_ASSET,
        INGEST_FAILURE_METADATA_ASSET,
        EVICTIONS_INGEST_ASSET,
        INCIDENTS_INGEST_ASSET,
        PERMITS_INGEST_ASSET,
    )

    for ingest_dag_id in (
        "ingest_permits",
        "ingest_evictions",
        "ingest_incidents",
    ):
        ingest = dag_bag.dags[ingest_dag_id]
        assert ingest.catchup is True
        assert ingest.max_active_runs == 1

    bronze = dag_bag.dags["promote_raw_to_bronze"]
    assert bronze.max_active_runs == 1
    assert {task.task_id for task in bronze.tasks} == {
        "plan_bronze_backlog",
        "branch_on_bronze_plan",
        "drain_bronze_backlog",
        "bronze_promotion_noop",
        "compact_lakehouse_metadata",
        "branch_on_bronze_drain_result",
        "bronze_promotion_complete",
        "bronze_promotion_asset_noop",
    }
    expected_bronze_schedule = (
        PERMITS_INGEST_ASSET | EVICTIONS_INGEST_ASSET | INCIDENTS_INGEST_ASSET
    )
    assert repr(bronze.schedule) == repr(expected_bronze_schedule)
    assert bronze.get_task("plan_bronze_backlog").downstream_task_ids == {
        "branch_on_bronze_plan"
    }
    assert bronze.get_task("branch_on_bronze_plan").downstream_task_ids == {
        "drain_bronze_backlog",
        "bronze_promotion_noop",
    }
    assert bronze.get_task("compact_lakehouse_metadata").downstream_task_ids == {
        "branch_on_bronze_drain_result"
    }
    assert bronze.get_task("branch_on_bronze_drain_result").downstream_task_ids == {
        "bronze_promotion_complete",
        "bronze_promotion_asset_noop",
    }

    gold = dag_bag.dags["build_lakehouse_gold"]
    assert gold.max_active_runs == 1
    gold_task_ids = {task.task_id for task in gold.tasks}
    assert gold_task_ids == {
        "dbt_debug",
        "dbt_build_lakehouse_gold",
        "lakehouse_gold_complete",
    }
    assert gold.get_task("dbt_debug").do_xcom_push is False
    assert gold.get_task("dbt_build_lakehouse_gold").do_xcom_push is False
    assert gold.get_task("dbt_debug").downstream_task_ids == {
        "dbt_build_lakehouse_gold"
    }
    assert gold.get_task("dbt_build_lakehouse_gold").downstream_task_ids == {
        "lakehouse_gold_complete"
    }

    failure = dag_bag.dags["handle_ingest_failure_metadata"]
    assert failure.max_active_runs == 1
    failure_task_ids = {task.task_id for task in failure.tasks}
    assert failure_task_ids == {
        "compact_failed_ingest_metadata",
        "dbt_build_observability_gold",
    }
    assert failure.get_task("compact_failed_ingest_metadata").downstream_task_ids == {
        "dbt_build_observability_gold"
    }
    assert failure.get_task("dbt_build_observability_gold").do_xcom_push is False

    assert gold.schedule == [BRONZE_PROMOTION_ASSET]
    assert failure.schedule == [INGEST_FAILURE_METADATA_ASSET]

    complete = gold.get_task("lakehouse_gold_complete")
    assert complete.outlets == [GOLD_TRANSFORM_ASSET]
