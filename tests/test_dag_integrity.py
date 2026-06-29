"""DAG-bag import test: every file under airflow/dags/ must import cleanly.

Skipped automatically when the `airflow` dependency group isn't installed.
This is a lightweight cousin of airflow/.astro/test_dag_integrity_default.py
that doesn't initialize the metadata DB — it only checks for import errors.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

pytest.importorskip("airflow.models", reason="airflow not installed; install --group airflow")


_REPO_ROOT = Path(__file__).resolve().parent.parent
_DAGS_DIR = _REPO_ROOT / "airflow" / "dags"

_EXPECTED_DAG_IDS = {
    "ingest_permits",
    "ingest_evictions",
    "ingest_incidents",
    "promote_raw_to_bronze",
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

    bronze = dag_bag.dags["promote_raw_to_bronze"]
    assert bronze.max_active_runs == 1
    assert "select_bronze_interval" in {task.task_id for task in bronze.tasks}
