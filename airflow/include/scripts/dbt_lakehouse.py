"""Run lakehouse dbt commands inside the Airflow runtime."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

_DEFAULT_PROJECT_DIR = Path("/usr/local/airflow/include/dbt")


class DbtLakehouseError(RuntimeError):
    """Raised when a dbt subprocess exits with a non-zero status."""


def dbt_project_dir() -> Path:
    """Return the mirrored dbt project directory for Airflow or local overrides."""
    return Path(os.environ.get("DBT_PROJECT_DIR", _DEFAULT_PROJECT_DIR))


def _run_dbt(args: list[str]) -> subprocess.CompletedProcess[str]:
    project_dir = dbt_project_dir()
    cmd = ["dbt", *args, "--profiles-dir", "."]
    result = subprocess.run(
        cmd,
        cwd=project_dir,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise DbtLakehouseError(
            f"dbt {' '.join(args)} failed (exit {result.returncode})\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    return result


def run_dbt_lakehouse_debug() -> subprocess.CompletedProcess[str]:
    """Verify the dbt Spark profile against the configured Thrift Server."""
    return _run_dbt(["debug"])


def run_dbt_lakehouse_build() -> subprocess.CompletedProcess[str]:
    """Build and test all lakehouse-tagged bronze/silver/gold dbt models."""
    return _run_dbt(["build", "--select", "tag:lakehouse"])


def run_dbt_lakehouse_observability_build() -> subprocess.CompletedProcess[str]:
    """Build and test operational observability gold models after failure metadata."""
    return _run_dbt(["build", "--select", "pipeline_health", "data_trust"])
