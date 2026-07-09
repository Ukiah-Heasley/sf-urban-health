"""Unit tests for scripts.dbt_lakehouse subprocess helpers."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts.dbt_lakehouse import (
    DbtLakehouseError,
    dbt_project_dir,
    run_dbt_lakehouse_build,
    run_dbt_lakehouse_debug,
    run_dbt_lakehouse_observability_build,
)


def test_dbt_project_dir_defaults_to_airflow_mirror():
    assert dbt_project_dir() == Path("/usr/local/airflow/include/dbt")


def test_dbt_project_dir_honors_env_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    monkeypatch.setenv("DBT_PROJECT_DIR", str(tmp_path))
    assert dbt_project_dir() == tmp_path


def test_run_dbt_lakehouse_debug_invokes_expected_command(
    monkeypatch: pytest.MonkeyPatch,
):
    captured: dict[str, object] = {}

    def fake_run(cmd, *, cwd, capture_output, text, check):
        captured["cmd"] = cmd
        captured["cwd"] = cwd
        captured["capture_output"] = capture_output
        captured["text"] = text
        captured["check"] = check
        return subprocess.CompletedProcess(cmd, 0, stdout="ok", stderr="")

    monkeypatch.setattr("scripts.dbt_lakehouse.subprocess.run", fake_run)

    result = run_dbt_lakehouse_debug()

    assert captured["cmd"] == ["dbt", "debug", "--profiles-dir", "."]
    assert captured["cwd"] == Path("/usr/local/airflow/include/dbt")
    assert captured["capture_output"] is True
    assert captured["text"] is True
    assert captured["check"] is False
    assert result.stdout == "ok"


def test_run_dbt_lakehouse_build_invokes_expected_command(
    monkeypatch: pytest.MonkeyPatch,
):
    captured: dict[str, object] = {}

    def fake_run(cmd, *, cwd, capture_output, text, check):
        captured["cmd"] = cmd
        captured["cwd"] = cwd
        return subprocess.CompletedProcess(cmd, 0, stdout="built", stderr="")

    monkeypatch.setattr("scripts.dbt_lakehouse.subprocess.run", fake_run)

    result = run_dbt_lakehouse_build()

    assert captured["cmd"] == [
        "dbt",
        "build",
        "--select",
        "tag:lakehouse",
        "--profiles-dir",
        ".",
    ]
    assert captured["cwd"] == Path("/usr/local/airflow/include/dbt")
    assert result.stdout == "built"


def test_run_dbt_lakehouse_observability_build_invokes_expected_command(
    monkeypatch: pytest.MonkeyPatch,
):
    captured: dict[str, object] = {}

    def fake_run(cmd, *, cwd, capture_output, text, check):
        captured["cmd"] = cmd
        captured["cwd"] = cwd
        return subprocess.CompletedProcess(cmd, 0, stdout="built", stderr="")

    monkeypatch.setattr("scripts.dbt_lakehouse.subprocess.run", fake_run)

    result = run_dbt_lakehouse_observability_build()

    assert captured["cmd"] == [
        "dbt",
        "build",
        "--select",
        "pipeline_health",
        "data_trust",
        "--profiles-dir",
        ".",
    ]
    assert captured["cwd"] == Path("/usr/local/airflow/include/dbt")
    assert result.stdout == "built"


def test_run_dbt_lakehouse_uses_project_dir_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    captured: dict[str, object] = {}

    def fake_run(cmd, *, cwd, capture_output, text, check):
        captured["cwd"] = cwd
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setenv("DBT_PROJECT_DIR", str(tmp_path))
    monkeypatch.setattr("scripts.dbt_lakehouse.subprocess.run", fake_run)

    run_dbt_lakehouse_debug()

    assert captured["cwd"] == tmp_path


def test_run_dbt_lakehouse_raises_on_nonzero_exit(monkeypatch: pytest.MonkeyPatch):
    def fake_run(cmd, *, cwd, capture_output, text, check):
        return subprocess.CompletedProcess(
            cmd, 1, stdout="bad", stderr="connection refused"
        )

    monkeypatch.setattr("scripts.dbt_lakehouse.subprocess.run", fake_run)

    with pytest.raises(DbtLakehouseError, match="dbt debug failed \\(exit 1\\)"):
        run_dbt_lakehouse_debug()
