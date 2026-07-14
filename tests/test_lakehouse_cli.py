from __future__ import annotations

from argparse import Namespace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts.airflow_rest_client import AirflowRestError
from scripts.lakehouse_cli import (
    LakehouseCliError,
    _genesis_dag_run_id,
    _trigger_or_reuse_genesis_run,
    build_parser,
    status_command,
)


_WINDOW_START = datetime(1997, 1, 1, tzinfo=timezone.utc)
_WINDOW_END = datetime(2026, 5, 1, 6, 0, tzinfo=timezone.utc)
_INGEST_CONF = {
    "load_mode": "full",
    "window_start": "1997-01-01T00:00:00Z",
    "window_end": "2026-05-01T06:00:00Z",
    "lookback_hours": 0,
}


def _duplicate_failed_client() -> tuple[MagicMock, str]:
    dag_run_id = _genesis_dag_run_id(
        dataset_name="permits",
        window_start=_WINDOW_START,
        window_end=_WINDOW_END,
    )
    client = MagicMock()
    client.trigger_dag_run.side_effect = AirflowRestError(
        "duplicate run",
        status_code=409,
    )
    client.get_dag_run.return_value = {
        "dag_run_id": dag_run_id,
        "state": "failed",
    }
    return client, dag_run_id


def test_failed_genesis_reuse_requires_explicit_retry() -> None:
    client, dag_run_id = _duplicate_failed_client()

    with pytest.raises(LakehouseCliError, match="--retry-failed"):
        _trigger_or_reuse_genesis_run(
            client,
            dataset_name="permits",
            ingest_conf=_INGEST_CONF,
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
            retry_failed=False,
        )

    client.clear_dag_run.assert_not_called()
    client.get_dag_run.assert_called_once_with("ingest_permits", dag_run_id)


def test_explicit_failed_genesis_retry_clears_and_reuses_run() -> None:
    client, dag_run_id = _duplicate_failed_client()

    result = _trigger_or_reuse_genesis_run(
        client,
        dataset_name="permits",
        ingest_conf=_INGEST_CONF,
        window_start=_WINDOW_START,
        window_end=_WINDOW_END,
        retry_failed=True,
    )

    assert result == dag_run_id
    client.clear_dag_run.assert_called_once_with("ingest_permits", dag_run_id)


def test_successful_genesis_run_is_reused_without_clearing() -> None:
    client, dag_run_id = _duplicate_failed_client()
    client.get_dag_run.return_value["state"] = "success"

    result = _trigger_or_reuse_genesis_run(
        client,
        dataset_name="permits",
        ingest_conf=_INGEST_CONF,
        window_start=_WINDOW_START,
        window_end=_WINDOW_END,
        retry_failed=False,
    )

    assert result == dag_run_id
    client.clear_dag_run.assert_not_called()


def test_status_hides_complete_intervals_by_default(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    diagnostics = (
        SimpleNamespace(state="complete", marker="hidden"),
        SimpleNamespace(state="ready", marker="visible"),
    )
    monkeypatch.setattr("scripts.lakehouse_cli.storage_from_env", lambda: object())
    monkeypatch.setattr(
        "scripts.lakehouse_cli.diagnose_lakehouse_intervals",
        lambda *args, **kwargs: diagnostics,
    )

    assert status_command(Namespace(all_intervals=False)) == 0

    captured = capsys.readouterr()
    assert '"marker": "visible"' in captured.out
    assert '"marker": "hidden"' not in captured.out
    assert "ready=1" in captured.err
    assert "complete=1" in captured.err


def test_status_all_includes_complete_intervals(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    diagnostics = (SimpleNamespace(state="complete", marker="included"),)
    monkeypatch.setattr("scripts.lakehouse_cli.storage_from_env", lambda: object())
    monkeypatch.setattr(
        "scripts.lakehouse_cli.diagnose_lakehouse_intervals",
        lambda *args, **kwargs: diagnostics,
    )

    assert status_command(Namespace(all_intervals=True)) == 0

    assert '"marker": "included"' in capsys.readouterr().out


def test_parser_accepts_status_all_and_genesis_retry_failed() -> None:
    parser = build_parser()

    status_args = parser.parse_args(["status", "--all"])
    genesis_args = parser.parse_args(["genesis", "--retry-failed"])

    assert status_args.all_intervals is True
    assert genesis_args.retry_failed is True
