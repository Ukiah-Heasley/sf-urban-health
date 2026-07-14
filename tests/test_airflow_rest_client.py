from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests

from scripts.airflow_rest_client import AirflowClient, AirflowRestError


def _authenticated_session() -> MagicMock:
    auth_response = MagicMock()
    auth_response.json.return_value = {"access_token": "test-token"}
    session = MagicMock()
    session.headers = {}
    session.post.return_value = auth_response
    return session


def test_client_authenticates_and_sets_bearer_token() -> None:
    session = _authenticated_session()

    client = AirflowClient(session=session, auth_attempts=1)

    session.post.assert_called_once_with(
        "http://localhost:8080/auth/token",
        json={"username": "admin", "password": "admin"},
        timeout=30.0,
    )
    assert session.headers["Authorization"] == "Bearer test-token"
    client.close()


def test_client_rejects_default_password_for_non_local_host() -> None:
    with pytest.raises(AirflowRestError, match="default admin/admin credentials"):
        AirflowClient(
            base_url="https://airflow.example.com",
            session=MagicMock(),
        )


def test_client_accepts_default_password_for_astro_localhost() -> None:
    session = _authenticated_session()

    client = AirflowClient(
        base_url="http://airflow.localhost:6563",
        session=session,
        auth_attempts=1,
    )

    session.post.assert_called_once_with(
        "http://airflow.localhost:6563/auth/token",
        json={"username": "admin", "password": "admin"},
        timeout=30.0,
    )
    client.close()


def test_post_error_preserves_http_status() -> None:
    session = _authenticated_session()
    auth_response = session.post.return_value
    conflict_response = MagicMock(status_code=409)
    conflict_response.raise_for_status.side_effect = requests.HTTPError(
        response=conflict_response
    )
    session.post.side_effect = [auth_response, conflict_response]
    client = AirflowClient(session=session, auth_attempts=1)

    with pytest.raises(AirflowRestError) as exc_info:
        client.trigger_dag_run(
            "ingest_permits",
            conf={},
            logical_date=None,
            dag_run_id="manual__duplicate",
        )

    assert exc_info.value.status_code == 409


def test_clear_dag_run_requeues_every_task() -> None:
    client = object.__new__(AirflowClient)
    post = MagicMock(return_value={"task_instances": []})
    client._post = post

    result = client.clear_dag_run("ingest permits", "manual/run")

    assert result == {"task_instances": []}
    post.assert_called_once_with(
        "dags/ingest%20permits/dagRuns/manual%2Frun/clear",
        {
            "dry_run": False,
            "only_failed": False,
        },
    )
