"""Minimal Airflow 3 REST API client for host-side lakehouse commands."""

from __future__ import annotations

import os
import time
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from urllib.parse import quote, urlparse

import requests


_DEFAULT_BASE_URL = "http://localhost:8080"
_DEFAULT_USER = "admin"
_DEFAULT_PASSWORD = "admin"
_LOCAL_HOSTS = frozenset(
    {"localhost", "127.0.0.1", "::1", "airflow.localhost", "host.docker.internal"}
)


class AirflowRestError(RuntimeError):
    """Raised when an Airflow REST request cannot be completed safely."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class AirflowClient:
    """Small authenticated client for the stable Airflow v2 API."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        user: str | None = None,
        password: str | None = None,
        session: requests.Session | None = None,
        timeout_seconds: float = 30.0,
        auth_attempts: int = 3,
    ) -> None:
        if auth_attempts < 1:
            raise ValueError("auth_attempts must be at least 1")

        self._root_url = (
            base_url or os.environ.get("AIRFLOW_API_BASE_URL") or _DEFAULT_BASE_URL
        ).rstrip("/")
        parsed_url = urlparse(self._root_url)
        if not parsed_url.scheme or not parsed_url.netloc:
            raise AirflowRestError(
                "AIRFLOW_API_BASE_URL must be an absolute http(s) URL; "
                f"got {self._root_url!r}"
            )

        self._user = user or os.environ.get("AIRFLOW_API_USER") or _DEFAULT_USER
        self._password = (
            password or os.environ.get("AIRFLOW_API_PASSWORD") or _DEFAULT_PASSWORD
        )
        host = (parsed_url.hostname or "").lower()
        if self._password == _DEFAULT_PASSWORD and host not in _LOCAL_HOSTS:
            raise AirflowRestError(
                f"refusing to authenticate against {host!r} with the default "
                "admin/admin credentials; set AIRFLOW_API_PASSWORD (and "
                "AIRFLOW_API_USER if needed)"
            )

        self._api_url = f"{self._root_url}/api/v2"
        self._timeout_seconds = timeout_seconds
        self._session = session or requests.Session()
        self._authenticate(auth_attempts=auth_attempts)

    def __enter__(self) -> AirflowClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying HTTP session."""

        self._session.close()

    def _authenticate(self, *, auth_attempts: int) -> None:
        last_error: requests.RequestException | None = None

        for attempt in range(auth_attempts):
            try:
                response = self._session.post(
                    f"{self._root_url}/auth/token",
                    json={"username": self._user, "password": self._password},
                    timeout=self._timeout_seconds,
                )
                response.raise_for_status()
            except requests.RequestException as exc:
                last_error = exc
                if attempt + 1 == auth_attempts:
                    break
                time.sleep(2**attempt)
                continue
            try:
                token = response.json().get("access_token")
            except ValueError as exc:
                raise AirflowRestError(
                    "Airflow token response was not valid JSON"
                ) from exc
            if not isinstance(token, str) or not token:
                raise AirflowRestError(
                    "Airflow token response did not include access_token"
                )
            self._session.headers["Authorization"] = f"Bearer {token}"
            return

        raise AirflowRestError(
            "failed to authenticate with the Airflow API"
        ) from last_error

    def _get(self, path: str) -> dict[str, Any]:
        try:
            response = self._session.get(
                f"{self._api_url}/{path.lstrip('/')}", timeout=self._timeout_seconds
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            response = getattr(exc, "response", None)
            status_code = response.status_code if response is not None else None
            raise AirflowRestError(
                f"Airflow API GET failed for {path!r}",
                status_code=status_code,
            ) from exc
        except ValueError as exc:
            raise AirflowRestError(
                f"Airflow API GET returned invalid JSON for {path!r}"
            ) from exc

    def _post(self, path: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        try:
            response = self._session.post(
                f"{self._api_url}/{path.lstrip('/')}",
                json=dict(payload),
                timeout=self._timeout_seconds,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            response = getattr(exc, "response", None)
            status_code = response.status_code if response is not None else None
            raise AirflowRestError(
                f"Airflow API POST failed for {path!r}",
                status_code=status_code,
            ) from exc
        except ValueError as exc:
            raise AirflowRestError(
                f"Airflow API POST returned invalid JSON for {path!r}"
            ) from exc

    def trigger_dag_run(
        self,
        dag_id: str,
        *,
        conf: Mapping[str, Any],
        logical_date: datetime | str | None,
        dag_run_id: str | None = None,
        note: str | None = None,
    ) -> dict[str, Any]:
        """Trigger a DAG run and return Airflow's run payload."""

        if isinstance(logical_date, datetime):
            serialized_logical_date: str | None = logical_date.isoformat()
        elif logical_date is None or isinstance(logical_date, str):
            serialized_logical_date = logical_date
        else:
            raise TypeError("logical_date must be a datetime, ISO string, or None")

        payload: dict[str, Any] = {
            "logical_date": serialized_logical_date,
            "conf": dict(conf),
        }
        if dag_run_id is not None:
            payload["dag_run_id"] = dag_run_id
        if note:
            payload["note"] = note
        return self._post(
            f"dags/{quote(dag_id, safe='')}/dagRuns",
            payload,
        )

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
        """Fetch the latest state for one DAG run."""

        return self._get(
            f"dags/{quote(dag_id, safe='')}/dagRuns/{quote(dag_run_id, safe='')}"
        )

    def clear_dag_run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
        """Clear every task in one DAG run and requeue it for processing."""

        return self._post(
            f"dags/{quote(dag_id, safe='')}/dagRuns/"
            f"{quote(dag_run_id, safe='')}/clear",
            {
                "dry_run": False,
                "only_failed": False,
            },
        )
