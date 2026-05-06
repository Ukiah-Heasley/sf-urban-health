"""Fetch Airflow DAG run + task instance metadata via REST API → Snowflake RAW."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, TypedDict

import requests

logger = logging.getLogger(__name__)

_UPSERT_DAG_RUNS      = (Path(__file__).parent.parent / "sql" / "upsert_dag_runs.sql").read_text()
_UPSERT_TASK_INSTANCES = (Path(__file__).parent.parent / "sql" / "upsert_task_instances.sql").read_text()


# ---------------------------------------------------------------------------
# API response shapes
# Keys accessed with ["bracket"] notation are required; the rest are optional.
# Python's TypedDict can't mix required and optional in one definition, so each
# type is split into a required base (_*Base) and an optional extension.
# ---------------------------------------------------------------------------

class _DagBase(TypedDict):
    dag_id: str

class Dag(_DagBase, total=False):
    is_active: bool


class _DagRunBase(TypedDict):
    dag_id: str
    dag_run_id: str

class DagRun(_DagRunBase, total=False):
    state: str
    run_type: str
    start_date: str | None
    end_date: str | None
    logical_date: str | None
    execution_date: str | None


class _TaskInstanceBase(TypedDict):
    task_id: str

class TaskInstance(_TaskInstanceBase, total=False):
    state: str
    start_date: str | None
    end_date: str | None
    try_number: int


class EnrichedTaskInstance(TaskInstance, total=False):
    """TaskInstance with XCom fields appended for extract_ tasks."""
    records_fetched: int | None
    max_watermark: str | None
    s3_path: str | None


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------

class AirflowClient:
    """Thin client for the Airflow 3 REST API (v2)."""

    def __init__(
        self,
        base_url: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ) -> None:
        base = (base_url or os.environ.get("AIRFLOW_API_BASE_URL", "http://localhost:8080")).rstrip("/")
        self._base = f"{base}/api/v2"
        _user = user or os.environ.get("AIRFLOW_API_USER", "admin")
        _pass = password or os.environ.get("AIRFLOW_API_PASSWORD", "admin")
        self._session = requests.Session()
        # Airflow 3 uses JWT auth; exchange credentials for a bearer token once per session.
        resp = self._session.post(
            f"{base}/auth/token",
            json={"username": _user, "password": _pass},
            timeout=30,
        )
        resp.raise_for_status()
        self._session.headers["Authorization"] = f"Bearer {resp.json()['access_token']}"

    def __enter__(self) -> AirflowClient:
        return self

    def __exit__(self, *_) -> None:
        self._session.close()

    def _get(self, path: str, **params) -> dict:
        resp = self._session.get(f"{self._base}/{path}", params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _paginate(self, path: str, key: str, **params) -> list[dict]:
        items: list[dict] = []
        offset = 0
        limit = 100
        while True:
            data = self._get(path, limit=limit, offset=offset, **params)
            batch = data.get(key, [])
            items.extend(batch)
            if len(items) >= data.get("total_entries", len(items)):
                break
            offset += limit
        return items

    def get_dags(self) -> list[Dag]:
        return self._paginate("dags", "dags", paused=False)  # type: ignore[return-value]

    def get_dag_runs(self, dag_id: str, since: str) -> list[DagRun]:
        return self._paginate(  # type: ignore[return-value]
            f"dags/{dag_id}/dagRuns", "dag_runs", logical_date_gte=since
        )

    def get_task_instances(self, dag_id: str, run_id: str) -> list[TaskInstance]:
        return self._paginate(  # type: ignore[return-value]
            f"dags/{dag_id}/dagRuns/{run_id}/taskInstances", "task_instances"
        )

    def get_xcom(self, dag_id: str, run_id: str, task_id: str, key: str) -> Any:
        try:
            data = self._get(
                f"dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{key}"
            )
            return data.get("value")
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            raise


# ---------------------------------------------------------------------------
# Transform helpers
# ---------------------------------------------------------------------------

def _parse_dt(s: str | None) -> str | None:
    return s.replace("Z", "+00:00") if s else None


def _duration(start: str | None, end: str | None) -> float | None:
    if not (start and end):
        return None
    return (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds()


def _enrich_task_instances(
    client: AirflowClient,
    dag_id: str,
    run_id: str,
    task_instances: list[TaskInstance],
) -> list[EnrichedTaskInstance]:
    """Append XCom fields to extract_ tasks; leave all other tasks unchanged."""
    enriched: list[EnrichedTaskInstance] = []
    for ti in task_instances:
        task_id = ti["task_id"]
        row: EnrichedTaskInstance = {**ti}  # type: ignore[assignment]
        if task_id.startswith("extract_"):
            row["records_fetched"] = client.get_xcom(dag_id, run_id, task_id, "records_fetched")
            raw_wm = client.get_xcom(dag_id, run_id, task_id, "max_watermark")
            row["max_watermark"] = f"{raw_wm}T00:00:00" if raw_wm else None
            row["s3_path"] = client.get_xcom(dag_id, run_id, task_id, "return_value")
        enriched.append(row)
    return enriched


# ---------------------------------------------------------------------------
# Snowflake persistence
# ---------------------------------------------------------------------------

def _upsert_dag_runs(cursor, dag_runs: list[DagRun]) -> None:
    for r in dag_runs:
        start = _parse_dt(r.get("start_date"))
        end = _parse_dt(r.get("end_date"))
        cursor.execute(_UPSERT_DAG_RUNS, {
            "dag_id": r["dag_id"],
            "run_id": r["dag_run_id"],
            "state": r.get("state"),
            "execution_date": _parse_dt(r.get("logical_date") or r.get("execution_date")),
            "start_date": start,
            "end_date": end,
            "duration_seconds": _duration(start, end),
            "run_type": r.get("run_type"),
        })


def _upsert_task_instances(
    cursor, dag_id: str, run_id: str, task_instances: list[EnrichedTaskInstance]
) -> None:
    for ti in task_instances:
        start = _parse_dt(ti.get("start_date"))
        end = _parse_dt(ti.get("end_date"))
        cursor.execute(_UPSERT_TASK_INSTANCES, {
            "dag_id": dag_id,
            "run_id": run_id,
            "task_id": ti["task_id"],
            "state": ti.get("state"),
            "start_date": start,
            "end_date": end,
            "duration_seconds": _duration(start, end),
            "try_number": ti.get("try_number"),
            "records_fetched": ti.get("records_fetched"),
            "max_watermark": ti.get("max_watermark"),
            "s3_path": ti.get("s3_path"),
        })


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(since_hours: int = 25) -> None:
    """Fetch runs from the last `since_hours` hours and upsert to Snowflake."""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    since = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()

    with AirflowClient() as client:
        dag_ids = [d["dag_id"] for d in client.get_dags()]
        logger.info("found %d DAGs", len(dag_ids))

        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = hook.get_conn()
        cursor = conn.cursor()
        total_runs = total_tasks = 0

        try:
            for dag_id in dag_ids:
                runs = client.get_dag_runs(dag_id, since=since)
                if not runs:
                    continue
                _upsert_dag_runs(cursor, runs)
                total_runs += len(runs)

                for run in runs:
                    run_id = run["dag_run_id"]
                    task_instances = client.get_task_instances(dag_id, run_id)
                    enriched = _enrich_task_instances(client, dag_id, run_id, task_instances)
                    _upsert_task_instances(cursor, dag_id, run_id, enriched)
                    total_tasks += len(enriched)

            conn.commit()
            logger.info("upserted %d dag runs, %d task instances", total_runs, total_tasks)
        finally:
            cursor.close()
            conn.close()
