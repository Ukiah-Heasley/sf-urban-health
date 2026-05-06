"""Shared dbt transform DAG: waits for all ingest DAGs then runs dbt once."""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule


def _latest_success(dag_id: str):
    """Return the logical date of the most recent successful run of dag_id.

    ExternalTaskSensor uses execution_date_fn to resolve which external DAGRun
    to wait for. Without this, it matches on the exact logical date of the
    current run — which works for scheduled runs (shared date) but hangs
    forever on manual triggers (each gets a unique timestamp).

    Airflow 3 forbids ORM access from task processes, so we use the REST API.
    """
    def fn(dt):
        import os
        from datetime import datetime
        import requests

        base = os.environ.get("AIRFLOW_API_BASE_URL", "http://localhost:8080").rstrip("/")
        user = os.environ.get("AIRFLOW_API_USER", "admin")
        pwd  = os.environ.get("AIRFLOW_API_PASSWORD", "admin")

        auth = requests.post(
            f"{base}/auth/token",
            json={"username": user, "password": pwd},
            timeout=10,
        )
        auth.raise_for_status()
        headers = {"Authorization": f"Bearer {auth.json()['access_token']}"}

        resp = requests.get(
            f"{base}/api/v2/dags/{dag_id}/dagRuns",
            headers=headers,
            params={"state": "success", "limit": 1, "order_by": "-logical_date"},
            timeout=10,
        )
        resp.raise_for_status()
        runs = resp.json().get("dag_runs", [])
        if not runs:
            return dt
        ts = runs[0].get("logical_date") or runs[0].get("execution_date")
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))

    return fn

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/usr/local/airflow/include/dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/usr/local/airflow/include/dbt")

with DAG(
    dag_id="transform_all",
    description="Wait for all ingest DAGs then run dbt models tagged 'daily'",
    schedule="0 6 * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["sf-civic", "dbt", "daily"],
) as dag:
    wait_permits = ExternalTaskSensor(
        task_id="wait_permits_watermark",
        external_dag_id="ingest_permits",
        external_task_id="update_watermark",
        execution_date_fn=_latest_success("ingest_permits"),
        mode="reschedule",
        timeout=10800,
        poke_interval=120,
        failed_states=["failed", "upstream_failed"],
    )

    wait_evictions = ExternalTaskSensor(
        task_id="wait_evictions_watermark",
        external_dag_id="ingest_evictions",
        external_task_id="update_watermark",
        execution_date_fn=_latest_success("ingest_evictions"),
        mode="reschedule",
        timeout=10800,
        poke_interval=120,
        failed_states=["failed", "upstream_failed"],
    )

    wait_incidents = ExternalTaskSensor(
        task_id="wait_incidents_watermark",
        external_dag_id="ingest_incidents",
        external_task_id="update_watermark",
        execution_date_fn=_latest_success("ingest_incidents"),
        mode="reschedule",
        timeout=10800,
        poke_interval=120,
        failed_states=["failed", "upstream_failed"],
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt deps --profiles-dir {DBT_PROFILES_DIR} --target prod"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select tag:daily elementary "
            f"--profiles-dir {DBT_PROFILES_DIR} --target prod"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --select tag:daily "
            f"--profiles-dir {DBT_PROFILES_DIR} --target prod"
        ),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (  # noqa: B018
        [wait_permits, wait_evictions, wait_incidents]
        >> dbt_deps
        >> dbt_run
        >> dbt_test
    )
