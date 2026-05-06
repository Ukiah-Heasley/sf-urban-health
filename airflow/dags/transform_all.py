"""Shared dbt transform DAG: waits for all ingest DAGs then runs dbt once."""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

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
        mode="reschedule",
        timeout=10800,
        poke_interval=120,
    )

    wait_evictions = ExternalTaskSensor(
        task_id="wait_evictions_watermark",
        external_dag_id="ingest_evictions",
        external_task_id="update_watermark",
        mode="reschedule",
        timeout=10800,
        poke_interval=120,
    )

    wait_incidents = ExternalTaskSensor(
        task_id="wait_incidents_watermark",
        external_dag_id="ingest_incidents",
        external_task_id="update_watermark",
        mode="reschedule",
        timeout=10800,
        poke_interval=120,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt deps --profiles-dir {DBT_PROFILES_DIR} --target prod"
        ),
    )

    run_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select tag:daily --profiles-dir {DBT_PROFILES_DIR} --target prod"
        ),
    )

    [wait_permits, wait_evictions, wait_incidents] >> dbt_deps >> run_models  # noqa: B018
