"""Shared dbt transform DAG: runs after all ingest assets are updated."""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.utils.trigger_rule import TriggerRule

from pipeline_assets import (
    EVICTIONS_INGEST_ASSET,
    INCIDENTS_INGEST_ASSET,
    PERMITS_INGEST_ASSET,
)

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/usr/local/airflow/include/dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/usr/local/airflow/include/dbt")

with DAG(
    dag_id="transform_all",
    description="Run dbt models tagged 'daily' after all ingest assets update",
    schedule=[PERMITS_INGEST_ASSET, EVICTIONS_INGEST_ASSET, INCIDENTS_INGEST_ASSET],
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["sf-civic", "dbt", "daily"],
) as dag:
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

    dbt_deps >> dbt_run >> dbt_test
