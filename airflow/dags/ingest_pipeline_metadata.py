"""Daily DAG: collect Airflow run metadata via REST API → Snowflake METADATA."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def _collect(**_context):
    from scripts.airflow_rest_client import run
    run(since_hours=25)


with DAG(
    dag_id="ingest_pipeline_metadata",
    description="Collect Airflow DAG run + task instance metadata → METADATA.AIRFLOW_DAG_RUNS",
    schedule="0 7 * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["sf-civic", "observability", "daily"],
) as dag:
    PythonOperator(
        task_id="collect_pipeline_metadata",
        python_callable=_collect,
    )
