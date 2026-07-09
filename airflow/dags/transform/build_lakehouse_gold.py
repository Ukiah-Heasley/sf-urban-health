"""Gold lakehouse dbt transform DAG: silver/gold Iceberg models after bronze promotion."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from _shared.pipeline_assets import BRONZE_PROMOTION_ASSET, GOLD_TRANSFORM_ASSET
from scripts.dbt_lakehouse import run_dbt_lakehouse_build, run_dbt_lakehouse_debug

with DAG(
    dag_id="build_lakehouse_gold",
    description="Build lakehouse silver/gold Iceberg models via dbt after bronze promotion",
    schedule=[BRONZE_PROMOTION_ASSET],
    start_date=datetime(2026, 5, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["sf-civic", "lakehouse", "dbt"],
) as dag:
    dbt_debug = PythonOperator(
        task_id="dbt_debug",
        python_callable=run_dbt_lakehouse_debug,
        do_xcom_push=False,
    )
    dbt_build_lakehouse_gold = PythonOperator(
        task_id="dbt_build_lakehouse_gold",
        python_callable=run_dbt_lakehouse_build,
        do_xcom_push=False,
    )
    lakehouse_gold_complete = EmptyOperator(
        task_id="lakehouse_gold_complete",
        outlets=[GOLD_TRANSFORM_ASSET],
    )

    dbt_debug >> dbt_build_lakehouse_gold >> lakehouse_gold_complete
