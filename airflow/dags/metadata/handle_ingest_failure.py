"""Handle failed ingest metadata for observability gold tables."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from _shared.pipeline_assets import INGEST_FAILURE_METADATA_ASSET
from scripts.dbt_lakehouse import run_dbt_lakehouse_observability_build
from scripts.lakehouse_metadata import compact_lakehouse_metadata

with DAG(
    dag_id="handle_ingest_failure_metadata",
    description="Compact failed ingest metadata and rebuild operational gold tables",
    schedule=[INGEST_FAILURE_METADATA_ASSET],
    start_date=datetime(2026, 5, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["sf-civic", "lakehouse", "metadata"],
) as dag:
    compact_failed_ingest_metadata = PythonOperator(
        task_id="compact_failed_ingest_metadata",
        python_callable=compact_lakehouse_metadata,
        do_xcom_push=False,
    )
    dbt_build_observability_gold = PythonOperator(
        task_id="dbt_build_observability_gold",
        python_callable=run_dbt_lakehouse_observability_build,
        do_xcom_push=False,
    )

    compact_failed_ingest_metadata >> dbt_build_observability_gold
