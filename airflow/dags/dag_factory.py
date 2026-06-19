"""Factory for building standard SODA ingest DAGs."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import DAG
from airflow.utils.trigger_rule import TriggerRule

from pipeline_assets import ingest_asset_for
from scripts.soda_ingest import (
    DatasetConfig,
    ExtractWindow,
    S3NdjsonWriter,
    SodaClient,
    build_soda_session,
    extract_to_raw,
)

_SQL_DIR = Path(__file__).parent.parent / "include" / "sql"


@dataclass
class DagConfig:
    dataset: DatasetConfig
    snowflake_table: str
    schedule: str
    start_date: datetime
    tags: list[str] = field(default_factory=list)
    snowflake_database: str = "SF_URBAN_HEALTH"


def make_ingest_dag(cfg: DagConfig) -> DAG:
    def _extract(**context):
        window = ExtractWindow(
            data_interval_start=context["data_interval_start"],
            data_interval_end=context["data_interval_end"],
        )
        result = extract_to_raw(
            cfg.dataset,
            window,
            client=SodaClient(session=build_soda_session()),
            writer=S3NdjsonWriter.from_env(),
        )
        context["ti"].xcom_push(key="raw_path", value=result.raw_path)
        context["ti"].xcom_push(key="raw_key", value=result.raw_key)
        context["ti"].xcom_push(key="records_fetched", value=result.records_fetched)
        context["ti"].xcom_push(
            key="max_loaded_at",
            value=result.max_loaded_at.isoformat() if result.max_loaded_at else None,
        )
        context["ti"].xcom_push(key="bytes_written", value=result.bytes_written)
        context["ti"].xcom_push(key="duration_seconds", value=result.duration_seconds)
        context["ti"].xcom_push(
            key="data_interval_start",
            value=result.data_interval_start.isoformat(),
        )
        context["ti"].xcom_push(
            key="data_interval_end",
            value=result.data_interval_end.isoformat(),
        )
        context["ti"].xcom_push(key="effective_start", value=result.effective_start.isoformat())
        return result.raw_path

    def _choose_load_path(**context):
        records = context["ti"].xcom_pull(
            task_ids=f"extract_{cfg.dataset.name}_to_raw",
            key="records_fetched",
        )
        return "load_s3_to_snowflake" if int(records or 0) > 0 else "no_new_records"

    with DAG(
        dag_id=f"ingest_{cfg.dataset.name}",
        description=f"Daily {cfg.dataset.name}: DataSF interval -> S3 raw",
        schedule=cfg.schedule,
        start_date=cfg.start_date,
        catchup=False,
        template_searchpath=[_SQL_DIR],
        default_args={
            "owner": "data-eng",
            "retries": 3,
            "retry_delay": timedelta(minutes=5),
        },
        tags=cfg.tags,
    ) as dag:
        extract = PythonOperator(
            task_id=f"extract_{cfg.dataset.name}_to_raw",
            python_callable=_extract,
        )

        choose_load_path = BranchPythonOperator(
            task_id="choose_load_path",
            python_callable=_choose_load_path,
        )

        load_s3_to_snowflake = SQLExecuteQueryOperator(
            task_id="load_s3_to_snowflake",
            conn_id="snowflake_default",
            sql="copy_into.sql",
            params={
                "database": cfg.snowflake_database,
                "table": cfg.snowflake_table,
                "extract_task_id": f"extract_{cfg.dataset.name}_to_raw",
            },
        )

        no_new_records = EmptyOperator(task_id="no_new_records")

        ingest_complete = EmptyOperator(
            task_id="ingest_complete",
            outlets=[ingest_asset_for(cfg.dataset.name)],
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        extract >> choose_load_path
        choose_load_path >> load_s3_to_snowflake >> ingest_complete
        choose_load_path >> no_new_records >> ingest_complete

    return dag
