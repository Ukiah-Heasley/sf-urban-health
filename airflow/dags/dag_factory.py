"""Factory for building standard SODA ingest DAGs."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from scripts.soda_ingest import DatasetConfig, run as _soda_run

_SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "include", "sql")


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
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        rows = hook.get_records(
            "SELECT watermark FROM METADATA.INGEST_WATERMARKS WHERE dataset_name = %s",
            parameters=[cfg.dataset.name],
        )
        since = rows[0][0].date() if rows else cfg.dataset.epoch
        run_date_str = context.get("ds")
        run_date = datetime.strptime(run_date_str, "%Y-%m-%d").date() if run_date_str else None
        s3_path, max_wm = _soda_run(cfg.dataset, run_date, since)
        context["ti"].xcom_push(key="max_watermark", value=max_wm.isoformat())
        return s3_path

    with DAG(
        dag_id=f"ingest_{cfg.dataset.name}",
        description=f"Daily {cfg.dataset.name}: DataSF -> S3 -> Snowflake -> watermark",
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
            task_id=f"extract_{cfg.dataset.name}_to_s3",
            python_callable=_extract,
        )

        load = SQLExecuteQueryOperator(
            task_id="load_s3_to_snowflake",
            conn_id="snowflake_default",
            sql="copy_into.sql",
            params={
                "database": cfg.snowflake_database,
                "table": cfg.snowflake_table,
                "name": cfg.dataset.name,
            },
        )

        update_wm = SQLExecuteQueryOperator(
            task_id="update_watermark",
            conn_id="snowflake_default",
            sql="update_watermark.sql",
            params={
                "name": cfg.dataset.name,
                "extract_task_id": f"extract_{cfg.dataset.name}_to_s3",
            },
        )

        extract >> load >> update_wm

    return dag
