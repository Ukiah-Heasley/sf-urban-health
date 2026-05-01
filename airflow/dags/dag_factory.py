"""Factory for building standard SODA ingest DAGs."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from scripts.soda_ingest import DatasetConfig, run as _soda_run

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/usr/local/airflow/include/dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/usr/local/airflow/include/dbt")

_COPY_INTO_SQL = """
COPY INTO {{ params.database }}.{{ params.table }} (payload)
FROM @{{ params.database }}.RAW.S3_STAGE/raw/{{ params.name }}/{{ ds_nodash[:4] }}/{{ ds_nodash[4:6] }}/{{ ds_nodash[6:8] }}/
FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = FALSE)
ON_ERROR = ABORT_STATEMENT;
"""


@dataclass
class DagConfig:
    dataset: DatasetConfig
    snowflake_table: str
    dbt_staging_models: list[str]
    dbt_mart_models: list[str]
    schedule: str
    start_date: datetime
    tags: list[str] = field(default_factory=list)
    snowflake_database: str = "SF_URBAN_HEALTH"


def make_ingest_dag(cfg: DagConfig) -> DAG:
    def _extract(**context):
        run_date_str = context.get("ds")
        run_date = datetime.strptime(run_date_str, "%Y-%m-%d").date() if run_date_str else None
        return _soda_run(cfg.dataset, run_date)

    with DAG(
        dag_id=f"ingest_{cfg.dataset.name}",
        description=f"Daily {cfg.dataset.name}: DataSF -> S3 -> Snowflake -> dbt",
        schedule=cfg.schedule,
        start_date=cfg.start_date,
        catchup=False,
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
            sql=_COPY_INTO_SQL,
            params={
                "database": cfg.snowflake_database,
                "table": cfg.snowflake_table,
                "name": cfg.dataset.name,
            },
        )

        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=(
                f"cd {DBT_PROJECT_DIR} && "
                f"dbt deps --profiles-dir {DBT_PROFILES_DIR} --target prod"
            ),
        )

        staging_models = " ".join(cfg.dbt_staging_models)
        run_staging = BashOperator(
            task_id="run_dbt_staging",
            bash_command=(
                f"cd {DBT_PROJECT_DIR} && "
                f"dbt run --select {staging_models} --profiles-dir {DBT_PROFILES_DIR} --target prod"
            ),
        )

        mart_models = " ".join(cfg.dbt_mart_models)
        run_marts = BashOperator(
            task_id="run_dbt_marts",
            bash_command=(
                f"cd {DBT_PROJECT_DIR} && "
                f"dbt run --select {mart_models} --profiles-dir {DBT_PROFILES_DIR} --target prod"
            ),
        )
        extract >> load >> dbt_deps >> run_staging >> run_marts

    return dag
