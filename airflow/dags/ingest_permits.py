"""Daily ingestion of SF building permits into the warehouse.

Extract:   DataSF SODA API (filed_date lookback)
Load:      S3 raw layer -> Snowflake RAW.PERMITS via COPY INTO
Transform: dbt staging -> marts
Test:      dbt tests
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from scripts import permits

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/usr/local/airflow/include/dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/usr/local/airflow/include/dbt")


def _extract_with_date_parsing(**context):
    run_date_str = context.get("ds")
    run_date = datetime.strptime(run_date_str, "%Y-%m-%d").date() if run_date_str else None
    return permits.run(run_date=run_date)


default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

COPY_INTO_RAW = """
COPY INTO {{ params.database }}.RAW.PERMITS (payload)
FROM @{{ params.database }}.RAW.S3_STAGE/raw/permits/{{ ds_nodash[:4] }}/{{ ds_nodash[4:6] }}/{{ ds_nodash[6:8] }}/
FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = FALSE)
ON_ERROR = ABORT_STATEMENT;
"""

with DAG(
    dag_id="ingest_permits",
    description="Daily SF building permits: DataSF -> S3 -> Snowflake -> dbt",
    schedule="0 6 * * *",
    start_date=datetime(2026, 3, 24),
    catchup=False,
    default_args=default_args,
    tags=["sf-civic", "permits", "daily"],
) as dag:
    extract_permits_to_s3 = PythonOperator(
        task_id="extract_permits_to_s3",
        python_callable=_extract_with_date_parsing,
    )

    load_s3_to_snowflake = SQLExecuteQueryOperator(
        task_id="load_s3_to_snowflake",
        conn_id="snowflake_default",
        sql=COPY_INTO_RAW,
        params={"database": "SF_URBAN_HEALTH"},
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt deps --profiles-dir {DBT_PROFILES_DIR} --target prod"
        ),
    )

    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select stg_permits --profiles-dir {DBT_PROFILES_DIR} --target prod"
        ),
    )

    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select int_permit_timelines mart_housing_production "
            f"--profiles-dir {DBT_PROFILES_DIR} --target prod"
        ),
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --target prod"
        ),
    )

    (
        extract_permits_to_s3
        >> load_s3_to_snowflake
        >> dbt_deps
        >> run_dbt_staging
        >> run_dbt_marts
        >> run_dbt_tests
    )