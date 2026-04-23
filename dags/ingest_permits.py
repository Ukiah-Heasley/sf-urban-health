"""Daily ingestion of SF building permits into the raw data lake.

Extract: DataSF SODA API (filed_date lookback)
Load:    S3 raw layer
Trigger: Snowflake COPY INTO raw.permits via SnowflakeOperator
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from ingestion import permits


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
COPY INTO {{ params.database }}.RAW.PERMITS
FROM @{{ params.database }}.RAW.S3_STAGE/raw/permits/{{ ds_nodash[:4] }}/{{ ds_nodash[4:6] }}/{{ ds_nodash[6:8] }}/
FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = FALSE)
ON_ERROR = ABORT_STATEMENT;
"""

with DAG(
    dag_id="ingest_permits",
    description="Daily SF building permits: DataSF -> S3 -> Snowflake RAW",
    schedule="0 7 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "housing"],
) as dag:
    extract = PythonOperator(
        task_id="extract_to_s3",
        python_callable=_extract_with_date_parsing,
        provide_context=True,
    )

    load_raw = SnowflakeOperator(
        task_id="copy_into_raw",
        snowflake_conn_id="snowflake_default",
        sql=COPY_INTO_RAW,
        params={"database": "SF_URBAN_HEALTH"},
    )

    extract >> load_raw
