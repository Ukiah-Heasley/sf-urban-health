"""Weekly ingestion of SF Board of Supervisors meeting minutes.

Extract: sfbos.org/meeting-minutes (scrape PDFs)
Parse:   pdfplumber + agenda-item chunking
Load:    S3 raw/transcripts/bos/ + Snowflake RAW.TRANSCRIPT_CHUNKS
Embed:   downstream dbt model via SNOWFLAKE.CORTEX.EMBED_TEXT_768

Skeleton — task bodies to be implemented once ingestion/transcripts.py is filled in.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from ingestion import transcripts


def _extract(**context):
    run_date_str = context.get("ds")
    run_date = datetime.strptime(run_date_str, "%Y-%m-%d").date() if run_date_str else None
    return transcripts.run(run_date=run_date)


default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

COPY_INTO_RAW = """
COPY INTO {{ params.database }}.RAW.TRANSCRIPT_CHUNKS
FROM @{{ params.database }}.RAW.S3_STAGE/raw/transcripts/bos/{{ ds_nodash[:4] }}/{{ ds_nodash[4:6] }}/{{ ds_nodash[6:8] }}/chunks.json
FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = FALSE)
ON_ERROR = ABORT_STATEMENT;
"""

with DAG(
    dag_id="ingest_transcripts",
    description="Weekly BOS meeting minutes: sfbos.org -> S3 -> Snowflake RAW -> Cortex embeddings",
    schedule="0 7 * * 1",  # Monday 07:00 UTC
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "rag"],
) as dag:
    extract = PythonOperator(
        task_id="extract_to_s3",
        python_callable=_extract,
        provide_context=True,
    )

    load_raw = SnowflakeOperator(
        task_id="copy_into_raw",
        snowflake_conn_id="snowflake_default",
        sql=COPY_INTO_RAW,
        params={"database": "SF_URBAN_HEALTH"},
    )

    extract >> load_raw
