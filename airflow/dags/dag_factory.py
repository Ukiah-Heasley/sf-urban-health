"""Factory for building standard SODA ingest DAGs."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time as datetime_time, timedelta
from pathlib import Path

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG
from airflow.utils.trigger_rule import TriggerRule

from pipeline_assets import ingest_asset_for
from scripts.soda_ingest import DatasetConfig, run as _soda_run

_SQL_DIR = Path(__file__).parent.parent / "include" / "sql"


@dataclass
class DagConfig:
    dataset: DatasetConfig
    snowflake_table: str
    schedule: str
    start_date: datetime
    tags: list[str] = field(default_factory=list)
    snowflake_database: str = "SF_URBAN_HEALTH"


def _coerce_watermark(value) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, date):
        return datetime.combine(value, datetime_time.min)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    raise TypeError(f"Unsupported watermark type from Snowflake: {type(value).__name__}")


def make_ingest_dag(cfg: DagConfig) -> DAG:
    def _extract(**context):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        rows = hook.get_records(
            "SELECT watermark FROM METADATA.INGEST_WATERMARKS WHERE dataset_name = %s",
            parameters=[cfg.dataset.name],
        )
        since = (
            _coerce_watermark(rows[0][0])
            if rows
            else datetime.combine(cfg.dataset.epoch, datetime_time.min)
        )
        include_since = not rows
        run_date_str = context.get("ds")
        run_date = (
            datetime.strptime(run_date_str, "%Y-%m-%d").date()
            if run_date_str
            else datetime.utcnow().date()
        )
        result = _soda_run(cfg.dataset, run_date, since, include_since=include_since)
        if result.max_watermark is not None:
            context["ti"].xcom_push(
                key="max_watermark",
                value=result.max_watermark.isoformat(timespec="milliseconds"),
            )
        context["ti"].xcom_push(key="records_fetched", value=result.records_fetched)
        context["ti"].xcom_push(key="fetch_duration_seconds", value=result.fetch_duration_seconds)
        return result.s3_path

    def _choose_load_path(**context):
        records = context["ti"].xcom_pull(
            task_ids=f"extract_{cfg.dataset.name}_to_s3",
            key="records_fetched",
        )
        return "load_s3_to_snowflake" if int(records or 0) > 0 else "no_new_records"

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

        choose_load_path = BranchPythonOperator(
            task_id="choose_load_path",
            python_callable=_choose_load_path,
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
            # parameters= flows through the driver as bind values (no SQL
            # injection surface). Jinja still resolves the XCom pull at
            # render time before the driver sees the watermark string.
            parameters={
                "name": cfg.dataset.name,
                "watermark": (
                    "{{ ti.xcom_pull("
                    f"task_ids='extract_{cfg.dataset.name}_to_s3', "
                    "key='max_watermark') }}"
                ),
            },
        )

        no_new_records = EmptyOperator(task_id="no_new_records")

        ingest_complete = EmptyOperator(
            task_id="ingest_complete",
            outlets=[ingest_asset_for(cfg.dataset.name)],
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        extract >> choose_load_path
        choose_load_path >> load >> update_wm >> ingest_complete
        choose_load_path >> no_new_records >> ingest_complete

    return dag
