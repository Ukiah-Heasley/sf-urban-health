"""Factory for API-to-raw DataSF ingest DAGs.

Each DAG built here owns raw capture only:

1. Use Airflow's data interval to request records from a DataSF SODA endpoint.
2. Write the interval's response to immutable raw NDJSON in S3.
3. Record extract metadata as a current S3 event plus an attempt audit event.
4. Emit the dataset ingest asset for downstream lakehouse and Snowflake work.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

from _shared.pipeline_assets import INGEST_FAILURE_METADATA_ASSET, ingest_asset_for
from scripts.lakehouse_metadata import (
    record_extract_failed_metadata,
    record_extract_metadata,
)
from scripts.soda_ingest import (
    DatasetConfig,
    ExtractWindow,
    S3NdjsonWriter,
    SodaClient,
    build_soda_session,
    extract_to_raw,
)
from scripts.time_utils import coerce_utc_datetime

_MANUAL_LOAD_MODES = frozenset({"full", "backfill"})
_MANUAL_WINDOW_CONF_KEYS = frozenset({"window_start", "window_end", "lookback_hours"})


def resolve_extract_window(
    *,
    data_interval_start: datetime,
    data_interval_end: datetime,
    dag_run_conf: dict | None = None,
) -> ExtractWindow:
    """Build an ``ExtractWindow`` from the Airflow interval or manual trigger conf.

    Scheduled runs use ``data_interval_start`` and ``data_interval_end`` unchanged.
    Manual full/backfill runs supply ``load_mode``, ``window_start``, ``window_end``,
    and optional ``lookback_hours`` in ``dag_run.conf``.
    """
    conf = dag_run_conf or {}
    load_mode = conf.get("load_mode")
    if load_mode is None:
        supplied_manual_keys = sorted(_MANUAL_WINDOW_CONF_KEYS.intersection(conf))
        if supplied_manual_keys:
            raise ValueError(
                "load_mode is required when manual window conf is supplied; "
                f"got keys {supplied_manual_keys!r}"
            )
        return ExtractWindow(
            data_interval_start=data_interval_start,
            data_interval_end=data_interval_end,
        )

    if load_mode not in _MANUAL_LOAD_MODES:
        raise ValueError(
            "load_mode must be 'full' or 'backfill' when set; " f"got {load_mode!r}"
        )

    window_start_raw = conf.get("window_start")
    window_end_raw = conf.get("window_end")
    if (window_start_raw is None) != (window_end_raw is None):
        raise ValueError("window_start and window_end must be supplied together")
    if window_start_raw is None:
        raise ValueError(
            f"load_mode {load_mode!r} requires window_start and window_end "
            "in dag_run conf"
        )

    lookback_hours_raw = conf.get("lookback_hours", 0)
    if lookback_hours_raw is None:
        lookback_hours_raw = 0
    try:
        lookback_hours = int(lookback_hours_raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("lookback_hours must be a non-negative integer") from exc
    if lookback_hours < 0:
        raise ValueError("lookback_hours must be non-negative")

    return ExtractWindow(
        data_interval_start=coerce_utc_datetime(window_start_raw),
        data_interval_end=coerce_utc_datetime(window_end_raw),
        lookback=timedelta(hours=lookback_hours),
    )


@dataclass
class DagConfig:
    """Configuration for one generated DataSF raw ingest DAG."""

    dataset: DatasetConfig
    schedule: str
    start_date: datetime
    tags: list[str] = field(default_factory=list)


def make_ingest_dag(cfg: DagConfig) -> DAG:
    """Build the Airflow DAG that extracts one DataSF dataset to raw S3."""

    extract_task_id = f"extract_{cfg.dataset.name}_to_raw"
    metadata_task_id = f"record_{cfg.dataset.name}_extract_metadata"
    failure_marker_task_id = f"mark_{cfg.dataset.name}_extract_failure_metadata"

    def _window_from_context(context) -> ExtractWindow:
        dag_run = context.get("dag_run")
        dag_run_conf = dag_run.conf if dag_run is not None else None
        return resolve_extract_window(
            data_interval_start=context["data_interval_start"],
            data_interval_end=context["data_interval_end"],
            dag_run_conf=dag_run_conf,
        )

    def _extract_to_raw(**context) -> str | None:
        window = _window_from_context(context)
        result = extract_to_raw(
            cfg.dataset,
            window,
            client=SodaClient(session=build_soda_session()),
            writer=S3NdjsonWriter.from_env(),
        )
        ti = context["ti"]
        ti.xcom_push(key="raw_path", value=result.raw_path)
        ti.xcom_push(key="raw_key", value=result.raw_key)
        ti.xcom_push(key="records_fetched", value=result.records_fetched)
        ti.xcom_push(
            key="max_loaded_at",
            value=result.max_loaded_at.isoformat() if result.max_loaded_at else None,
        )
        ti.xcom_push(key="bytes_written", value=result.bytes_written)
        ti.xcom_push(key="duration_seconds", value=result.duration_seconds)
        ti.xcom_push(
            key="data_interval_start",
            value=result.data_interval_start.isoformat(),
        )
        ti.xcom_push(
            key="data_interval_end",
            value=result.data_interval_end.isoformat(),
        )
        ti.xcom_push(key="effective_start", value=result.effective_start.isoformat())
        ti.xcom_push(key="started_at", value=result.started_at.isoformat())
        ti.xcom_push(key="completed_at", value=result.completed_at.isoformat())
        return result.raw_path

    def _record_extract_metadata(**context) -> str:
        from scripts.soda_ingest import ExtractResult

        ti = context["ti"]
        # The extract task pushes its XComs only after a successful return, and
        # the Airflow 3 Task SDK gives task processes no way to read another
        # task's state, so a missing completed_at XCom is the failure signal.
        completed_at_raw = ti.xcom_pull(task_ids=extract_task_id, key="completed_at")
        if completed_at_raw is None:
            window = _window_from_context(context)
            event_key = record_extract_failed_metadata(
                extract_window=window,
                ingest_run_id=context["run_id"],
                dag_id=context["dag"].dag_id,
                dataset_name=cfg.dataset.name,
            )
            ti.xcom_push(key="ingest_metadata_event_key", value=event_key)
            ti.xcom_push(key="ingest_metadata_event_status", value="failed")
            return event_key

        max_loaded_at_raw = ti.xcom_pull(task_ids=extract_task_id, key="max_loaded_at")
        extract_result = ExtractResult(
            raw_path=ti.xcom_pull(task_ids=extract_task_id, key="raw_path"),
            raw_key=ti.xcom_pull(task_ids=extract_task_id, key="raw_key"),
            records_fetched=ti.xcom_pull(
                task_ids=extract_task_id, key="records_fetched"
            ),
            max_loaded_at=coerce_utc_datetime(max_loaded_at_raw)
            if max_loaded_at_raw
            else None,
            bytes_written=ti.xcom_pull(task_ids=extract_task_id, key="bytes_written"),
            duration_seconds=ti.xcom_pull(
                task_ids=extract_task_id, key="duration_seconds"
            ),
            data_interval_start=coerce_utc_datetime(
                ti.xcom_pull(task_ids=extract_task_id, key="data_interval_start")
            ),
            data_interval_end=coerce_utc_datetime(
                ti.xcom_pull(task_ids=extract_task_id, key="data_interval_end")
            ),
            effective_start=coerce_utc_datetime(
                ti.xcom_pull(task_ids=extract_task_id, key="effective_start")
            ),
            started_at=coerce_utc_datetime(
                ti.xcom_pull(task_ids=extract_task_id, key="started_at")
            ),
            completed_at=coerce_utc_datetime(completed_at_raw),
        )
        event_key = record_extract_metadata(
            extract_result=extract_result,
            ingest_run_id=context["run_id"],
            dag_id=context["dag"].dag_id,
            dataset_name=cfg.dataset.name,
        )
        ti.xcom_push(key="ingest_metadata_event_key", value=event_key)
        status = "empty" if extract_result.records_fetched == 0 else "success"
        ti.xcom_push(key="ingest_metadata_event_status", value=status)
        return event_key

    def _mark_extract_failure_metadata(**context) -> str:
        metadata_status = context["ti"].xcom_pull(
            task_ids=metadata_task_id,
            key="ingest_metadata_event_status",
        )
        if metadata_status != "failed":
            raise AirflowSkipException("extract did not write failed ingest metadata")
        return "failed ingest metadata recorded"

    with DAG(
        dag_id=f"ingest_{cfg.dataset.name}",
        description=f"Daily {cfg.dataset.name}: DataSF interval -> S3 raw + metadata event",
        schedule=CronDataIntervalTimetable(cfg.schedule, timezone="UTC"),
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
            task_id=extract_task_id,
            python_callable=_extract_to_raw,
        )

        record_metadata = PythonOperator(
            task_id=metadata_task_id,
            python_callable=_record_extract_metadata,
            trigger_rule="all_done",
        )

        failure_marker = PythonOperator(
            task_id=failure_marker_task_id,
            python_callable=_mark_extract_failure_metadata,
            trigger_rule="all_done",
            outlets=[INGEST_FAILURE_METADATA_ASSET],
        )

        ingest_complete = EmptyOperator(
            task_id="ingest_complete",
            outlets=[ingest_asset_for(cfg.dataset.name)],
        )

        extract >> record_metadata >> ingest_complete
        extract >> ingest_complete
        [extract, record_metadata] >> failure_marker

    return dag
