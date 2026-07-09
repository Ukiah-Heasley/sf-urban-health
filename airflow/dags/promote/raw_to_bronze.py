"""Bronze promotion DAG: promote complete raw intervals to bronze Parquet."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.branch import BaseBranchOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from _shared.pipeline_assets import (
    BRONZE_PROMOTION_ASSET,
    EVICTIONS_INGEST_ASSET,
    INCIDENTS_INGEST_ASSET,
    PERMITS_INGEST_ASSET,
    bronze_asset_for,
)
from scripts.evictions import EVICTIONS_CONFIG
from scripts.incident_reports import INCIDENTS_CONFIG
from scripts.lakehouse_metadata import (
    compact_lakehouse_metadata,
    lakehouse_interval_plan_to_dict,
    parse_lakehouse_plan_limit,
    plan_lakehouse_intervals,
    read_ingest_run_event,
)
from scripts.lakehouse_load import promote_raw_to_bronze, storage_from_env
from scripts.permits import PERMITS_CONFIG
from scripts.time_utils import coerce_utc_datetime

_DATASETS = {
    "permits": PERMITS_CONFIG,
    "evictions": EVICTIONS_CONFIG,
    "incidents": INCIDENTS_CONFIG,
}

_SELECT_TASK_ID = "select_bronze_interval"
_PROMOTE_PERMITS_TASK_ID = "promote_permits_to_bronze"
_NOOP_TASK_ID = "bronze_promotion_noop"


def _parse_optional_plan_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    return coerce_utc_datetime(value)


def _select_bronze_interval(**context) -> dict | None:
    storage = storage_from_env()
    mode = os.environ.get("LAKEHOUSE_PLAN_MODE", "pending")
    limit = parse_lakehouse_plan_limit(os.environ.get("LAKEHOUSE_PLAN_LIMIT", "1"))
    start = _parse_optional_plan_timestamp(os.environ.get("LAKEHOUSE_PLAN_START"))
    end = _parse_optional_plan_timestamp(os.environ.get("LAKEHOUSE_PLAN_END"))
    plans = plan_lakehouse_intervals(
        storage,
        mode=mode,
        start=start,
        end=end,
        limit=limit,
    )
    if not plans:
        context["ti"].xcom_push(key="bronze_interval_plan", value=None)
        return None
    plan = plans[0]
    payload = lakehouse_interval_plan_to_dict(plan)
    context["ti"].xcom_push(key="bronze_interval_plan", value=payload)
    return payload


class BranchOnBronzePlanOperator(BaseBranchOperator):
    """Skip promotion and asset emission when no interval is selected."""

    def choose_branch(self, context) -> str:
        plan = context["ti"].xcom_pull(
            task_ids=_SELECT_TASK_ID, key="bronze_interval_plan"
        )
        if plan is None:
            return _NOOP_TASK_ID
        return _PROMOTE_PERMITS_TASK_ID


def _promote_dataset(dataset_name: str, **context) -> str | None:
    plan_payload = context["ti"].xcom_pull(
        task_ids=_SELECT_TASK_ID, key="bronze_interval_plan"
    )
    if plan_payload is None:
        return None

    config = _DATASETS[dataset_name]
    storage = storage_from_env()
    ingest_event_key = plan_payload["ingest_event_keys"][dataset_name]
    ingest_event = read_ingest_run_event(storage, ingest_event_key)
    result = promote_raw_to_bronze(
        dataset_name=dataset_name,
        dataset_id=config.dataset_id,
        ingest_event=ingest_event,
        storage=storage,
    )
    context["ti"].xcom_push(key="bronze_path", value=result.bronze_path)
    context["ti"].xcom_push(key="bronze_key", value=result.bronze_key)
    context["ti"].xcom_push(key="records_promoted", value=result.records_promoted)
    context["ti"].xcom_push(key="manifest_event_key", value=result.manifest_event_key)
    return result.bronze_path


def _compact_metadata(**context) -> None:
    compact_lakehouse_metadata()


with DAG(
    dag_id="promote_raw_to_bronze",
    description="Promote complete raw intervals to bronze Parquet and compact lakehouse metadata",
    schedule=[PERMITS_INGEST_ASSET, EVICTIONS_INGEST_ASSET, INCIDENTS_INGEST_ASSET],
    start_date=datetime(2026, 5, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["sf-civic", "lakehouse", "daily"],
) as dag:
    select_bronze_interval = PythonOperator(
        task_id=_SELECT_TASK_ID,
        python_callable=_select_bronze_interval,
    )
    branch_on_plan = BranchOnBronzePlanOperator(
        task_id="branch_on_bronze_plan",
    )
    promote_permits = PythonOperator(
        task_id=_PROMOTE_PERMITS_TASK_ID,
        python_callable=_promote_dataset,
        op_kwargs={"dataset_name": "permits"},
        outlets=[bronze_asset_for("permits")],
    )
    promote_evictions = PythonOperator(
        task_id="promote_evictions_to_bronze",
        python_callable=_promote_dataset,
        op_kwargs={"dataset_name": "evictions"},
        outlets=[bronze_asset_for("evictions")],
    )
    promote_incidents = PythonOperator(
        task_id="promote_incidents_to_bronze",
        python_callable=_promote_dataset,
        op_kwargs={"dataset_name": "incidents"},
        outlets=[bronze_asset_for("incidents")],
    )
    compact_lakehouse_metadata_task = PythonOperator(
        task_id="compact_lakehouse_metadata",
        python_callable=_compact_metadata,
        trigger_rule="none_failed_min_one_success",
    )
    bronze_promotion_complete = EmptyOperator(
        task_id="bronze_promotion_complete",
        outlets=[BRONZE_PROMOTION_ASSET],
    )
    bronze_promotion_noop = EmptyOperator(
        task_id=_NOOP_TASK_ID,
    )

    select_bronze_interval >> branch_on_plan
    branch_on_plan >> [promote_permits, bronze_promotion_noop]
    (
        promote_permits
        >> promote_evictions
        >> promote_incidents
        >> compact_lakehouse_metadata_task
    )
    bronze_promotion_noop >> compact_lakehouse_metadata_task
    [promote_incidents, compact_lakehouse_metadata_task] >> bronze_promotion_complete
