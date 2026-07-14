"""Drain complete raw intervals into bronze Parquet from a durable snapshot."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

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
    IngestRunEvent,
    LakehouseIntervalPlan,
    compact_lakehouse_metadata,
    find_lakehouse_plan_snapshot,
    plan_lakehouse_intervals,
    read_ingest_run_event,
    read_lakehouse_plan_snapshot,
    resolve_lakehouse_interval,
    resolve_lakehouse_plan_config,
    write_lakehouse_plan_snapshot,
)
from scripts.lakehouse_load import promote_raw_to_bronze, storage_from_env
from scripts.permits import PERMITS_CONFIG

_LOG = logging.getLogger(__name__)

_DATASETS = {
    "permits": PERMITS_CONFIG,
    "evictions": EVICTIONS_CONFIG,
    "incidents": INCIDENTS_CONFIG,
}
_REQUIRED_DATASETS = tuple(_DATASETS)

_PLAN_TASK_ID = "plan_bronze_backlog"
_DRAIN_TASK_ID = "drain_bronze_backlog"
_PLAN_NOOP_TASK_ID = "bronze_promotion_noop"
_ASSET_NOOP_TASK_ID = "bronze_promotion_asset_noop"
_COMPLETE_TASK_ID = "bronze_promotion_complete"
_SNAPSHOT_KEY_XCOM = "bronze_backlog_snapshot_key"
_SNAPSHOT_COUNT_XCOM = "bronze_backlog_plan_count"
_DRAIN_RESULT_XCOM = "bronze_drain_result"


@dataclass(frozen=True)
class BronzeDrainResult:
    """Small XCom-safe result of draining one immutable backlog snapshot."""

    snapshot_plan_count: int
    receipt_complete_intervals: int
    promoted_intervals: int
    skipped_complete_intervals: int
    skipped_failed_intervals: int
    skipped_incomplete_intervals: int


def _dag_run_conf(context: Mapping[str, Any]) -> Mapping[str, Any] | None:
    dag_run = context.get("dag_run")
    return dag_run.conf if dag_run is not None else None


def _plan_bronze_backlog(**context) -> dict[str, object]:
    """Discover work once and store a durable snapshot rather than a large XCom."""

    storage = storage_from_env()
    existing_snapshot_key = find_lakehouse_plan_snapshot(
        storage,
        dag_run_id=context["run_id"],
    )
    if existing_snapshot_key is not None:
        existing_plans = read_lakehouse_plan_snapshot(storage, existing_snapshot_key)
        payload = {
            "snapshot_key": existing_snapshot_key,
            "plan_count": len(existing_plans),
        }
    else:
        config = resolve_lakehouse_plan_config(dag_run_conf=_dag_run_conf(context))
        plans = plan_lakehouse_intervals(
            storage,
            required_datasets=_REQUIRED_DATASETS,
            mode=config.mode,
            start=config.start,
            end=config.end,
            limit=config.max_intervals,
        )
        snapshot_key = (
            write_lakehouse_plan_snapshot(
                storage,
                dag_run_id=context["run_id"],
                plans=plans,
            )
            if plans
            else None
        )
        payload = {"snapshot_key": snapshot_key, "plan_count": len(plans)}

    context["ti"].xcom_push(key=_SNAPSHOT_KEY_XCOM, value=payload["snapshot_key"])
    context["ti"].xcom_push(key=_SNAPSHOT_COUNT_XCOM, value=payload["plan_count"])
    return payload


class BranchOnBronzePlanOperator(BaseBranchOperator):
    """Send empty discovery snapshots through metadata compaction only."""

    def choose_branch(self, context) -> str:
        snapshot_key = context["ti"].xcom_pull(
            task_ids=_PLAN_TASK_ID,
            key=_SNAPSHOT_KEY_XCOM,
        )
        return _DRAIN_TASK_ID if snapshot_key else _PLAN_NOOP_TASK_ID


def _fresh_events_for_plan(
    storage,
    plan: LakehouseIntervalPlan,
) -> dict[str, IngestRunEvent] | None:
    """Read current events immediately before promotion and reject fresh failures."""

    events: dict[str, IngestRunEvent] = {}
    for dataset_name in _REQUIRED_DATASETS:
        ingest_event = read_ingest_run_event(
            storage,
            plan.ingest_event_keys[dataset_name],
        )
        if ingest_event.status == "failed":
            _LOG.warning(
                "Skipping interval %s..%s: %s current event became failed",
                plan.data_interval_start.isoformat(),
                plan.data_interval_end.isoformat(),
                dataset_name,
            )
            return None
        events[dataset_name] = ingest_event
    return events


def _drain_bronze_backlog(**context) -> dict[str, int]:
    """Drain only the original snapshot, validating receipts before every member.

    The task intentionally does not rediscover intervals. On retry it rereads
    the same S3 snapshot and uses fresh, exact-bound receipt resolution to skip
    intervals completed by an interrupted prior attempt.
    """

    snapshot_key = context["ti"].xcom_pull(
        task_ids=_PLAN_TASK_ID,
        key=_SNAPSHOT_KEY_XCOM,
    )
    if not snapshot_key:
        result = BronzeDrainResult(0, 0, 0, 0, 0, 0)
        payload = asdict(result)
        context["ti"].xcom_push(key=_DRAIN_RESULT_XCOM, value=payload)
        return payload

    storage = storage_from_env()
    snapshot_plans = read_lakehouse_plan_snapshot(storage, str(snapshot_key))
    receipt_complete_intervals = 0
    promoted_intervals = 0
    skipped_complete_intervals = 0
    skipped_failed_intervals = 0
    skipped_incomplete_intervals = 0

    for snapshot_plan in snapshot_plans:
        fresh = resolve_lakehouse_interval(
            storage,
            data_interval_start=snapshot_plan.data_interval_start,
            data_interval_end=snapshot_plan.data_interval_end,
            required_datasets=_REQUIRED_DATASETS,
            mode=snapshot_plan.mode,
        )
        if fresh.state == "complete":
            # The plan was pending when the snapshot was made. A receipt now
            # means a prior task attempt finished it and this run must still
            # emit the global promotion asset once.
            receipt_complete_intervals += 1
            skipped_complete_intervals += 1
            continue
        if fresh.state == "blocked_failed":
            _LOG.warning("Skipping blocked interval: %s", fresh.reason)
            skipped_failed_intervals += 1
            continue
        if fresh.state == "incomplete":
            _LOG.warning("Skipping incomplete interval: %s", fresh.reason)
            skipped_incomplete_intervals += 1
            continue

        plan = fresh.plan
        if plan is None:  # Defensive guard for future resolution states.
            _LOG.warning("Skipping unresolved interval: %s", fresh.reason)
            skipped_incomplete_intervals += 1
            continue

        fresh_events = _fresh_events_for_plan(storage, plan)
        if fresh_events is None:
            skipped_failed_intervals += 1
            continue

        for dataset_name in _REQUIRED_DATASETS:
            ingest_event = fresh_events[dataset_name]
            if (
                plan.mode == "pending"
                and ingest_event.records_fetched > 0
                and dataset_name in plan.existing_manifest_event_keys
            ):
                continue
            promote_raw_to_bronze(
                dataset_name=dataset_name,
                dataset_id=_DATASETS[dataset_name].dataset_id,
                ingest_event=ingest_event,
                storage=storage,
            )

        receipt_check = resolve_lakehouse_interval(
            storage,
            data_interval_start=plan.data_interval_start,
            data_interval_end=plan.data_interval_end,
            required_datasets=_REQUIRED_DATASETS,
            mode="pending",
        )
        if receipt_check.state == "complete":
            receipt_complete_intervals += 1
            promoted_intervals += 1
        elif receipt_check.state == "blocked_failed":
            _LOG.warning(
                "Interval became blocked after promotion: %s",
                receipt_check.reason,
            )
            skipped_failed_intervals += 1
        else:
            _LOG.warning(
                "Interval is not receipt-complete after promotion: %s",
                receipt_check.reason,
            )
            skipped_incomplete_intervals += 1

    result = BronzeDrainResult(
        snapshot_plan_count=len(snapshot_plans),
        receipt_complete_intervals=receipt_complete_intervals,
        promoted_intervals=promoted_intervals,
        skipped_complete_intervals=skipped_complete_intervals,
        skipped_failed_intervals=skipped_failed_intervals,
        skipped_incomplete_intervals=skipped_incomplete_intervals,
    )
    payload = asdict(result)
    context["ti"].xcom_push(key=_DRAIN_RESULT_XCOM, value=payload)
    return payload


class BranchOnBronzeDrainResultOperator(BaseBranchOperator):
    """Emit downstream bronze assets only after at least one receipt completed."""

    def choose_branch(self, context) -> str:
        result = context["ti"].xcom_pull(
            task_ids=_DRAIN_TASK_ID,
            key=_DRAIN_RESULT_XCOM,
        )
        if (
            isinstance(result, Mapping)
            and int(result.get("receipt_complete_intervals", 0)) > 0
        ):
            return _COMPLETE_TASK_ID
        return _ASSET_NOOP_TASK_ID


def _compact_metadata(**context) -> None:
    compact_lakehouse_metadata()


with DAG(
    dag_id="promote_raw_to_bronze",
    description="Drain complete raw intervals to bronze from a durable S3 snapshot",
    schedule=(PERMITS_INGEST_ASSET | EVICTIONS_INGEST_ASSET | INCIDENTS_INGEST_ASSET),
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
    plan_bronze_backlog = PythonOperator(
        task_id=_PLAN_TASK_ID,
        python_callable=_plan_bronze_backlog,
    )
    branch_on_plan = BranchOnBronzePlanOperator(task_id="branch_on_bronze_plan")
    drain_bronze_backlog = PythonOperator(
        task_id=_DRAIN_TASK_ID,
        python_callable=_drain_bronze_backlog,
    )
    bronze_promotion_noop = EmptyOperator(task_id=_PLAN_NOOP_TASK_ID)
    compact_lakehouse_metadata_task = PythonOperator(
        task_id="compact_lakehouse_metadata",
        python_callable=_compact_metadata,
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
    )
    branch_on_drain_result = BranchOnBronzeDrainResultOperator(
        task_id="branch_on_bronze_drain_result"
    )
    bronze_promotion_complete = EmptyOperator(
        task_id=_COMPLETE_TASK_ID,
        outlets=[
            bronze_asset_for("permits"),
            bronze_asset_for("evictions"),
            bronze_asset_for("incidents"),
            BRONZE_PROMOTION_ASSET,
        ],
    )
    bronze_promotion_asset_noop = EmptyOperator(task_id=_ASSET_NOOP_TASK_ID)

    plan_bronze_backlog >> branch_on_plan
    branch_on_plan >> [drain_bronze_backlog, bronze_promotion_noop]
    [drain_bronze_backlog, bronze_promotion_noop] >> compact_lakehouse_metadata_task
    compact_lakehouse_metadata_task >> branch_on_drain_result
    branch_on_drain_result >> [
        bronze_promotion_complete,
        bronze_promotion_asset_noop,
    ]
