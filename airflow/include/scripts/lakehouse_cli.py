"""Trigger and inspect lakehouse ingest and promotion work through Airflow."""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any

from scripts.airflow_rest_client import AirflowClient, AirflowRestError
from scripts.lakehouse_load import LakehouseLoadError, storage_from_env
from scripts.lakehouse_metadata import (
    diagnose_lakehouse_intervals,
    first_shared_daily_interval_start,
    load_ingest_run_event_for_interval,
)
from scripts.time_utils import coerce_utc_datetime


_REQUIRED_DATASETS = ("permits", "evictions", "incidents")
_INGEST_DAG_IDS = {
    "permits": "ingest_permits",
    "evictions": "ingest_evictions",
    "incidents": "ingest_incidents",
}
_PROMOTION_DAG_ID = "promote_raw_to_bronze"
_DEFAULT_GENESIS_START = datetime(1997, 1, 1, tzinfo=timezone.utc)
_FAILED_DAG_RUN_STATES = frozenset({"failed", "canceled"})
_TERMINAL_DAG_RUN_STATES = frozenset({"success", *_FAILED_DAG_RUN_STATES})


class LakehouseCliError(RuntimeError):
    """Raised when the requested lakehouse control operation is invalid."""


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least one")
    return parsed


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _run_id(run_payload: Mapping[str, Any]) -> str:
    run_id = run_payload.get("dag_run_id") or run_payload.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise LakehouseCliError("Airflow trigger response did not include dag_run_id")
    return run_id


def _genesis_dag_run_id(
    *,
    dataset_name: str,
    window_start: datetime,
    window_end: datetime,
) -> str:
    """Return a stable Airflow run id for one dataset's genesis window."""

    start_stamp = window_start.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    end_stamp = window_end.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"manual__lakehouse_genesis__{dataset_name}__{start_stamp}__{end_stamp}"


def _validate_bounds(start: datetime, end: datetime) -> None:
    if start >= end:
        raise LakehouseCliError("window/plan start must be earlier than its end")


def _promotion_conf(
    *,
    mode: str,
    plan_start: datetime | None,
    plan_end: datetime | None,
    max_intervals: int | None,
) -> dict[str, Any]:
    if (plan_start is None) != (plan_end is None):
        raise LakehouseCliError("--plan-start and --plan-end must be supplied together")
    if plan_start is not None and plan_end is not None:
        _validate_bounds(plan_start, plan_end)

    conf: dict[str, Any] = {"plan_mode": mode}
    if plan_start is not None:
        conf["plan_start"] = _format_timestamp(plan_start)
        conf["plan_end"] = _format_timestamp(plan_end)
    if max_intervals is not None:
        conf["plan_max_intervals"] = max_intervals
    return conf


def wait_for_dag_run(
    client: AirflowClient,
    *,
    dag_id: str,
    dag_run_id: str,
    poll_interval_seconds: float,
) -> dict[str, Any]:
    """Poll one DAG run until Airflow reports a terminal state."""

    previous_state: str | None = None
    while True:
        payload = client.get_dag_run(dag_id, dag_run_id)
        raw_state = payload.get("state")
        if not isinstance(raw_state, str) or not raw_state:
            raise LakehouseCliError(
                f"Airflow did not return a state for {dag_id}/{dag_run_id}"
            )
        state = raw_state.lower()
        if state != previous_state:
            print(f"{dag_id}/{dag_run_id}: {state}")
            previous_state = state
        if state in _TERMINAL_DAG_RUN_STATES:
            return payload
        time.sleep(poll_interval_seconds)


def _wait_for_success(
    client: AirflowClient,
    *,
    dag_id: str,
    dag_run_id: str,
    poll_interval_seconds: float,
) -> bool:
    payload = wait_for_dag_run(
        client,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        poll_interval_seconds=poll_interval_seconds,
    )
    return payload["state"].lower() == "success"


def _trigger_promotion(
    client: AirflowClient,
    *,
    mode: str,
    plan_start: datetime | None,
    plan_end: datetime | None,
    max_intervals: int | None,
    no_wait: bool,
    poll_interval_seconds: float,
) -> int:
    conf = _promotion_conf(
        mode=mode,
        plan_start=plan_start,
        plan_end=plan_end,
        max_intervals=max_intervals,
    )
    run_payload = client.trigger_dag_run(
        _PROMOTION_DAG_ID,
        conf=conf,
        logical_date=None,
        note="lakehouse_cli promote",
    )
    dag_run_id = _run_id(run_payload)
    print(f"Triggered {_PROMOTION_DAG_ID}: {dag_run_id}")
    if no_wait:
        return 0
    return int(
        not _wait_for_success(
            client,
            dag_id=_PROMOTION_DAG_ID,
            dag_run_id=dag_run_id,
            poll_interval_seconds=poll_interval_seconds,
        )
    )


def _diagnostic_to_payload(diagnostic: object) -> dict[str, Any]:
    if is_dataclass(diagnostic) and not isinstance(diagnostic, type):
        return asdict(diagnostic)
    if isinstance(diagnostic, Mapping):
        return dict(diagnostic)
    try:
        return vars(diagnostic)
    except TypeError:
        return {"diagnostic": str(diagnostic)}


def _json_default(value: object) -> object:
    if isinstance(value, datetime):
        return _format_timestamp(value)
    if isinstance(value, set | tuple):
        return list(value)
    return str(value)


def status_command(args: argparse.Namespace) -> int:
    """Print planner diagnostics without contacting the Airflow API."""

    storage = storage_from_env()
    diagnostics = tuple(
        diagnose_lakehouse_intervals(
            storage,
            required_datasets=_REQUIRED_DATASETS,
        )
    )
    if not diagnostics:
        print("No lakehouse interval diagnostics found.")
        return 0

    state_counts = Counter(diagnostic.state for diagnostic in diagnostics)
    print(
        "Lakehouse interval summary: "
        + ", ".join(
            f"{state}={state_counts.get(state, 0)}"
            for state in ("ready", "blocked_failed", "incomplete", "complete")
        ),
        file=sys.stderr,
    )
    visible_diagnostics = (
        diagnostics
        if args.all_intervals
        else tuple(
            diagnostic
            for diagnostic in diagnostics
            if diagnostic.state != "complete"
        )
    )
    if not visible_diagnostics:
        print("No actionable lakehouse interval diagnostics found.")
        return 0

    for diagnostic in visible_diagnostics:
        print(
            json.dumps(
                _diagnostic_to_payload(diagnostic),
                default=_json_default,
                sort_keys=True,
            )
        )
    return 0


def promote_command(args: argparse.Namespace) -> int:
    """Trigger one configurable drain-mode promotion run."""

    with AirflowClient() as client:
        return _trigger_promotion(
            client,
            mode=args.mode,
            plan_start=args.plan_start,
            plan_end=args.plan_end,
            max_intervals=args.max_intervals,
            no_wait=args.no_wait,
            poll_interval_seconds=args.poll_interval_seconds,
        )


def _resolve_genesis_end(
    storage: object,
    requested_end: str,
) -> datetime:
    if requested_end.lower() != "auto":
        return coerce_utc_datetime(requested_end)

    resolved = first_shared_daily_interval_start(
        storage,
        required_datasets=_REQUIRED_DATASETS,
    )
    if resolved is None:
        raise LakehouseCliError(
            "could not find a shared daily ingest interval; "
            "supply --window-end explicitly"
        )
    return resolved


def _print_genesis_ingest_summary(
    storage: object,
    *,
    window_start: datetime,
    window_end: datetime,
) -> None:
    print("Genesis ingest summary:")
    for dataset_name in _REQUIRED_DATASETS:
        event = load_ingest_run_event_for_interval(
            storage,
            dataset_name=dataset_name,
            data_interval_start=window_start,
            data_interval_end=window_end,
        )
        print(
            f"{dataset_name}: status={event.status}, "
            f"records_fetched={event.records_fetched}"
        )


def _trigger_or_reuse_genesis_run(
    client: AirflowClient,
    *,
    dataset_name: str,
    ingest_conf: Mapping[str, Any],
    window_start: datetime,
    window_end: datetime,
    retry_failed: bool,
) -> str:
    """Create exactly one full-load DAG run for this deterministic window."""

    dag_id = _INGEST_DAG_IDS[dataset_name]
    dag_run_id = _genesis_dag_run_id(
        dataset_name=dataset_name,
        window_start=window_start,
        window_end=window_end,
    )
    try:
        payload = client.trigger_dag_run(
            dag_id,
            conf=ingest_conf,
            logical_date=None,
            dag_run_id=dag_run_id,
            note="lakehouse_cli genesis",
        )
    except AirflowRestError as exc:
        if exc.status_code != 409:
            raise
        payload = client.get_dag_run(dag_id, dag_run_id)
        print(f"Reusing {dag_id}: {dag_run_id}")
        state_raw = payload.get("state")
        state = state_raw.lower() if isinstance(state_raw, str) else None
        if state in _FAILED_DAG_RUN_STATES:
            if not retry_failed:
                raise LakehouseCliError(
                    f"{dag_id}/{dag_run_id} is {state}; rerun genesis with "
                    "--retry-failed to clear and requeue every task in that run"
                )
            client.clear_dag_run(dag_id, dag_run_id)
            print(f"Cleared and requeued {dag_id}: {dag_run_id}")
    else:
        print(f"Triggered {dag_id}: {dag_run_id}")
    return _run_id(payload)


def genesis_command(args: argparse.Namespace) -> int:
    """Submit the matching full-load interval for all required datasets."""

    storage = storage_from_env()
    window_start = args.window_start
    window_end = _resolve_genesis_end(storage, args.window_end)
    _validate_bounds(window_start, window_end)
    if window_end > datetime.now(timezone.utc):
        raise LakehouseCliError("--window-end cannot be in the future")

    ingest_conf = {
        "load_mode": "full",
        "window_start": _format_timestamp(window_start),
        "window_end": _format_timestamp(window_end),
        "lookback_hours": 0,
    }
    triggered_runs: dict[str, str] = {}
    with AirflowClient() as client:
        for dataset_name in _REQUIRED_DATASETS:
            triggered_runs[dataset_name] = _trigger_or_reuse_genesis_run(
                client,
                dataset_name=dataset_name,
                ingest_conf=ingest_conf,
                window_start=window_start,
                window_end=window_end,
                retry_failed=args.retry_failed,
            )

        if args.no_wait:
            print(
                "Genesis ingest runs submitted; the ingest assets will wake "
                "promotion after the interval is complete."
            )
            return 0

        all_succeeded = True
        for dataset_name in _REQUIRED_DATASETS:
            dag_id = _INGEST_DAG_IDS[dataset_name]
            succeeded = _wait_for_success(
                client,
                dag_id=dag_id,
                dag_run_id=triggered_runs[dataset_name],
                poll_interval_seconds=args.poll_interval_seconds,
            )
            all_succeeded = all_succeeded and succeeded

        if not all_succeeded:
            print(
                "At least one genesis ingest run failed; "
                "promotion was not triggered."
            )
            return 1

        _print_genesis_ingest_summary(
            storage,
            window_start=window_start,
            window_end=window_end,
        )
        promotion_result = _trigger_promotion(
            client,
            mode="pending",
            plan_start=window_start,
            plan_end=window_end,
            max_intervals=None,
            no_wait=False,
            poll_interval_seconds=args.poll_interval_seconds,
        )
        if promotion_result == 0:
            print("Genesis promotion completed.")
        return promotion_result


def build_parser() -> argparse.ArgumentParser:
    """Build the command parser for the host-side lakehouse control CLI."""

    parser = argparse.ArgumentParser(
        description="Inspect and trigger SF Urban Health lakehouse workflows."
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    status = subcommands.add_parser(
        "status",
        help=(
            "List pending, incomplete, and failed interval diagnostics "
            "from S3 metadata."
        ),
    )
    status.add_argument(
        "--all",
        dest="all_intervals",
        action="store_true",
        help="Include receipt-complete intervals in addition to actionable states.",
    )
    status.set_defaults(handler=status_command)

    promote = subcommands.add_parser(
        "promote",
        help="Trigger a drain-mode raw-to-bronze promotion run.",
    )
    promote.add_argument("--mode", choices=("pending", "refresh"), default="pending")
    promote.add_argument("--plan-start", type=coerce_utc_datetime, default=None)
    promote.add_argument("--plan-end", type=coerce_utc_datetime, default=None)
    promote.add_argument("--max-intervals", type=_positive_int, default=None)
    promote.add_argument("--no-wait", action="store_true")
    promote.add_argument(
        "--poll-interval-seconds",
        type=_positive_float,
        default=5.0,
        help="Seconds between Airflow DAG-run state checks (default: 5).",
    )
    promote.set_defaults(handler=promote_command)

    genesis = subcommands.add_parser(
        "genesis",
        help=(
            "Trigger the same full-load interval for permits, evictions, "
            "and incidents."
        ),
    )
    genesis.add_argument(
        "--window-start",
        type=coerce_utc_datetime,
        default=_DEFAULT_GENESIS_START,
        help="Inclusive full-load start (default: 1997-01-01T00:00:00Z).",
    )
    genesis.add_argument(
        "--window-end",
        default="auto",
        help=(
            "Exclusive full-load end as ISO-8601, or auto for the first "
            "shared daily interval."
        ),
    )
    genesis.add_argument("--no-wait", action="store_true")
    genesis.add_argument(
        "--retry-failed",
        action="store_true",
        help="Clear all tasks in a reused failed genesis run and requeue that run.",
    )
    genesis.add_argument(
        "--poll-interval-seconds",
        type=_positive_float,
        default=5.0,
        help="Seconds between Airflow DAG-run state checks (default: 5).",
    )
    genesis.set_defaults(handler=genesis_command)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.handler(args)
    except (
        AirflowRestError,
        LakehouseCliError,
        LakehouseLoadError,
        RuntimeError,
        ValueError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
