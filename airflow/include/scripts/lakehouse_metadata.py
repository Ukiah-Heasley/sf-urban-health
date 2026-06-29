"""S3 metadata events and single-task compaction for the lakehouse path."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.lakehouse_contracts import TableContract, get_contract
from scripts.lakehouse_load import (
    LakehouseLoadError,
    StorageConfig,
    format_interval_partition,
    pyarrow_schema_for_contract,
    storage_from_env,
)
from scripts.time_utils import coerce_utc_datetime

METADATA_EVENTS_PREFIX = "lake/metadata/events"
INGEST_RUNS_CURRENT_PREFIX = f"{METADATA_EVENTS_PREFIX}/ingest_runs_current"
INGEST_RUN_ATTEMPTS_PREFIX = f"{METADATA_EVENTS_PREFIX}/ingest_run_attempts"
METADATA_PARQUET_INGEST_RUNS_PREFIX = "lake/parquet/metadata/ingest_runs/"
METADATA_PARQUET_FILE_MANIFEST_PREFIX = "lake/parquet/metadata/file_manifest/"
EVENT_VERSION = 1
LAKEHOUSE_PLAN_LIMIT = 1


@dataclass(frozen=True)
class IngestRunEvent:
    event_type: str
    event_version: int
    ingest_run_id: str
    dag_id: str
    dataset_name: str
    data_interval_start: datetime
    data_interval_end: datetime
    effective_start: datetime
    raw_s3_path: str | None
    raw_s3_key: str | None
    records_fetched: int
    max_loaded_at: datetime | None
    bytes_written: int | None
    duration_seconds: float | None
    status: str
    started_at: datetime
    completed_at: datetime


@dataclass(frozen=True)
class LakehouseIntervalPlan:
    data_interval_start: datetime
    data_interval_end: datetime
    ingest_event_keys: dict[str, str]
    existing_manifest_event_keys: dict[str, str]
    mode: str
    reason: str


@dataclass(frozen=True)
class FileManifestEvent:
    event_type: str
    event_version: int
    manifest_id: str
    layer: str
    table_name: str
    s3_path: str
    s3_key: str
    data_interval_start: datetime | None
    data_interval_end: datetime | None
    record_count: int
    file_size_bytes: int
    content_hash: str
    written_at: datetime
    promotion_started_at: datetime
    promotion_completed_at: datetime


def run_id_hash(ingest_run_id: str) -> str:
    return hashlib.sha256(ingest_run_id.encode("utf-8")).hexdigest()


def ingest_run_current_event_key(
    *,
    dataset_name: str,
    data_interval_start: datetime,
    data_interval_end: datetime,
) -> str:
    return (
        f"{INGEST_RUNS_CURRENT_PREFIX}/"
        f"dataset_name={dataset_name}/"
        f"data_interval_start={format_interval_partition(data_interval_start)}/"
        f"data_interval_end={format_interval_partition(data_interval_end)}/"
        "event.json"
    )


def ingest_run_attempt_event_key(
    *,
    dataset_name: str,
    data_interval_start: datetime,
    data_interval_end: datetime,
    ingest_run_id: str,
) -> str:
    return (
        f"{INGEST_RUN_ATTEMPTS_PREFIX}/"
        f"dataset_name={dataset_name}/"
        f"data_interval_start={format_interval_partition(data_interval_start)}/"
        f"data_interval_end={format_interval_partition(data_interval_end)}/"
        f"run_id_hash={run_id_hash(ingest_run_id)}.json"
    )


def file_manifest_event_key(
    *,
    layer: str,
    table_name: str,
    data_interval_start: datetime,
    data_interval_end: datetime,
    manifest_id: str,
) -> str:
    return (
        f"{METADATA_EVENTS_PREFIX}/file_manifest/"
        f"layer={layer}/"
        f"table_name={table_name}/"
        f"data_interval_start={format_interval_partition(data_interval_start)}/"
        f"data_interval_end={format_interval_partition(data_interval_end)}/"
        f"manifest_id={manifest_id}.json"
    )


def _serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def event_to_json(event: IngestRunEvent | FileManifestEvent) -> bytes:
    payload = {key: _serialize_value(value) for key, value in asdict(event).items()}
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _parse_event_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    return coerce_utc_datetime(str(value))


def parse_ingest_run_event(payload: Mapping[str, Any]) -> IngestRunEvent:
    return IngestRunEvent(
        event_type=str(payload["event_type"]),
        event_version=int(payload["event_version"]),
        ingest_run_id=str(payload["ingest_run_id"]),
        dag_id=str(payload["dag_id"]),
        dataset_name=str(payload["dataset_name"]),
        data_interval_start=coerce_utc_datetime(payload["data_interval_start"]),
        data_interval_end=coerce_utc_datetime(payload["data_interval_end"]),
        effective_start=coerce_utc_datetime(payload["effective_start"]),
        raw_s3_path=str(payload["raw_s3_path"]) if payload.get("raw_s3_path") else None,
        raw_s3_key=str(payload["raw_s3_key"]) if payload.get("raw_s3_key") else None,
        records_fetched=int(payload["records_fetched"]),
        max_loaded_at=_parse_event_datetime(payload.get("max_loaded_at")),
        bytes_written=int(payload["bytes_written"]) if payload.get("bytes_written") is not None else None,
        duration_seconds=float(payload["duration_seconds"])
        if payload.get("duration_seconds") is not None
        else None,
        status=str(payload["status"]),
        started_at=coerce_utc_datetime(payload["started_at"]),
        completed_at=coerce_utc_datetime(payload["completed_at"]),
    )


def parse_file_manifest_event(payload: Mapping[str, Any]) -> FileManifestEvent:
    return FileManifestEvent(
        event_type=str(payload["event_type"]),
        event_version=int(payload["event_version"]),
        manifest_id=str(payload["manifest_id"]),
        layer=str(payload["layer"]),
        table_name=str(payload["table_name"]),
        s3_path=str(payload["s3_path"]),
        s3_key=str(payload["s3_key"]),
        data_interval_start=_parse_event_datetime(payload.get("data_interval_start")),
        data_interval_end=_parse_event_datetime(payload.get("data_interval_end")),
        record_count=int(payload["record_count"]),
        file_size_bytes=int(payload["file_size_bytes"]),
        content_hash=str(payload["content_hash"]),
        written_at=coerce_utc_datetime(payload["written_at"]),
        promotion_started_at=coerce_utc_datetime(payload["promotion_started_at"]),
        promotion_completed_at=coerce_utc_datetime(payload["promotion_completed_at"]),
    )


def write_ingest_run_event(storage: StorageConfig, event: IngestRunEvent) -> str:
    payload = event_to_json(event)
    current_key = ingest_run_current_event_key(
        dataset_name=event.dataset_name,
        data_interval_start=event.data_interval_start,
        data_interval_end=event.data_interval_end,
    )
    attempt_key = ingest_run_attempt_event_key(
        dataset_name=event.dataset_name,
        data_interval_start=event.data_interval_start,
        data_interval_end=event.data_interval_end,
        ingest_run_id=event.ingest_run_id,
    )
    storage.write_bytes(attempt_key, payload)
    storage.write_bytes(current_key, payload)
    return current_key


def write_file_manifest_event(storage: StorageConfig, event: FileManifestEvent) -> str:
    if event.data_interval_start is None or event.data_interval_end is None:
        raise LakehouseLoadError("file manifest events require interval bounds")
    key = file_manifest_event_key(
        layer=event.layer,
        table_name=event.table_name,
        data_interval_start=event.data_interval_start,
        data_interval_end=event.data_interval_end,
        manifest_id=event.manifest_id,
    )
    storage.write_bytes(key, event_to_json(event))
    return key


def read_ingest_run_event(storage: StorageConfig, key: str) -> IngestRunEvent:
    payload = json.loads(storage.read_bytes(key).decode("utf-8"))
    return parse_ingest_run_event(payload)


def read_file_manifest_event(storage: StorageConfig, key: str) -> FileManifestEvent:
    payload = json.loads(storage.read_bytes(key).decode("utf-8"))
    return parse_file_manifest_event(payload)


def lakehouse_interval_plan_to_dict(plan: LakehouseIntervalPlan) -> dict[str, Any]:
    return {
        "data_interval_start": plan.data_interval_start.astimezone(timezone.utc).isoformat(),
        "data_interval_end": plan.data_interval_end.astimezone(timezone.utc).isoformat(),
        "ingest_event_keys": dict(plan.ingest_event_keys),
        "existing_manifest_event_keys": dict(plan.existing_manifest_event_keys),
        "mode": plan.mode,
        "reason": plan.reason,
    }


def lakehouse_interval_plan_from_dict(payload: Mapping[str, Any]) -> LakehouseIntervalPlan:
    return LakehouseIntervalPlan(
        data_interval_start=coerce_utc_datetime(str(payload["data_interval_start"])),
        data_interval_end=coerce_utc_datetime(str(payload["data_interval_end"])),
        ingest_event_keys=dict(payload["ingest_event_keys"]),
        existing_manifest_event_keys=dict(payload.get("existing_manifest_event_keys", {})),
        mode=str(payload["mode"]),
        reason=str(payload["reason"]),
    )


def _interval_needs_bronze_manifest(ingest_event: IngestRunEvent) -> bool:
    return ingest_event.records_fetched > 0


def _bronze_manifests_for_interval(
    manifest_events: list[FileManifestEvent],
) -> dict[str, str]:
    by_table: dict[str, str] = {}
    for event in manifest_events:
        if event.layer != "bronze":
            continue
        if event.data_interval_start is None or event.data_interval_end is None:
            continue
        by_table[event.table_name] = file_manifest_event_key(
            layer=event.layer,
            table_name=event.table_name,
            data_interval_start=event.data_interval_start,
            data_interval_end=event.data_interval_end,
            manifest_id=event.manifest_id,
        )
    return by_table


def parse_lakehouse_plan_limit(raw: str | int | None = None) -> int:
    """Return the supported lakehouse plan limit for one interval per DAG run."""

    if raw is None:
        value = LAKEHOUSE_PLAN_LIMIT
    else:
        value = int(raw)
    if value != LAKEHOUSE_PLAN_LIMIT:
        raise LakehouseLoadError(
            f"LAKEHOUSE_PLAN_LIMIT must be {LAKEHOUSE_PLAN_LIMIT} for the current "
            f"promote_raw_to_bronze DAG; got {value}"
        )
    return value


def plan_lakehouse_intervals(
    storage: StorageConfig,
    *,
    required_datasets: tuple[str, ...] = ("permits", "evictions", "incidents"),
    mode: str = "pending",
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = LAKEHOUSE_PLAN_LIMIT,
) -> list[LakehouseIntervalPlan]:
    """Select lakehouse intervals from current S3 JSON ingest metadata events."""

    if mode not in {"pending", "refresh"}:
        raise LakehouseLoadError(f"unsupported lakehouse plan mode: {mode!r}")
    limit = parse_lakehouse_plan_limit(limit)

    ingest_events = [
        read_ingest_run_event(storage, key)
        for key in _iter_event_keys(storage, f"{INGEST_RUNS_CURRENT_PREFIX}/")
    ]
    manifest_events = [
        read_file_manifest_event(storage, key)
        for key in _iter_event_keys(storage, f"{METADATA_EVENTS_PREFIX}/file_manifest/")
    ]

    grouped_ingest: dict[tuple[datetime, datetime], dict[str, tuple[str, IngestRunEvent]]] = {}
    for event in ingest_events:
        interval = (event.data_interval_start, event.data_interval_end)
        grouped_ingest.setdefault(interval, {})[event.dataset_name] = (
            ingest_run_current_event_key(
                dataset_name=event.dataset_name,
                data_interval_start=event.data_interval_start,
                data_interval_end=event.data_interval_end,
            ),
            event,
        )

    grouped_manifests: dict[tuple[datetime, datetime], list[FileManifestEvent]] = {}
    for event in manifest_events:
        if event.data_interval_start is None or event.data_interval_end is None:
            continue
        interval = (event.data_interval_start, event.data_interval_end)
        grouped_manifests.setdefault(interval, []).append(event)

    plans: list[LakehouseIntervalPlan] = []
    for interval, dataset_events in sorted(grouped_ingest.items(), key=lambda item: item[0]):
        data_interval_start, data_interval_end = interval
        if start is not None and data_interval_start < start:
            continue
        if end is not None and data_interval_end > end:
            continue
        if set(dataset_events) != set(required_datasets):
            continue

        ingest_event_keys = {name: key for name, (key, _) in dataset_events.items()}
        existing_manifest_event_keys = _bronze_manifests_for_interval(
            grouped_manifests.get(interval, [])
        )
        missing_manifests = [
            dataset_name
            for dataset_name in required_datasets
            if _interval_needs_bronze_manifest(dataset_events[dataset_name][1])
            and dataset_name not in existing_manifest_event_keys
        ]

        if mode == "pending" and not missing_manifests:
            continue

        reason = (
            f"missing bronze manifests for {', '.join(missing_manifests)}"
            if missing_manifests
            else "refresh requested for complete interval"
        )
        plans.append(
            LakehouseIntervalPlan(
                data_interval_start=data_interval_start,
                data_interval_end=data_interval_end,
                ingest_event_keys=ingest_event_keys,
                existing_manifest_event_keys=existing_manifest_event_keys,
                mode=mode,
                reason=reason,
            )
        )

    return plans[:limit]


def load_ingest_run_event_for_interval(
    storage: StorageConfig,
    *,
    dataset_name: str,
    data_interval_start: datetime,
    data_interval_end: datetime,
) -> IngestRunEvent:
    key = ingest_run_current_event_key(
        dataset_name=dataset_name,
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
    )
    if key not in storage.list_keys(f"{INGEST_RUNS_CURRENT_PREFIX}/"):
        raise LakehouseLoadError(
            f"no current ingest metadata event found for {dataset_name} interval "
            f"{format_interval_partition(data_interval_start)}.."
            f"{format_interval_partition(data_interval_end)}"
        )
    return read_ingest_run_event(storage, key)


def ingest_run_event_from_extract(
    *,
    extract_result: Any,
    ingest_run_id: str,
    dag_id: str,
    dataset_name: str,
) -> IngestRunEvent:
    status = "empty" if extract_result.records_fetched == 0 else "success"
    return IngestRunEvent(
        event_type="ingest_run",
        event_version=EVENT_VERSION,
        ingest_run_id=ingest_run_id,
        dag_id=dag_id,
        dataset_name=dataset_name,
        data_interval_start=extract_result.data_interval_start,
        data_interval_end=extract_result.data_interval_end,
        effective_start=extract_result.effective_start,
        raw_s3_path=extract_result.raw_path,
        raw_s3_key=extract_result.raw_key,
        records_fetched=extract_result.records_fetched,
        max_loaded_at=extract_result.max_loaded_at,
        bytes_written=extract_result.bytes_written,
        duration_seconds=extract_result.duration_seconds,
        status=status,
        started_at=extract_result.started_at,
        completed_at=extract_result.completed_at,
    )


def record_extract_metadata(
    *,
    extract_result: Any,
    ingest_run_id: str,
    dag_id: str,
    dataset_name: str,
    storage: StorageConfig | None = None,
) -> str:
    storage = storage or storage_from_env()
    event = ingest_run_event_from_extract(
        extract_result=extract_result,
        ingest_run_id=ingest_run_id,
        dag_id=dag_id,
        dataset_name=dataset_name,
    )
    return write_ingest_run_event(storage, event)


def _iter_event_keys(storage: StorageConfig, prefix: str) -> Iterator[str]:
    for key in storage.list_keys(prefix):
        if key.endswith(".json"):
            yield key


def _contract_row_from_event(event: IngestRunEvent | FileManifestEvent, contract: TableContract) -> dict[str, Any]:
    if isinstance(event, IngestRunEvent):
        source = {
            "ingest_run_id": event.ingest_run_id,
            "dag_id": event.dag_id,
            "dataset_name": event.dataset_name,
            "data_interval_start": event.data_interval_start,
            "data_interval_end": event.data_interval_end,
            "effective_start": event.effective_start,
            "raw_s3_path": event.raw_s3_path,
            "raw_s3_key": event.raw_s3_key,
            "records_fetched": event.records_fetched,
            "max_loaded_at": event.max_loaded_at,
            "bytes_written": event.bytes_written,
            "duration_seconds": event.duration_seconds,
            "status": event.status,
            "started_at": event.started_at,
            "completed_at": event.completed_at,
        }
    else:
        source = {
            "manifest_id": event.manifest_id,
            "layer": event.layer,
            "table_name": event.table_name,
            "s3_path": event.s3_path,
            "s3_key": event.s3_key,
            "data_interval_start": event.data_interval_start,
            "data_interval_end": event.data_interval_end,
            "record_count": event.record_count,
            "file_size_bytes": event.file_size_bytes,
            "content_hash": event.content_hash,
            "written_at": event.written_at,
        }
    return {column.name: source.get(column.name) for column in contract.columns}


def compact_lakehouse_metadata(
    storage: StorageConfig | None = None,
    *,
    contract_root: Path | None = None,
) -> None:
    """Compact S3 metadata events into contract-compatible metadata Parquet."""

    storage = storage or storage_from_env()
    ingest_contract = get_contract("metadata", "ingest_runs", contract_root)
    manifest_contract = get_contract("metadata", "file_manifest", contract_root)

    storage.delete_prefix(METADATA_PARQUET_INGEST_RUNS_PREFIX)
    storage.delete_prefix(METADATA_PARQUET_FILE_MANIFEST_PREFIX)

    ingest_events = [
        read_ingest_run_event(storage, key)
        for key in _iter_event_keys(storage, f"{INGEST_RUNS_CURRENT_PREFIX}/")
    ]
    manifest_events = [
        read_file_manifest_event(storage, key)
        for key in _iter_event_keys(storage, f"{METADATA_EVENTS_PREFIX}/file_manifest/")
    ]

    conn = duckdb.connect(":memory:")
    try:
        _load_events_into_duckdb(conn, ingest_events, ingest_contract, "ingest_runs")
        _load_events_into_duckdb(conn, manifest_events, manifest_contract, "file_manifest")
        _export_compacted_table(
            conn,
            table_name="ingest_runs",
            contract=ingest_contract,
            partition_column="ingest_date",
            partition_source="completed_at",
            storage=storage,
        )
        _export_compacted_table(
            conn,
            table_name="file_manifest",
            contract=manifest_contract,
            partition_column="manifest_date",
            partition_source="written_at",
            storage=storage,
        )
    finally:
        conn.close()


def _load_events_into_duckdb(
    conn: duckdb.DuckDBPyConnection,
    events: list[IngestRunEvent] | list[FileManifestEvent],
    contract: TableContract,
    table_name: str,
) -> None:
    schema = pyarrow_schema_for_contract(contract)
    if events:
        rows = [_contract_row_from_event(event, contract) for event in events]
        arrow_table = pa.Table.from_pylist(rows, schema=schema)
    else:
        arrow_table = pa.Table.from_pylist([], schema=schema)
    source_name = f"{table_name}_src"
    conn.register(source_name, arrow_table)
    column_list = ", ".join(column.name for column in contract.columns)
    conn.execute(f"CREATE TABLE {table_name} AS SELECT {column_list} FROM {source_name}")


def _export_compacted_table(
    conn: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    contract: TableContract,
    partition_column: str,
    partition_source: str,
    storage: StorageConfig,
) -> None:
    conn.execute(
        f"""
        SELECT
            *,
            strftime(CAST({partition_source} AS TIMESTAMPTZ), '%Y%m%d') AS {partition_column}
        FROM {table_name}
        """
    )
    rows = conn.fetchall()
    if not rows:
        return
    columns = [desc[0] for desc in conn.description]
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        payload = dict(zip(columns, row, strict=True))
        partition_value = payload.pop(partition_column)
        grouped.setdefault(str(partition_value), []).append(payload)

    schema = pyarrow_schema_for_contract(contract)
    for partition_value, partition_rows in grouped.items():
        table = pa.Table.from_pylist(partition_rows, schema=schema)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, use_dictionary=False)
        export_key = (
            f"lake/parquet/metadata/{table_name}/"
            f"{partition_column}={partition_value}/records.parquet"
        )
        storage.write_bytes(export_key, sink.getvalue().to_pybytes())
