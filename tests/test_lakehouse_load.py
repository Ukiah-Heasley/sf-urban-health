from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow.parquet as pq
import pytest

from scripts import lakehouse_contracts as lc
from scripts.lakehouse_load import (
    LakehouseLoadError,
    StorageConfig,
    bronze_object_key,
    canonical_record_hash,
    hash_file,
    map_bronze_row,
    promote_raw_to_bronze,
    pyarrow_schema_for_contract,
)
from scripts.lakehouse_metadata import (
    EVENT_VERSION,
    INGEST_RUN_ATTEMPTS_PREFIX,
    METADATA_PARQUET_FILE_MANIFEST_PREFIX,
    METADATA_PARQUET_INGEST_RUNS_PREFIX,
    compact_lakehouse_metadata,
    event_to_json,
    ingest_run_attempt_event_key,
    ingest_run_current_event_key,
    ingest_run_event_from_extract,
    load_ingest_run_event_for_interval,
    plan_lakehouse_intervals,
    read_file_manifest_event,
    read_ingest_run_event,
    record_extract_metadata,
    parse_lakehouse_plan_limit,
    write_ingest_run_event,
)
from scripts.soda_ingest import ExtractResult

_FIXTURES = Path(__file__).resolve().parent / "fixtures" / "lakehouse"
_INTERVAL_START = datetime(2024, 3, 15, 6, 0, tzinfo=timezone.utc)
_INTERVAL_END = datetime(2024, 3, 16, 6, 0, tzinfo=timezone.utc)
_STARTED_AT = datetime(2024, 3, 16, 6, 0, tzinfo=timezone.utc)
_COMPLETED_AT = datetime(2024, 3, 16, 6, 4, tzinfo=timezone.utc)
_COMPLETED_AT_LATER = datetime(2024, 3, 17, 6, 4, tzinfo=timezone.utc)


@pytest.fixture
def contract_root() -> Path:
    return Path(__file__).resolve().parents[1] / "contracts" / "lakehouse"


@pytest.fixture
def lake_root(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture
def storage(lake_root: Path) -> StorageConfig:
    return StorageConfig(bucket=None, local_root=lake_root, s3_client=None)


def _extract_result(
    *,
    records_fetched: int,
    raw_path: str | None = None,
    raw_key: str | None = None,
) -> ExtractResult:
    return ExtractResult(
        raw_path=raw_path,
        raw_key=raw_key,
        records_fetched=records_fetched,
        max_loaded_at=_COMPLETED_AT,
        bytes_written=128 if records_fetched else 0,
        duration_seconds=4.0,
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
        effective_start=_INTERVAL_START,
        started_at=_STARTED_AT,
        completed_at=_COMPLETED_AT,
    )


def _seed_raw(storage: StorageConfig, dataset_name: str, fixture_name: str) -> tuple[str, str, int]:
    raw_key = (
        f"raw/{dataset_name}/"
        f"data_interval_start=20240315T060000Z/"
        f"data_interval_end=20240316T060000Z/"
        "records.ndjson"
    )
    destination = storage.local_root / raw_key
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(_FIXTURES / fixture_name, destination)
    return storage.uri_for_key(raw_key), raw_key, sum(1 for _ in destination.read_text().splitlines() if _.strip())


def _write_ingest_event(
    storage: StorageConfig,
    *,
    dataset_name: str,
    records_fetched: int,
    raw_path: str | None = None,
    raw_key: str | None = None,
    ingest_run_id: str = "run-1",
) -> str:
    event = ingest_run_event_from_extract(
        extract_result=_extract_result(
            records_fetched=records_fetched,
            raw_path=raw_path,
            raw_key=raw_key,
        ),
        ingest_run_id=ingest_run_id,
        dag_id=f"ingest_{dataset_name}",
        dataset_name=dataset_name,
    )
    return write_ingest_run_event(storage, event)


_INTERVAL_START_2 = datetime(2024, 3, 16, 6, 0, tzinfo=timezone.utc)
_INTERVAL_END_2 = datetime(2024, 3, 17, 6, 0, tzinfo=timezone.utc)


def _extract_result_for_interval(
    *,
    interval_start: datetime,
    interval_end: datetime,
    records_fetched: int,
    raw_path: str | None = None,
    raw_key: str | None = None,
) -> ExtractResult:
    return ExtractResult(
        raw_path=raw_path,
        raw_key=raw_key,
        records_fetched=records_fetched,
        max_loaded_at=_COMPLETED_AT,
        bytes_written=128 if records_fetched else 0,
        duration_seconds=4.0,
        data_interval_start=interval_start,
        data_interval_end=interval_end,
        effective_start=interval_start,
        started_at=_STARTED_AT,
        completed_at=_COMPLETED_AT,
    )


def _write_ingest_event_for_interval(
    storage: StorageConfig,
    *,
    dataset_name: str,
    interval_start: datetime,
    interval_end: datetime,
    records_fetched: int,
    raw_path: str | None = None,
    raw_key: str | None = None,
    ingest_run_id: str = "run-1",
) -> str:
    event = ingest_run_event_from_extract(
        extract_result=_extract_result_for_interval(
            interval_start=interval_start,
            interval_end=interval_end,
            records_fetched=records_fetched,
            raw_path=raw_path,
            raw_key=raw_key,
        ),
        ingest_run_id=ingest_run_id,
        dag_id=f"ingest_{dataset_name}",
        dataset_name=dataset_name,
    )
    return write_ingest_run_event(storage, event)


def _seed_raw_for_interval(
    storage: StorageConfig,
    dataset_name: str,
    fixture_name: str,
    *,
    interval_start: datetime,
    interval_end: datetime,
) -> tuple[str, str, int]:
    from scripts.lakehouse_load import format_interval_partition

    raw_key = (
        f"raw/{dataset_name}/"
        f"data_interval_start={format_interval_partition(interval_start)}/"
        f"data_interval_end={format_interval_partition(interval_end)}/"
        "records.ndjson"
    )
    destination = storage.local_root / raw_key
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(_FIXTURES / fixture_name, destination)
    return storage.uri_for_key(raw_key), raw_key, sum(1 for _ in destination.read_text().splitlines() if _.strip())


def _seed_all_datasets(
    storage: StorageConfig,
    *,
    interval_start: datetime = _INTERVAL_START,
    interval_end: datetime = _INTERVAL_END,
    ingest_run_suffix: str = "1",
) -> None:
    for dataset_name, fixture_name in [
        ("permits", "permits.ndjson"),
        ("evictions", "evictions.ndjson"),
        ("incidents", "incidents.ndjson"),
    ]:
        raw_path, raw_key, records = _seed_raw_for_interval(
            storage,
            dataset_name,
            fixture_name,
            interval_start=interval_start,
            interval_end=interval_end,
        )
        _write_ingest_event_for_interval(
            storage,
            dataset_name=dataset_name,
            interval_start=interval_start,
            interval_end=interval_end,
            records_fetched=records,
            raw_path=raw_path,
            raw_key=raw_key,
            ingest_run_id=f"run-{dataset_name}-{ingest_run_suffix}",
        )


@pytest.mark.parametrize(
    ("dataset_name", "dataset_id", "fixture_name", "natural_key"),
    [
        ("permits", "i98e-djp9", "permits.ndjson", "permit_number"),
        ("evictions", "5cei-gny5", "evictions.ndjson", "eviction_id"),
        ("incidents", "wg3w-h783", "incidents.ndjson", "row_id"),
    ],
)
def test_typed_mappings_for_datasets(
    dataset_name: str,
    dataset_id: str,
    fixture_name: str,
    natural_key: str,
    contract_root: Path,
    storage: StorageConfig,
) -> None:
    contract = lc.get_contract("bronze", dataset_name, contract_root)
    raw_path, raw_key, _ = _seed_raw(storage, dataset_name, fixture_name)
    payload = json.loads((_FIXTURES / fixture_name).read_text().splitlines()[0])
    row = map_bronze_row(
        dataset_name=dataset_name,
        contract=contract,
        payload=payload,
        ingest_run_id="run-1",
        raw_s3_path=raw_path,
        raw_s3_key=raw_key,
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
        effective_start=_INTERVAL_START,
        extracted_at=_COMPLETED_AT,
        source_dataset_id=dataset_id,
    )
    assert row[natural_key] is not None
    if dataset_name == "permits":
        assert row["status"] == "issued"
        assert row["_extracted_at"] == _COMPLETED_AT

def test_required_bronze_metadata_columns(contract_root: Path, storage: StorageConfig) -> None:
    contract = lc.get_contract("bronze", "permits", contract_root)
    raw_path, raw_key, _ = _seed_raw(storage, "permits", "permits.ndjson")
    payload = json.loads((_FIXTURES / "permits.ndjson").read_text().splitlines()[0])
    row = map_bronze_row(
        dataset_name="permits",
        contract=contract,
        payload=payload,
        ingest_run_id="run-1",
        raw_s3_path=raw_path,
        raw_s3_key=raw_key,
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
        effective_start=_INTERVAL_START,
        extracted_at=_COMPLETED_AT,
        source_dataset_id="i98e-djp9",
    )
    for column in lc.BRONZE_METADATA_COLUMNS:
        assert column["name"] in row
        assert row[column["name"]] is not None


def test_permit_integer_fields_accept_integral_decimal_strings(
    contract_root: Path,
    storage: StorageConfig,
) -> None:
    contract = lc.get_contract("bronze", "permits", contract_root)
    raw_path, raw_key, _ = _seed_raw(storage, "permits", "permits.ndjson")
    payload = json.loads((_FIXTURES / "permits.ndjson").read_text().splitlines()[0])
    payload["existing_units"] = "2.0"
    payload["proposed_units"] = "275.0"
    payload["number_of_existing_stories"] = "3.0"
    payload["number_of_proposed_stories"] = "4.0"

    row = map_bronze_row(
        dataset_name="permits",
        contract=contract,
        payload=payload,
        ingest_run_id="run-1",
        raw_s3_path=raw_path,
        raw_s3_key=raw_key,
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
        effective_start=_INTERVAL_START,
        extracted_at=_COMPLETED_AT,
        source_dataset_id="i98e-djp9",
    )

    assert row["existing_units"] == 2
    assert row["proposed_units"] == 275
    assert row["existing_stories"] == 3
    assert row["proposed_stories"] == 4


def test_parquet_schema_matches_contract(contract_root: Path, storage: StorageConfig) -> None:
    contract = lc.get_contract("bronze", "permits", contract_root)
    raw_path, raw_key, records = _seed_raw(storage, "permits", "permits.ndjson")
    _write_ingest_event(
        storage,
        dataset_name="permits",
        records_fetched=records,
        raw_path=raw_path,
        raw_key=raw_key,
    )
    ingest_event = load_ingest_run_event_for_interval(
        storage,
        dataset_name="permits",
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
    )
    result = promote_raw_to_bronze(
        dataset_name="permits",
        dataset_id="i98e-djp9",
        ingest_event=ingest_event,
        storage=storage,
        contract_root=contract_root,
    )
    table = pq.ParquetFile(result.bronze_path).read()
    expected = pyarrow_schema_for_contract(contract)
    assert table.schema.equals(expected, check_metadata=False)


def test_bronze_path_rendering_is_deterministic(contract_root: Path) -> None:
    contract = lc.get_contract("bronze", "permits", contract_root)
    key = bronze_object_key(
        contract,
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
    )
    assert key.endswith("records.parquet")


def test_record_hash_is_deterministic() -> None:
    payload = {"permit_number": "1", "status": "issued", "data_loaded_at": "2024-01-01T00:00:00.000"}
    expected = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    assert canonical_record_hash(payload) == expected


def test_extract_metadata_event_writes_current_and_attempt_keys(storage: StorageConfig) -> None:
    result = _extract_result(records_fetched=0)
    current_key = record_extract_metadata(
        extract_result=result,
        ingest_run_id="run-deterministic",
        dag_id="ingest_permits",
        dataset_name="permits",
        storage=storage,
    )
    second_key = record_extract_metadata(
        extract_result=result,
        ingest_run_id="run-deterministic",
        dag_id="ingest_permits",
        dataset_name="permits",
        storage=storage,
    )
    assert current_key == second_key
    assert current_key == ingest_run_current_event_key(
        dataset_name="permits",
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
    )
    attempt_key = ingest_run_attempt_event_key(
        dataset_name="permits",
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
        ingest_run_id="run-deterministic",
    )
    assert attempt_key.startswith(f"{INGEST_RUN_ATTEMPTS_PREFIX}/")
    assert storage.list_keys(f"{INGEST_RUN_ATTEMPTS_PREFIX}/") == [attempt_key]


def test_current_ingest_event_overwrites_prior_for_same_interval(storage: StorageConfig) -> None:
    first_current = _write_ingest_event(
        storage,
        dataset_name="permits",
        records_fetched=0,
        ingest_run_id="run-first",
    )
    second_current = _write_ingest_event(
        storage,
        dataset_name="permits",
        records_fetched=5,
        ingest_run_id="run-second",
    )
    assert first_current == second_current
    event = read_ingest_run_event(storage, second_current)
    assert event.ingest_run_id == "run-second"
    assert event.records_fetched == 5
    attempt_keys = storage.list_keys(f"{INGEST_RUN_ATTEMPTS_PREFIX}/dataset_name=permits/")
    assert len(attempt_keys) == 2


def test_planner_reads_current_events_not_attempt_events(storage: StorageConfig) -> None:
    for dataset_name in ("permits", "evictions", "incidents"):
        stale_event = ingest_run_event_from_extract(
            extract_result=_extract_result(records_fetched=999),
            ingest_run_id=f"stale-{dataset_name}",
            dag_id=f"ingest_{dataset_name}",
            dataset_name=dataset_name,
        )
        from scripts.lakehouse_metadata import event_to_json

        attempt_key = ingest_run_attempt_event_key(
            dataset_name=dataset_name,
            data_interval_start=_INTERVAL_START,
            data_interval_end=_INTERVAL_END,
            ingest_run_id=f"stale-{dataset_name}",
        )
        storage.write_bytes(attempt_key, event_to_json(stale_event))

    assert plan_lakehouse_intervals(storage) == []

    _seed_all_datasets(storage)
    plans = plan_lakehouse_intervals(storage)
    assert len(plans) == 1
    for dataset_name in ("permits", "evictions", "incidents"):
        event = read_ingest_run_event(storage, plans[0].ingest_event_keys[dataset_name])
        assert event.records_fetched != 999
        assert event.ingest_run_id.startswith("run-")


def test_backfill_rerun_uses_latest_current_event(storage: StorageConfig) -> None:
    raw_path, raw_key, records = _seed_raw(storage, "permits", "permits.ndjson")
    _write_ingest_event(
        storage,
        dataset_name="permits",
        records_fetched=records,
        raw_path=raw_path,
        raw_key=raw_key,
        ingest_run_id="scheduled__2024-03-16T06:00:00+00:00",
    )
    _write_ingest_event(
        storage,
        dataset_name="permits",
        records_fetched=1,
        raw_path=raw_path,
        raw_key=raw_key,
        ingest_run_id="manual__2026-06-25T12:00:00+00:00",
    )
    event = load_ingest_run_event_for_interval(
        storage,
        dataset_name="permits",
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
    )
    assert event.ingest_run_id == "manual__2026-06-25T12:00:00+00:00"
    assert event.records_fetched == 1


def test_extract_metadata_event_uses_extract_timestamps(storage: StorageConfig) -> None:
    key = record_extract_metadata(
        extract_result=_extract_result(records_fetched=0),
        ingest_run_id="run-ts",
        dag_id="ingest_permits",
        dataset_name="permits",
        storage=storage,
    )
    event = read_ingest_run_event(storage, key)
    assert event.started_at == _STARTED_AT
    assert event.completed_at == _COMPLETED_AT
    assert event.event_type == "ingest_run"
    assert event.event_version == EVENT_VERSION


def test_empty_interval_writes_ingest_metadata_but_no_bronze_manifest(
    storage: StorageConfig,
    contract_root: Path,
) -> None:
    _write_ingest_event(storage, dataset_name="permits", records_fetched=0)
    ingest_event = load_ingest_run_event_for_interval(
        storage,
        dataset_name="permits",
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
    )
    result = promote_raw_to_bronze(
        dataset_name="permits",
        dataset_id="i98e-djp9",
        ingest_event=ingest_event,
        storage=storage,
        contract_root=contract_root,
    )
    assert result.bronze_path is None
    assert result.manifest_event_key is None
    manifest_events = storage.list_keys("lake/metadata/events/file_manifest/")
    assert manifest_events == []


def test_row_count_mismatch_fails_without_manifest_event(
    storage: StorageConfig,
    contract_root: Path,
) -> None:
    raw_path, raw_key, _ = _seed_raw(storage, "permits", "permits.ndjson")
    contract = lc.get_contract("bronze", "permits", contract_root)
    bronze_key = bronze_object_key(
        contract,
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
    )
    event = ingest_run_event_from_extract(
        extract_result=_extract_result(records_fetched=3, raw_path=raw_path, raw_key=raw_key),
        ingest_run_id="run-mismatch",
        dag_id="ingest_permits",
        dataset_name="permits",
    )
    with pytest.raises(LakehouseLoadError, match="promoted 5 rows but expected 3"):
        promote_raw_to_bronze(
            dataset_name="permits",
            dataset_id="i98e-djp9",
            ingest_event=event,
            storage=storage,
            contract_root=contract_root,
        )
    assert storage.list_keys("lake/metadata/events/file_manifest/") == []
    assert not (storage.local_root / bronze_key).exists()


def test_missing_natural_key_fails(contract_root: Path, storage: StorageConfig) -> None:
    contract = lc.get_contract("bronze", "permits", contract_root)
    payload = {"data_loaded_at": "2024-03-15T00:00:00.000"}
    with pytest.raises(LakehouseLoadError, match="missing required natural key"):
        map_bronze_row(
            dataset_name="permits",
            contract=contract,
            payload=payload,
            ingest_run_id="run-1",
            raw_s3_path="file:///tmp/raw",
            raw_s3_key="raw/permits/records.ndjson",
            data_interval_start=_INTERVAL_START,
            data_interval_end=_INTERVAL_END,
            effective_start=_INTERVAL_START,
            extracted_at=_COMPLETED_AT,
            source_dataset_id="i98e-djp9",
        )


def test_invalid_loaded_at_fails(contract_root: Path, storage: StorageConfig) -> None:
    contract = lc.get_contract("bronze", "permits", contract_root)
    payload = {"permit_number": "1", "data_loaded_at": "not-a-timestamp"}
    with pytest.raises(LakehouseLoadError, match="invalid data_loaded_at"):
        map_bronze_row(
            dataset_name="permits",
            contract=contract,
            payload=payload,
            ingest_run_id="run-1",
            raw_s3_path="file:///tmp/raw",
            raw_s3_key="raw/permits/records.ndjson",
            data_interval_start=_INTERVAL_START,
            data_interval_end=_INTERVAL_END,
            effective_start=_INTERVAL_START,
            extracted_at=_COMPLETED_AT,
            source_dataset_id="i98e-djp9",
        )


def test_promotion_writes_file_manifest_event_without_duckdb(
    storage: StorageConfig,
    contract_root: Path,
) -> None:
    raw_path, raw_key, records = _seed_raw(storage, "permits", "permits.ndjson")
    _write_ingest_event(
        storage,
        dataset_name="permits",
        records_fetched=records,
        raw_path=raw_path,
        raw_key=raw_key,
    )
    ingest_event = load_ingest_run_event_for_interval(
        storage,
        dataset_name="permits",
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
    )
    result = promote_raw_to_bronze(
        dataset_name="permits",
        dataset_id="i98e-djp9",
        ingest_event=ingest_event,
        storage=storage,
        contract_root=contract_root,
    )
    assert result.manifest_event_key is not None
    assert storage.list_keys("lake/metadata/events/file_manifest/") == [result.manifest_event_key]


def test_promotion_batches_without_full_interval_accumulation(
    storage: StorageConfig,
    contract_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_path, raw_key, records = _seed_raw(storage, "permits", "permits.ndjson")
    _write_ingest_event(
        storage,
        dataset_name="permits",
        records_fetched=records,
        raw_path=raw_path,
        raw_key=raw_key,
    )
    ingest_event = load_ingest_run_event_for_interval(
        storage,
        dataset_name="permits",
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
    )
    writer_calls: list[int] = []

    def _capture_writer(path, schema, use_dictionary=False):
        handle = MagicMock()

        def _write_table(table):
            writer_calls.append(table.num_rows)

        handle.write_table.side_effect = _write_table
        handle.close.return_value = None
        return handle

    monkeypatch.setattr("scripts.lakehouse_load.pq.ParquetWriter", _capture_writer)
    promote_raw_to_bronze(
        dataset_name="permits",
        dataset_id="i98e-djp9",
        ingest_event=ingest_event,
        storage=storage,
        contract_root=contract_root,
        batch_size=1,
    )
    assert writer_calls == [1, 1, 1, 1, 1]
    assert sum(writer_calls) == records


def test_metadata_compaction_writes_contract_parquet(
    storage: StorageConfig,
    contract_root: Path,
    lake_root: Path,
) -> None:
    raw_path, raw_key, records = _seed_raw(storage, "permits", "permits.ndjson")
    _write_ingest_event(
        storage,
        dataset_name="permits",
        records_fetched=records,
        raw_path=raw_path,
        raw_key=raw_key,
    )
    ingest_event = load_ingest_run_event_for_interval(
        storage,
        dataset_name="permits",
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
    )
    promote_raw_to_bronze(
        dataset_name="permits",
        dataset_id="i98e-djp9",
        ingest_event=ingest_event,
        storage=storage,
        contract_root=contract_root,
    )
    compact_lakehouse_metadata(storage=storage, contract_root=contract_root)
    assert any((lake_root / "lake/parquet/metadata/ingest_runs").rglob("records.parquet"))
    assert any((lake_root / "lake/parquet/metadata/file_manifest").rglob("records.parquet"))
    manifest_events = storage.list_keys("lake/metadata/events/file_manifest/")
    assert len(manifest_events) == 1
    from scripts.lakehouse_metadata import read_file_manifest_event

    for event_key in manifest_events:
        event = read_file_manifest_event(storage, event_key)
        assert event.layer == "bronze"


def test_ingest_dag_task_order() -> None:
    pytest.importorskip("airflow.models", reason="airflow not installed")
    from dag_factory import DagConfig, make_ingest_dag
    from pipeline_assets import PERMITS_INGEST_ASSET
    from scripts.permits import PERMITS_CONFIG

    dag = make_ingest_dag(
        DagConfig(
            dataset=PERMITS_CONFIG,
            schedule="0 6 * * *",
            start_date=datetime(2026, 5, 1, tzinfo=timezone.utc),
            tags=["test"],
        )
    )
    assert [task.task_id for task in dag.tasks] == [
        "extract_permits_to_raw",
        "record_permits_extract_metadata",
        "ingest_complete",
    ]
    complete = dag.get_task("ingest_complete")
    assert complete.outlets == [PERMITS_INGEST_ASSET]
    assert complete.upstream_task_ids == {"record_permits_extract_metadata"}


def test_transform_lakehouse_dag_depends_on_ingest_assets() -> None:
    pytest.importorskip("airflow.models", reason="airflow not installed")
    from transform_lakehouse import dag

    assert dag.dag_id == "transform_lakehouse"
    assert [task.task_id for task in dag.tasks] == [
        "select_lakehouse_interval",
        "branch_on_lakehouse_plan",
        "promote_permits_to_bronze",
        "promote_evictions_to_bronze",
        "promote_incidents_to_bronze",
        "compact_lakehouse_metadata",
        "lakehouse_transform_complete",
        "lakehouse_noop",
    ]
    select = dag.get_task("select_lakehouse_interval")
    branch = dag.get_task("branch_on_lakehouse_plan")
    promote_permits = dag.get_task("promote_permits_to_bronze")
    compact = dag.get_task("compact_lakehouse_metadata")
    noop = dag.get_task("lakehouse_noop")
    complete = dag.get_task("lakehouse_transform_complete")
    assert select.downstream_task_ids == {"branch_on_lakehouse_plan"}
    assert branch.downstream_task_ids == {"promote_permits_to_bronze", "lakehouse_noop"}
    assert promote_permits.upstream_task_ids == {"branch_on_lakehouse_plan"}
    assert compact.upstream_task_ids == {"promote_incidents_to_bronze"}
    assert noop.outlets == []
    assert complete.outlets


def test_planner_requires_all_datasets_for_interval(storage: StorageConfig) -> None:
    _write_ingest_event(storage, dataset_name="permits", records_fetched=0)
    _write_ingest_event(storage, dataset_name="evictions", records_fetched=0)
    assert plan_lakehouse_intervals(storage) == []


def test_planner_groups_complete_intervals(storage: StorageConfig) -> None:
    _seed_all_datasets(storage)
    plans = plan_lakehouse_intervals(storage, limit=1)
    assert len(plans) == 1
    assert plans[0].data_interval_start == _INTERVAL_START
    assert set(plans[0].ingest_event_keys) == {"permits", "evictions", "incidents"}


def test_planner_pending_skips_complete_intervals(
    storage: StorageConfig,
    contract_root: Path,
) -> None:
    _seed_all_datasets(storage)
    for dataset_name, dataset_id in [
        ("permits", "i98e-djp9"),
        ("evictions", "5cei-gny5"),
        ("incidents", "wg3w-h783"),
    ]:
        ingest_event = load_ingest_run_event_for_interval(
            storage,
            dataset_name=dataset_name,
            data_interval_start=_INTERVAL_START,
            data_interval_end=_INTERVAL_END,
        )
        promote_raw_to_bronze(
            dataset_name=dataset_name,
            dataset_id=dataset_id,
            ingest_event=ingest_event,
            storage=storage,
            contract_root=contract_root,
        )
    assert plan_lakehouse_intervals(storage, mode="pending") == []


def test_planner_refresh_includes_complete_intervals(
    storage: StorageConfig,
    contract_root: Path,
) -> None:
    _seed_all_datasets(storage)
    ingest_event = load_ingest_run_event_for_interval(
        storage,
        dataset_name="permits",
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
    )
    promote_raw_to_bronze(
        dataset_name="permits",
        dataset_id="i98e-djp9",
        ingest_event=ingest_event,
        storage=storage,
        contract_root=contract_root,
    )
    plans = plan_lakehouse_intervals(storage, mode="refresh", limit=1)
    assert len(plans) == 1
    assert "permits" in plans[0].existing_manifest_event_keys


def test_planner_applies_start_end_limit_and_oldest_first(storage: StorageConfig) -> None:
    _seed_all_datasets(storage, interval_start=_INTERVAL_START, interval_end=_INTERVAL_END, ingest_run_suffix="a")
    _seed_all_datasets(
        storage,
        interval_start=_INTERVAL_START_2,
        interval_end=_INTERVAL_END_2,
        ingest_run_suffix="b",
    )
    plans = plan_lakehouse_intervals(
        storage,
        start=_INTERVAL_START_2,
        end=_INTERVAL_END_2,
        limit=1,
    )
    assert len(plans) == 1
    assert plans[0].data_interval_start == _INTERVAL_START_2

    oldest_first = plan_lakehouse_intervals(storage, limit=1)
    assert len(oldest_first) == 1
    assert oldest_first[0].data_interval_start == _INTERVAL_START


def test_compaction_succeeds_with_ingest_events_only(
    storage: StorageConfig,
    contract_root: Path,
    lake_root: Path,
) -> None:
    _write_ingest_event(storage, dataset_name="permits", records_fetched=0)
    compact_lakehouse_metadata(storage=storage, contract_root=contract_root)
    assert any((lake_root / "lake/parquet/metadata/ingest_runs").rglob("records.parquet"))
    assert not (lake_root / "lake/parquet/metadata/file_manifest").exists()


def test_storage_streams_raw_reads(storage: StorageConfig, monkeypatch: pytest.MonkeyPatch) -> None:
    raw_path, raw_key, _ = _seed_raw(storage, "permits", "permits.ndjson")
    read_calls: list[str] = []

    original_open = Path.open

    def _tracked_open(self, *args, **kwargs):
        if self == storage.local_root / raw_key:
            read_calls.append("opened")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", _tracked_open)
    lines = list(storage.read_text_lines(raw_key))
    assert read_calls == ["opened"]
    assert len(lines) == 5


def test_storage_write_file_uploads_local_copy(storage: StorageConfig, tmp_path: Path) -> None:
    source = tmp_path / "payload.parquet"
    source.write_bytes(b"parquet-bytes")
    size = storage.write_file("lake/parquet/bronze/permits/records.parquet", source)
    destination = storage.local_root / "lake/parquet/bronze/permits/records.parquet"
    assert size == destination.stat().st_size
    assert destination.read_bytes() == b"parquet-bytes"


def test_hash_file_streams_without_loading_entire_file(tmp_path: Path) -> None:
    source = tmp_path / "large.bin"
    source.write_bytes(b"abc" * 1000)
    expected = hashlib.sha256(source.read_bytes()).hexdigest()
    assert hash_file(source) == expected


def test_transform_lakehouse_noop_when_no_interval_selected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("airflow.models", reason="airflow not installed")
    from transform_lakehouse import (
        BranchOnLakehousePlanOperator,
        _compact_metadata,
        _promote_dataset,
        _select_lakehouse_interval,
    )

    monkeypatch.setattr(
        "transform_lakehouse.plan_lakehouse_intervals",
        lambda *args, **kwargs: [],
    )

    class _Ti:
        def __init__(self) -> None:
            self.values: dict[str, object] = {}

        def xcom_push(self, key: str, value: object) -> None:
            self.values[key] = value

        def xcom_pull(self, task_ids: str, key: str) -> object:
            return self.values.get(key)

    context = {"ti": _Ti()}
    assert _select_lakehouse_interval(**context) is None
    assert _promote_dataset("permits", **context) is None
    _compact_metadata(**context)
    branch = BranchOnLakehousePlanOperator(task_id="branch_on_lakehouse_plan")
    assert branch.choose_branch(context) == "lakehouse_noop"


def test_ingest_runs_contract_grain_matches_current_state(contract_root: Path) -> None:
    contract = lc.get_contract("metadata", "ingest_runs", contract_root)
    assert contract.grain == ("dataset_name", "data_interval_start", "data_interval_end")


def test_lakehouse_plan_limit_must_be_one() -> None:
    assert parse_lakehouse_plan_limit(1) == 1
    with pytest.raises(LakehouseLoadError, match="LAKEHOUSE_PLAN_LIMIT must be 1"):
        parse_lakehouse_plan_limit(2)
    with pytest.raises(LakehouseLoadError, match="LAKEHOUSE_PLAN_LIMIT must be 1"):
        plan_lakehouse_intervals(MagicMock(), limit=2)


def test_promotion_uses_selected_ingest_event_key(
    storage: StorageConfig,
    contract_root: Path,
) -> None:
    _seed_all_datasets(storage)
    plans = plan_lakehouse_intervals(storage, limit=1)
    ingest_event = read_ingest_run_event(storage, plans[0].ingest_event_keys["permits"])
    result = promote_raw_to_bronze(
        dataset_name="permits",
        dataset_id="i98e-djp9",
        ingest_event=ingest_event,
        storage=storage,
        contract_root=contract_root,
    )
    assert result.bronze_path is not None
    assert result.records_promoted == 5


def _write_ingest_event_with_completed_at(
    storage: StorageConfig,
    *,
    dataset_name: str,
    records_fetched: int,
    completed_at: datetime,
    raw_path: str | None = None,
    raw_key: str | None = None,
    ingest_run_id: str = "run-1",
) -> str:
    event = ingest_run_event_from_extract(
        extract_result=_extract_result(
            records_fetched=records_fetched,
            raw_path=raw_path,
            raw_key=raw_key,
        ),
        ingest_run_id=ingest_run_id,
        dag_id=f"ingest_{dataset_name}",
        dataset_name=dataset_name,
    )
    return write_ingest_run_event(storage, replace(event, completed_at=completed_at))


def test_attempt_write_failure_does_not_advance_current(
    storage: StorageConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = ingest_run_event_from_extract(
        extract_result=_extract_result(records_fetched=0),
        ingest_run_id="run-attempt-fail",
        dag_id="ingest_permits",
        dataset_name="permits",
    )
    current_key = ingest_run_current_event_key(
        dataset_name="permits",
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
    )
    original_write = StorageConfig.write_bytes

    def _failing_write(self: StorageConfig, key: str, payload: bytes) -> int:
        if key.startswith(f"{INGEST_RUN_ATTEMPTS_PREFIX}/"):
            raise RuntimeError("attempt write failed")
        return original_write(self, key, payload)

    monkeypatch.setattr(StorageConfig, "write_bytes", _failing_write)
    with pytest.raises(RuntimeError, match="attempt write failed"):
        write_ingest_run_event(storage, event)
    assert not (storage.local_root / current_key).exists()


def test_current_write_failure_after_attempt_raises(
    storage: StorageConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = ingest_run_event_from_extract(
        extract_result=_extract_result(records_fetched=0),
        ingest_run_id="run-current-fail",
        dag_id="ingest_permits",
        dataset_name="permits",
    )
    current_key = ingest_run_current_event_key(
        dataset_name="permits",
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
    )
    attempt_key = ingest_run_attempt_event_key(
        dataset_name="permits",
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
        ingest_run_id="run-current-fail",
    )
    original_write = StorageConfig.write_bytes

    def _failing_write(self: StorageConfig, key: str, payload: bytes) -> int:
        if key == current_key:
            raise RuntimeError("current write failed")
        return original_write(self, key, payload)

    monkeypatch.setattr(StorageConfig, "write_bytes", _failing_write)
    with pytest.raises(RuntimeError, match="current write failed"):
        write_ingest_run_event(storage, event)
    assert (storage.local_root / attempt_key).exists()
    assert not (storage.local_root / current_key).exists()


def test_metadata_compaction_replaces_stale_ingest_partitions(
    storage: StorageConfig,
    contract_root: Path,
    lake_root: Path,
) -> None:
    _write_ingest_event_with_completed_at(
        storage,
        dataset_name="permits",
        records_fetched=0,
        completed_at=_COMPLETED_AT,
    )
    compact_lakehouse_metadata(storage=storage, contract_root=contract_root)
    ingest_root = lake_root / "lake/parquet/metadata/ingest_runs"
    first_partitions = list(ingest_root.rglob("records.parquet"))
    assert len(first_partitions) == 1
    old_partition = first_partitions[0]

    event = ingest_run_event_from_extract(
        extract_result=_extract_result(records_fetched=0),
        ingest_run_id="run-rerun",
        dag_id="ingest_permits",
        dataset_name="permits",
    )
    write_ingest_run_event(storage, replace(event, completed_at=_COMPLETED_AT_LATER))
    compact_lakehouse_metadata(storage=storage, contract_root=contract_root)

    assert not old_partition.exists()
    rebuilt_partitions = list(ingest_root.rglob("records.parquet"))
    assert len(rebuilt_partitions) == 1
    assert rebuilt_partitions[0] != old_partition


def test_metadata_compaction_replaces_stale_file_manifest_partitions(
    storage: StorageConfig,
    contract_root: Path,
    lake_root: Path,
) -> None:
    raw_path, raw_key, records = _seed_raw(storage, "permits", "permits.ndjson")
    _write_ingest_event(
        storage,
        dataset_name="permits",
        records_fetched=records,
        raw_path=raw_path,
        raw_key=raw_key,
    )
    ingest_event = load_ingest_run_event_for_interval(
        storage,
        dataset_name="permits",
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
    )
    result = promote_raw_to_bronze(
        dataset_name="permits",
        dataset_id="i98e-djp9",
        ingest_event=ingest_event,
        storage=storage,
        contract_root=contract_root,
    )
    assert result.manifest_event_key is not None

    compact_lakehouse_metadata(storage=storage, contract_root=contract_root)
    manifest_root = lake_root / "lake/parquet/metadata/file_manifest"
    first_partitions = list(manifest_root.rglob("records.parquet"))
    assert len(first_partitions) == 1
    old_partition = first_partitions[0]

    manifest_event = read_file_manifest_event(storage, result.manifest_event_key)
    updated_event = replace(
        manifest_event,
        written_at=_COMPLETED_AT_LATER,
        promotion_completed_at=_COMPLETED_AT_LATER,
    )
    storage.write_bytes(result.manifest_event_key, event_to_json(updated_event))
    compact_lakehouse_metadata(storage=storage, contract_root=contract_root)

    assert not old_partition.exists()
    rebuilt_partitions = list(manifest_root.rglob("records.parquet"))
    assert len(rebuilt_partitions) == 1
    assert rebuilt_partitions[0] != old_partition


def test_metadata_compaction_deletes_prefixes_before_rebuild(
    storage: StorageConfig,
    contract_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_ingest_event(storage, dataset_name="permits", records_fetched=0)
    compact_lakehouse_metadata(storage=storage, contract_root=contract_root)

    delete_calls: list[str] = []
    original_delete = StorageConfig.delete_prefix

    def _tracked_delete(self: StorageConfig, prefix: str) -> None:
        delete_calls.append(prefix)
        original_delete(self, prefix)

    write_calls: list[str] = []
    original_write = StorageConfig.write_bytes

    def _tracked_write(self: StorageConfig, key: str, payload: bytes) -> int:
        if key.startswith("lake/parquet/metadata/"):
            write_calls.append(key)
        return original_write(self, key, payload)

    monkeypatch.setattr(StorageConfig, "delete_prefix", _tracked_delete)
    monkeypatch.setattr(StorageConfig, "write_bytes", _tracked_write)
    compact_lakehouse_metadata(storage=storage, contract_root=contract_root)

    assert delete_calls == [
        METADATA_PARQUET_INGEST_RUNS_PREFIX,
        METADATA_PARQUET_FILE_MANIFEST_PREFIX,
    ]
    assert write_calls
    assert all(
        call.startswith("lake/parquet/metadata/")
        for call in write_calls
    )


def test_transform_lakehouse_max_active_runs_is_one() -> None:
    pytest.importorskip("airflow.models", reason="airflow not installed")
    from transform_lakehouse import dag

    assert dag.max_active_runs == 1


def _s3_storage(client: MagicMock) -> StorageConfig:
    return StorageConfig(bucket="test-bucket", local_root=None, s3_client=client)


def test_s3_delete_prefix_raises_on_partial_delete_errors() -> None:
    prefix = "lake/parquet/metadata/ingest_runs/"
    keys = [
        "lake/parquet/metadata/ingest_runs/a.parquet",
        "lake/parquet/metadata/ingest_runs/b.parquet",
    ]
    client = MagicMock()
    client.list_objects_v2.return_value = {
        "Contents": [{"Key": key} for key in keys],
        "IsTruncated": False,
    }
    client.delete_objects.return_value = {
        "Deleted": [{"Key": keys[0]}],
        "Errors": [
            {
                "Key": keys[1],
                "Code": "AccessDenied",
                "Message": "denied",
            }
        ],
    }
    storage = _s3_storage(client)

    with pytest.raises(LakehouseLoadError, match=prefix) as exc_info:
        storage.delete_prefix(prefix)

    message = str(exc_info.value)
    assert keys[1] in message
    assert "AccessDenied" in message or "denied" in message


def test_s3_delete_prefix_batches_and_succeeds_without_errors() -> None:
    prefix = "lake/parquet/metadata/ingest_runs/"
    keys = [f"lake/parquet/metadata/ingest_runs/key_{index:04d}.parquet" for index in range(1001)]
    client = MagicMock()
    client.list_objects_v2.return_value = {
        "Contents": [{"Key": key} for key in keys],
        "IsTruncated": False,
    }
    client.delete_objects.return_value = {"Deleted": [], "Errors": []}
    storage = _s3_storage(client)

    storage.delete_prefix(prefix)

    assert client.delete_objects.call_count == 2
    first_batch = client.delete_objects.call_args_list[0].kwargs["Delete"]["Objects"]
    second_batch = client.delete_objects.call_args_list[1].kwargs["Delete"]["Objects"]
    assert len(first_batch) == 1000
    assert len(second_batch) == 1


def test_s3_delete_prefix_empty_prefix_is_noop() -> None:
    client = MagicMock()
    client.list_objects_v2.return_value = {"IsTruncated": False}
    storage = _s3_storage(client)

    storage.delete_prefix("lake/parquet/metadata/ingest_runs/")

    client.delete_objects.assert_not_called()
