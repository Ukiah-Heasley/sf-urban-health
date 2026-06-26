#!/usr/bin/env python3
"""Reset the local MinIO lakehouse sandbox and seed permits fixture data for dbt."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from scripts.time_utils import coerce_utc_datetime

from scripts.lakehouse_load import promote_raw_to_bronze
from scripts.lakehouse_local import (
    load_lakehouse_env,
    refresh_spark_catalog_after_bucket_reset,
    require_local_lakehouse_stack,
    reset_lakehouse_bucket,
)
from scripts.lakehouse_metadata import (
    ingest_run_event_from_extract,
    load_ingest_run_event_for_interval,
    write_ingest_run_event,
)
from scripts.soda_ingest import ExtractResult

_FIXTURES = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "lakehouse"
_INTERVAL_START = datetime(2024, 3, 15, 6, 0, tzinfo=timezone.utc)
_INTERVAL_END = datetime(2024, 3, 16, 6, 0, tzinfo=timezone.utc)
_STARTED_AT = datetime(2024, 3, 16, 6, 0, tzinfo=timezone.utc)
_COMPLETED_AT = datetime(2024, 3, 16, 6, 4, tzinfo=timezone.utc)
_PERMITS_DATASET_ID = "i98e-djp9"
_RAW_KEY = (
    "raw/permits/"
    "data_interval_start=20240315T060000Z/"
    "data_interval_end=20240316T060000Z/"
    "records.ndjson"
)


def _count_fixture_records(path: Path) -> int:
    return sum(1 for line in path.read_text().splitlines() if line.strip())


def _max_loaded_at_from_fixture(path: Path) -> datetime:
    max_loaded_at: datetime | None = None
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        payload = json.loads(stripped)
        loaded_at = coerce_utc_datetime(str(payload["data_loaded_at"]))
        if max_loaded_at is None or loaded_at > max_loaded_at:
            max_loaded_at = loaded_at
    if max_loaded_at is None:
        raise ValueError(f"fixture has no records: {path}")
    return max_loaded_at


def prepare_permits_fixture() -> None:
    load_lakehouse_env()
    storage = require_local_lakehouse_stack()
    assert storage.bucket is not None

    deleted = reset_lakehouse_bucket(storage)
    print(f"reset local lakehouse bucket {storage.bucket!r}; deleted {deleted} object(s)")

    fixture_path = _FIXTURES / "permits.ndjson"
    records_fetched = _count_fixture_records(fixture_path)
    max_loaded_at = _max_loaded_at_from_fixture(fixture_path)
    payload = fixture_path.read_bytes()
    bytes_written = storage.write_bytes(_RAW_KEY, payload)
    raw_path = storage.uri_for_key(_RAW_KEY)
    print(f"seeded raw permits fixture at {raw_path} ({records_fetched} record(s))")

    extract_result = ExtractResult(
        raw_path=raw_path,
        raw_key=_RAW_KEY,
        records_fetched=records_fetched,
        max_loaded_at=max_loaded_at,
        bytes_written=bytes_written,
        duration_seconds=1.5,
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
        effective_start=_INTERVAL_START,
        started_at=_STARTED_AT,
        completed_at=_COMPLETED_AT,
    )
    write_ingest_run_event(
        storage,
        ingest_run_event_from_extract(
            extract_result=extract_result,
            ingest_run_id="fixture-permits",
            dag_id="ingest_permits",
            dataset_name="permits",
        ),
    )
    ingest_event = load_ingest_run_event_for_interval(
        storage,
        dataset_name="permits",
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
    )
    result = promote_raw_to_bronze(
        dataset_name="permits",
        dataset_id=_PERMITS_DATASET_ID,
        ingest_event=ingest_event,
        storage=storage,
    )
    assert result.bronze_path is not None
    assert result.records_promoted == records_fetched
    print(f"promoted bronze permits to {result.bronze_path}")

    refresh_spark_catalog_after_bucket_reset()
    print("refreshed Spark catalog after local bucket reset")


def main() -> None:
    prepare_permits_fixture()
    print("local permits fixture preparation complete")


if __name__ == "__main__":
    main()
