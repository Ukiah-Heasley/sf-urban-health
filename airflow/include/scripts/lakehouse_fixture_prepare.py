#!/usr/bin/env python3
"""Reset the local MinIO lakehouse sandbox and seed fixture data for dbt."""
from __future__ import annotations

import json
from dataclasses import dataclass
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


@dataclass(frozen=True)
class FixtureDataset:
    dataset_name: str
    fixture_filename: str
    dataset_id: str
    dag_id: str

    @property
    def raw_key(self) -> str:
        return (
            f"raw/{self.dataset_name}/"
            "data_interval_start=20240315T060000Z/"
            "data_interval_end=20240316T060000Z/"
            "records.ndjson"
        )


FIXTURE_DATASETS: tuple[FixtureDataset, ...] = (
    FixtureDataset(
        dataset_name="permits",
        fixture_filename="permits.ndjson",
        dataset_id="i98e-djp9",
        dag_id="ingest_permits",
    ),
    FixtureDataset(
        dataset_name="evictions",
        fixture_filename="evictions.ndjson",
        dataset_id="5cei-gny5",
        dag_id="ingest_evictions",
    ),
    FixtureDataset(
        dataset_name="incidents",
        fixture_filename="incidents.ndjson",
        dataset_id="wg3w-h783",
        dag_id="ingest_incidents",
    ),
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


def _seed_and_promote_dataset(
    *,
    dataset: FixtureDataset,
    storage,
) -> None:
    fixture_path = _FIXTURES / dataset.fixture_filename
    records_fetched = _count_fixture_records(fixture_path)
    max_loaded_at = _max_loaded_at_from_fixture(fixture_path)
    payload = fixture_path.read_bytes()
    bytes_written = storage.write_bytes(dataset.raw_key, payload)
    raw_path = storage.uri_for_key(dataset.raw_key)
    print(
        f"seeded raw {dataset.dataset_name} fixture at {raw_path} "
        f"({records_fetched} record(s))"
    )

    extract_result = ExtractResult(
        raw_path=raw_path,
        raw_key=dataset.raw_key,
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
            ingest_run_id=f"fixture-{dataset.dataset_name}",
            dag_id=dataset.dag_id,
            dataset_name=dataset.dataset_name,
        ),
    )
    ingest_event = load_ingest_run_event_for_interval(
        storage,
        dataset_name=dataset.dataset_name,
        data_interval_start=_INTERVAL_START,
        data_interval_end=_INTERVAL_END,
    )
    result = promote_raw_to_bronze(
        dataset_name=dataset.dataset_name,
        dataset_id=dataset.dataset_id,
        ingest_event=ingest_event,
        storage=storage,
    )
    assert result.bronze_path is not None
    assert result.records_promoted == records_fetched, (
        f"{dataset.dataset_name}: promoted {result.records_promoted} rows, "
        f"expected {records_fetched}"
    )
    print(f"promoted bronze {dataset.dataset_name} to {result.bronze_path}")


def prepare_lakehouse_fixtures() -> None:
    load_lakehouse_env()
    storage = require_local_lakehouse_stack()
    assert storage.bucket is not None

    deleted = reset_lakehouse_bucket(storage)
    print(f"reset local lakehouse bucket {storage.bucket!r}; deleted {deleted} object(s)")

    for dataset in FIXTURE_DATASETS:
        _seed_and_promote_dataset(dataset=dataset, storage=storage)

    refresh_spark_catalog_after_bucket_reset()
    print("refreshed Spark catalog after local bucket reset")


def prepare_permits_fixture() -> None:
    """Compatibility alias for the all-dataset fixture preparation flow."""
    prepare_lakehouse_fixtures()


def main() -> None:
    prepare_lakehouse_fixtures()
    print("local lakehouse fixture preparation complete")


if __name__ == "__main__":
    main()
