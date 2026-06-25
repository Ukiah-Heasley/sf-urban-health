#!/usr/bin/env python3
"""Local lakehouse promotion smoke test without AWS credentials."""
from __future__ import annotations

import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from scripts.lakehouse_load import StorageConfig, promote_raw_to_bronze
from scripts.lakehouse_metadata import (
    compact_lakehouse_metadata,
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

_DATASETS = {
    "permits": {
        "dataset_id": "i98e-djp9",
        "fixture": "permits.ndjson",
        "records": 2,
    },
    "evictions": {
        "dataset_id": "5cei-gny5",
        "fixture": "evictions.ndjson",
        "records": 2,
    },
    "incidents": {
        "dataset_id": "wg3w-h783",
        "fixture": "incidents.ndjson",
        "records": 2,
    },
}


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="lakehouse-smoke-"))
    try:
        storage = StorageConfig(bucket=None, local_root=root, s3_client=None)
        for dataset_name, config in _DATASETS.items():
            raw_key = (
                f"raw/{dataset_name}/"
                f"data_interval_start=20240315T060000Z/"
                f"data_interval_end=20240316T060000Z/"
                "records.ndjson"
            )
            destination = root / raw_key
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(_FIXTURES / config["fixture"], destination)
            raw_path = storage.uri_for_key(raw_key)
            extract_result = ExtractResult(
                raw_path=raw_path,
                raw_key=raw_key,
                records_fetched=config["records"],
                max_loaded_at=_COMPLETED_AT,
                bytes_written=destination.stat().st_size,
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
                    ingest_run_id=f"smoke-{dataset_name}",
                    dag_id=f"ingest_{dataset_name}",
                    dataset_name=dataset_name,
                ),
            )
            ingest_event = load_ingest_run_event_for_interval(
                storage,
                dataset_name=dataset_name,
                data_interval_start=_INTERVAL_START,
                data_interval_end=_INTERVAL_END,
            )
            result = promote_raw_to_bronze(
                dataset_name=dataset_name,
                dataset_id=config["dataset_id"],
                ingest_event=ingest_event,
                storage=storage,
            )
            assert result.bronze_path is not None
            assert result.records_promoted == config["records"]
            print(f"promoted {dataset_name}: {result.bronze_path}")
        compact_lakehouse_metadata(storage=storage)
        print(f"lakehouse smoke passed; artifacts in {root}")
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
