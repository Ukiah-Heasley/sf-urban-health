"""Smoke tests for the evictions ingest config and shared raw extractor."""
from datetime import datetime, timezone
from unittest.mock import MagicMock

from scripts import soda_ingest
from scripts.evictions import EVICTIONS_CONFIG


def _mock_response(payload):
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


def test_evictions_config_is_well_formed():
    assert EVICTIONS_CONFIG.name == "evictions"
    assert EVICTIONS_CONFIG.dataset_id == "5cei-gny5"
    assert EVICTIONS_CONFIG.date_field == "data_loaded_at"
    assert EVICTIONS_CONFIG.endpoint.startswith("https://data.sfgov.org/")


def test_evictions_extract_to_raw_returns_extract_result():
    window = soda_ingest.ExtractWindow(
        data_interval_start=datetime(2024, 3, 1, tzinfo=timezone.utc),
        data_interval_end=datetime(2024, 3, 2, tzinfo=timezone.utc),
    )
    records = [
        {"eviction_id": "e1", "data_loaded_at": "2024-02-01T00:00:00.000"},
        {"eviction_id": "e2", "data_loaded_at": "2024-03-01T00:00:00.000"},
    ]

    client = MagicMock()
    client.fetch_records.return_value = iter(records)
    writer = MagicMock()
    writer.write_records.side_effect = lambda _config, _window, recs: soda_ingest.WriteResult(
        "s3://bucket/evictions",
        "raw/evictions/records.ndjson",
        len(list(recs)),
        128,
    )

    result = soda_ingest.extract_to_raw(
        EVICTIONS_CONFIG,
        window,
        client=client,
        writer=writer,
    )

    assert result.raw_path == "s3://bucket/evictions"
    assert result.raw_key == "raw/evictions/records.ndjson"
    assert result.max_loaded_at == datetime(2024, 3, 1, tzinfo=timezone.utc)
    assert result.records_fetched == 2
