"""Smoke tests for the incidents ingest config and shared raw extractor."""
from datetime import datetime, timezone
from unittest.mock import MagicMock

from scripts import soda_ingest
from scripts.incident_reports import INCIDENTS_CONFIG


def _mock_response(payload):
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


def test_incidents_config_is_well_formed():
    assert INCIDENTS_CONFIG.name == "incidents"
    assert INCIDENTS_CONFIG.dataset_id == "wg3w-h783"
    assert INCIDENTS_CONFIG.date_field == "data_loaded_at"
    assert INCIDENTS_CONFIG.endpoint.startswith("https://data.sfgov.org/")


def test_incidents_extract_to_raw_returns_extract_result():
    window = soda_ingest.ExtractWindow(
        data_interval_start=datetime(2024, 4, 1, tzinfo=timezone.utc),
        data_interval_end=datetime(2024, 4, 2, tzinfo=timezone.utc),
    )
    records = [
        {"row_id": "r1", "data_loaded_at": "2024-02-15T00:00:00.000"},
        {"row_id": "r2", "data_loaded_at": "2024-04-01T00:00:00.000"},
        {"row_id": "r3", "data_loaded_at": "2024-03-10T00:00:00.000"},
    ]

    client = MagicMock()
    client.fetch_records.return_value = iter(records)
    writer = MagicMock()
    writer.write_records.side_effect = lambda _config, _window, recs: soda_ingest.WriteResult(
        "s3://bucket/incidents",
        "raw/incidents/records.ndjson",
        len(list(recs)),
        128,
    )

    result = soda_ingest.extract_to_raw(
        INCIDENTS_CONFIG,
        window,
        client=client,
        writer=writer,
    )

    assert result.raw_path == "s3://bucket/incidents"
    assert result.raw_key == "raw/incidents/records.ndjson"
    assert result.max_loaded_at == datetime(2024, 4, 1, tzinfo=timezone.utc)
    assert result.records_fetched == 3
