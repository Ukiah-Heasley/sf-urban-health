"""Smoke tests for the incidents ingest config + run wrapper."""
from datetime import date, datetime
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


def test_incidents_run_returns_runresult():
    since = datetime(2024, 1, 1)
    records = [
        {"row_id": "r1", "data_loaded_at": "2024-02-15T00:00:00.000"},
        {"row_id": "r2", "data_loaded_at": "2024-04-01T00:00:00.000"},
        {"row_id": "r3", "data_loaded_at": "2024-03-10T00:00:00.000"},
    ]

    client = MagicMock()
    client.fetch_records.return_value = iter(records)
    writer = MagicMock()
    writer.write_records.side_effect = lambda _config, _run_date, recs: soda_ingest.WriteResult(
        "s3://bucket/incidents",
        len(list(recs)),
        128,
    )

    result = soda_ingest.run(
        INCIDENTS_CONFIG,
        date(2024, 4, 5),
        since,
        client=client,
        writer=writer,
    )

    assert result.s3_path == "s3://bucket/incidents"
    assert result.max_watermark == datetime(2024, 4, 1)
    assert result.records_fetched == 3
