"""Smoke tests for the incidents ingest config + run wrapper."""
from datetime import date
from unittest.mock import MagicMock, patch

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
    since = date(2024, 1, 1)
    records = [
        {"row_id": "r1", "data_loaded_at": "2024-02-15T00:00:00.000"},
        {"row_id": "r2", "data_loaded_at": "2024-04-01T00:00:00.000"},
        {"row_id": "r3", "data_loaded_at": "2024-03-10T00:00:00.000"},
    ]

    with (
        patch.object(soda_ingest, "count_records", return_value=3),
        patch.object(soda_ingest, "fetch_records", return_value=iter(records)),
        patch.object(soda_ingest, "_write_s3", return_value="s3://bucket/incidents"),
    ):
        result = soda_ingest.run(INCIDENTS_CONFIG, date(2024, 4, 5), since)

    assert result.s3_path == "s3://bucket/incidents"
    assert result.max_watermark == date(2024, 4, 1)
    assert result.records_fetched == 3
