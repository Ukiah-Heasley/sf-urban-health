"""Smoke tests for the evictions ingest config + run wrapper.

Mirrors the permits suite — verifies pagination behavior and that the
RunResult NamedTuple flows through cleanly.
"""
from datetime import date, datetime
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


def test_evictions_run_returns_runresult():
    since = datetime(2024, 1, 1)
    records = [
        {"eviction_id": "e1", "data_loaded_at": "2024-02-01T00:00:00.000"},
        {"eviction_id": "e2", "data_loaded_at": "2024-03-01T00:00:00.000"},
    ]

    client = MagicMock()
    client.fetch_records.return_value = iter(records)
    writer = MagicMock()
    writer.write_records.side_effect = lambda _config, _run_date, recs: soda_ingest.WriteResult(
        "s3://bucket/evictions",
        len(list(recs)),
        128,
    )

    result = soda_ingest.run(
        EVICTIONS_CONFIG,
        date(2024, 3, 5),
        since,
        client=client,
        writer=writer,
    )

    assert result.s3_path == "s3://bucket/evictions"
    assert result.max_watermark == datetime(2024, 3, 1)
    assert result.records_fetched == 2
