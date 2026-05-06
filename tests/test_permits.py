from datetime import date
from unittest.mock import MagicMock, patch

from scripts import soda_ingest
from scripts.permits import PERMITS_CONFIG


def _mock_response(payload):
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


def test_fetch_records_paginates_until_short_batch():
    full_page = [{"permit_number": str(i)} for i in range(PERMITS_CONFIG.page_size)]
    short_page = [{"permit_number": "x"}]

    session = MagicMock()
    session.get.side_effect = [_mock_response(full_page), _mock_response(short_page)]

    with patch.object(soda_ingest, "_session", return_value=session):
        records = list(soda_ingest.fetch_records(PERMITS_CONFIG, date(2024, 1, 1)))

    assert len(records) == PERMITS_CONFIG.page_size + 1
    assert session.get.call_count == 2


def test_fetch_records_stops_on_empty_batch():
    session = MagicMock()
    session.get.return_value = _mock_response([])

    with patch.object(soda_ingest, "_session", return_value=session):
        records = list(soda_ingest.fetch_records(PERMITS_CONFIG, date(2024, 1, 1)))

    assert records == []


def test_count_records_parses_response():
    session = MagicMock()
    session.get.return_value = _mock_response([{"count": "42"}])

    with patch.object(soda_ingest, "_session", return_value=session):
        assert soda_ingest.count_records(PERMITS_CONFIG, date(2024, 1, 1)) == 42


def test_run_returns_s3_path_and_watermark():
    since = date(2024, 1, 1)
    records = [
        {"permit_number": "1", "data_loaded_at": "2024-03-15T00:00:00.000"},
        {"permit_number": "2", "data_loaded_at": "2024-01-10T00:00:00.000"},
    ]

    with (
        patch.object(soda_ingest, "count_records", return_value=2),
        patch.object(soda_ingest, "fetch_records", return_value=iter(records)),
        patch.object(soda_ingest, "_write_s3", return_value="s3://bucket/key") as mock_write,
    ):
        s3_path, max_wm = soda_ingest.run(PERMITS_CONFIG, date(2024, 3, 20), since)

    assert s3_path == "s3://bucket/key"
    assert max_wm == date(2024, 3, 15)
    mock_write.assert_called_once()
