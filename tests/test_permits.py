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


def test_fetch_records_resumes_from_offset():
    session = MagicMock()
    session.get.return_value = _mock_response([])

    with patch.object(soda_ingest, "_session", return_value=session):
        list(soda_ingest.fetch_records(PERMITS_CONFIG, date(2024, 1, 1), resume_offset=2000))

    _, kwargs = session.get.call_args
    assert kwargs["params"]["$offset"] == 2000


def test_count_records_parses_response():
    session = MagicMock()
    session.get.return_value = _mock_response([{"count": "42"}])

    with patch.object(soda_ingest, "_session", return_value=session):
        assert soda_ingest.count_records(PERMITS_CONFIG, date(2024, 1, 1)) == 42
