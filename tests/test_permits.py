from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock

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

    client = soda_ingest.SodaClient(session=session)
    records = list(client.fetch_records(PERMITS_CONFIG, datetime(2024, 1, 1)))

    assert len(records) == PERMITS_CONFIG.page_size + 1
    assert session.get.call_count == 2


def test_fetch_records_stops_on_empty_batch():
    session = MagicMock()
    session.get.return_value = _mock_response([])

    client = soda_ingest.SodaClient(session=session)
    records = list(client.fetch_records(PERMITS_CONFIG, datetime(2024, 1, 1)))

    assert records == []


def test_fetch_records_uses_strict_timestamp_watermark_and_stable_order():
    session = MagicMock()
    session.get.return_value = _mock_response([])

    client = soda_ingest.SodaClient(session=session)
    list(client.fetch_records(PERMITS_CONFIG, datetime(2024, 1, 1, 12, 30, 5, 123000)))

    params = session.get.call_args.kwargs["params"]
    assert params["$where"] == "`data_loaded_at` > '2024-01-01T12:30:05.123'"
    assert params["$order"] == "`data_loaded_at`, `permit_number`"


def test_fetch_records_can_include_initial_watermark_boundary():
    session = MagicMock()
    session.get.return_value = _mock_response([])

    client = soda_ingest.SodaClient(session=session)
    list(
        client.fetch_records(
            PERMITS_CONFIG,
            datetime(2013, 1, 1),
            include_since=True,
        )
    )

    params = session.get.call_args.kwargs["params"]
    assert params["$where"] == "`data_loaded_at` >= '2013-01-01T00:00:00.000'"


def test_count_records_parses_response():
    session = MagicMock()
    session.get.return_value = _mock_response([{"count": "42"}])

    client = soda_ingest.SodaClient(session=session)
    assert client.count_records(PERMITS_CONFIG, datetime(2024, 1, 1)) == 42


def test_run_returns_s3_path_and_watermark():
    since = datetime(2024, 1, 1)
    records = [
        {"permit_number": "1", "data_loaded_at": "2024-03-15T00:00:00.000"},
        {"permit_number": "2", "data_loaded_at": "2024-01-10T00:00:00.000"},
    ]

    client = MagicMock()
    client.fetch_records.return_value = iter(records)
    writer = MagicMock()
    writer.write_records.side_effect = lambda _config, _run_date, recs: soda_ingest.WriteResult(
        "s3://bucket/key",
        len(list(recs)),
        128,
    )

    result = soda_ingest.run(
        PERMITS_CONFIG,
        date(2024, 3, 20),
        since,
        client=client,
        writer=writer,
    )

    assert result.s3_path == "s3://bucket/key"
    assert result.max_watermark == datetime(2024, 3, 15)
    assert result.records_fetched == 2
    assert result.fetch_duration_seconds >= 0
    writer.write_records.assert_called_once()


def test_run_handles_empty_fetch_without_upload_or_watermark():
    client = MagicMock()
    client.fetch_records.return_value = iter([])
    writer = MagicMock()
    writer.write_records.side_effect = lambda _config, _run_date, recs: soda_ingest.WriteResult(
        None,
        len(list(recs)),
        0,
    )

    result = soda_ingest.run(
        PERMITS_CONFIG,
        date(2024, 3, 20),
        datetime(2024, 1, 1),
        client=client,
        writer=writer,
    )

    assert result.s3_path is None
    assert result.max_watermark is None
    assert result.records_fetched == 0
    writer.write_records.assert_called_once()


def test_ndjson_s3_writer_uploads_records_from_temp_file():
    class FakeS3:
        def upload_file(self, filename, bucket, key, ExtraArgs=None):
            self.filename = filename
            self.bucket = bucket
            self.key = key
            self.extra_args = ExtraArgs
            self.body = Path(filename).read_text()

    s3 = FakeS3()
    writer = soda_ingest.NdjsonS3Writer(bucket="test-bucket", s3_client=s3)

    result = writer.write_records(
        PERMITS_CONFIG,
        date(2024, 3, 20),
        [{"permit_number": "1"}, {"permit_number": "2"}],
    )

    assert result.s3_path == "s3://test-bucket/raw/permits/2024/03/20/permits.json"
    assert result.records_written == 2
    assert s3.bucket == "test-bucket"
    assert s3.key == "raw/permits/2024/03/20/permits.json"
    assert s3.body == '{"permit_number":"1"}\n{"permit_number":"2"}\n'
    assert s3.extra_args == {"ContentType": "application/x-ndjson"}


def test_ndjson_s3_writer_skips_empty_upload():
    s3 = MagicMock()
    writer = soda_ingest.NdjsonS3Writer(bucket="test-bucket", s3_client=s3)

    result = writer.write_records(PERMITS_CONFIG, date(2024, 3, 20), [])

    assert result == soda_ingest.WriteResult(None, 0, 0)
    s3.upload_file.assert_not_called()
