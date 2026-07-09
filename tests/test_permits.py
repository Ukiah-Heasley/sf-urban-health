from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scripts import soda_ingest
from scripts.permits import PERMITS_CONFIG


def _mock_response(payload):
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


def _window(
    start: datetime = datetime(2024, 1, 1, tzinfo=timezone.utc),
    end: datetime = datetime(2024, 1, 2, tzinfo=timezone.utc),
    lookback: timedelta = timedelta(0),
) -> soda_ingest.ExtractWindow:
    return soda_ingest.ExtractWindow(
        data_interval_start=start,
        data_interval_end=end,
        lookback=lookback,
    )


def test_extract_window_normalizes_to_utc_and_applies_lookback():
    window = _window(
        datetime(2024, 1, 2, 8, tzinfo=timezone.utc),
        datetime(2024, 1, 3, 8, tzinfo=timezone.utc),
        timedelta(hours=2),
    )

    assert window.data_interval_start == datetime(2024, 1, 2, 8, tzinfo=timezone.utc)
    assert window.data_interval_end == datetime(2024, 1, 3, 8, tzinfo=timezone.utc)
    assert window.effective_start == datetime(2024, 1, 2, 6, tzinfo=timezone.utc)


def test_fetch_records_paginates_until_short_batch():
    full_page = [{"permit_number": str(i)} for i in range(PERMITS_CONFIG.page_size)]
    short_page = [{"permit_number": "x"}]

    session = MagicMock()
    session.get.side_effect = [_mock_response(full_page), _mock_response(short_page)]

    client = soda_ingest.SodaClient(session=session)
    records = list(client.fetch_records(PERMITS_CONFIG, _window()))

    assert len(records) == PERMITS_CONFIG.page_size + 1
    assert session.get.call_count == 2


def test_fetch_records_stops_on_empty_batch():
    session = MagicMock()
    session.get.return_value = _mock_response([])

    client = soda_ingest.SodaClient(session=session)
    records = list(client.fetch_records(PERMITS_CONFIG, _window()))

    assert records == []


def test_fetch_records_uses_half_open_interval_and_stable_order():
    session = MagicMock()
    session.get.return_value = _mock_response([])

    client = soda_ingest.SodaClient(session=session)
    window = _window(
        datetime(2024, 1, 1, 12, 30, 5, 123000, tzinfo=timezone.utc),
        datetime(2024, 1, 2, tzinfo=timezone.utc),
    )
    list(client.fetch_records(PERMITS_CONFIG, window))

    params = session.get.call_args.kwargs["params"]
    assert params["$where"] == (
        "`data_loaded_at` >= '2024-01-01T12:30:05.123' "
        "AND `data_loaded_at` < '2024-01-02T00:00:00.000'"
    )
    assert params["$order"] == "`data_loaded_at`, `permit_number`"


def test_extract_to_raw_returns_s3_path_and_observed_max_loaded_at():
    window = _window()
    records = [
        {"permit_number": "1", "data_loaded_at": "2024-03-15T00:00:00.000"},
        {"permit_number": "2", "data_loaded_at": "2024-01-10T00:00:00.000"},
    ]

    client = MagicMock()
    client.fetch_records.return_value = iter(records)
    writer = MagicMock()
    writer.write_records.side_effect = (
        lambda _config, _window, recs: soda_ingest.WriteResult(
            "s3://bucket/key",
            "raw/permits/key.ndjson",
            len(list(recs)),
            128,
        )
    )

    result = soda_ingest.extract_to_raw(
        PERMITS_CONFIG,
        window,
        client=client,
        writer=writer,
    )

    assert result.raw_path == "s3://bucket/key"
    assert result.raw_key == "raw/permits/key.ndjson"
    assert result.max_loaded_at == datetime(2024, 3, 15, tzinfo=timezone.utc)
    assert result.records_fetched == 2
    assert result.duration_seconds >= 0
    assert result.data_interval_start == window.data_interval_start
    assert result.data_interval_end == window.data_interval_end
    writer.write_records.assert_called_once()


def test_extract_to_raw_handles_empty_fetch_without_upload_or_loaded_at():
    window = _window()
    client = MagicMock()
    client.fetch_records.return_value = iter([])
    writer = MagicMock()
    writer.write_records.side_effect = (
        lambda _config, _window, recs: soda_ingest.WriteResult(
            None,
            None,
            len(list(recs)),
            0,
        )
    )

    result = soda_ingest.extract_to_raw(
        PERMITS_CONFIG,
        window,
        client=client,
        writer=writer,
    )

    assert result.raw_path is None
    assert result.raw_key is None
    assert result.max_loaded_at is None
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
    writer = soda_ingest.S3NdjsonWriter(bucket="test-bucket", s3_client=s3)
    window = _window(
        datetime(2024, 3, 20, tzinfo=timezone.utc),
        datetime(2024, 3, 21, tzinfo=timezone.utc),
    )

    result = writer.write_records(
        PERMITS_CONFIG,
        window,
        [{"permit_number": "1"}, {"permit_number": "2"}],
    )

    expected_key = (
        "raw/permits/"
        "data_interval_start=20240320T000000Z/"
        "data_interval_end=20240321T000000Z/"
        "records.ndjson"
    )
    assert result.raw_path == f"s3://test-bucket/{expected_key}"
    assert result.raw_key == expected_key
    assert result.records_written == 2
    assert s3.bucket == "test-bucket"
    assert s3.key == expected_key
    assert s3.body == '{"permit_number":"1"}\n{"permit_number":"2"}\n'
    assert s3.extra_args == {"ContentType": "application/x-ndjson"}


def test_ndjson_s3_writer_skips_empty_upload():
    s3 = MagicMock()
    writer = soda_ingest.S3NdjsonWriter(bucket="test-bucket", s3_client=s3)

    result = writer.write_records(PERMITS_CONFIG, _window(), [])

    assert result == soda_ingest.WriteResult(None, None, 0, 0)
    s3.upload_file.assert_not_called()


def test_s3_ndjson_writer_from_env_requires_bucket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AWS_S3_BUCKET", raising=False)

    with pytest.raises(RuntimeError, match="AWS_S3_BUCKET must be set"):
        soda_ingest.S3NdjsonWriter.from_env()


def test_s3_ndjson_writer_from_env_uses_default_s3_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_S3_BUCKET", "my-bucket")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_S3_ENDPOINT_URL", raising=False)

    with patch("boto3.client") as mock_client:
        mock_client.return_value = MagicMock()
        writer = soda_ingest.S3NdjsonWriter.from_env()

    mock_client.assert_called_once_with("s3")
    assert writer.bucket == "my-bucket"


@pytest.mark.parametrize(
    ("endpoint_env", "endpoint_url"),
    [
        ("AWS_ENDPOINT_URL", "http://host.docker.internal:9000"),
        ("AWS_S3_ENDPOINT_URL", "http://localhost:9000"),
    ],
)
def test_s3_ndjson_writer_from_env_honors_custom_endpoint(
    monkeypatch: pytest.MonkeyPatch,
    endpoint_env: str,
    endpoint_url: str,
) -> None:
    monkeypatch.setenv("AWS_S3_BUCKET", "lakehouse")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_S3_ENDPOINT_URL", raising=False)
    monkeypatch.setenv(endpoint_env, endpoint_url)

    with patch("boto3.client") as mock_client:
        mock_client.return_value = MagicMock()
        writer = soda_ingest.S3NdjsonWriter.from_env()

    mock_client.assert_called_once_with("s3", endpoint_url=endpoint_url)
    assert writer.bucket == "lakehouse"


def test_s3_ndjson_writer_from_env_prefers_aws_endpoint_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_S3_BUCKET", "lakehouse")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://primary:9000")
    monkeypatch.setenv("AWS_S3_ENDPOINT_URL", "http://fallback:9000")

    with patch("boto3.client") as mock_client:
        mock_client.return_value = MagicMock()
        soda_ingest.S3NdjsonWriter.from_env()

    mock_client.assert_called_once_with("s3", endpoint_url="http://primary:9000")
