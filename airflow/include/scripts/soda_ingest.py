"""Generic SODA API ingestion: paginate a DataSF endpoint -> S3 raw NDJSON."""
from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass, field
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatasetConfig:
    name: str
    dataset_id: str
    date_field: str
    order_field: str
    epoch: date
    page_size: int = field(default=1000)

    @property
    def endpoint(self) -> str:
        return f"https://data.sfgov.org/resource/{self.dataset_id}.json"


@dataclass(frozen=True)
class ExtractWindow:
    """Half-open DataSF extraction interval: [effective_start, data_interval_end)."""

    data_interval_start: datetime
    data_interval_end: datetime
    lookback: timedelta = timedelta(0)

    def __post_init__(self) -> None:
        if self.data_interval_start is None or self.data_interval_end is None:
            raise ValueError("data_interval_start and data_interval_end must be set")

        start = _coerce_utc_datetime(self.data_interval_start)
        end = _coerce_utc_datetime(self.data_interval_end)

        if start >= end:
            raise ValueError("data_interval_start must be before data_interval_end")
        if self.lookback < timedelta(0):
            raise ValueError("lookback must be non-negative")

        object.__setattr__(self, "data_interval_start", start)
        object.__setattr__(self, "data_interval_end", end)

    @property
    def effective_start(self) -> datetime:
        return self.data_interval_start - self.lookback


@dataclass
class ExtractAccumulator:
    """Track metadata while records stream from SODA to the raw writer."""

    records_fetched: int = 0
    max_loaded_at: datetime | None = None

    def observe(self, config: DatasetConfig, record: dict) -> None:
        timestamp = _parse_soda_timestamp(record.get(config.date_field))
        if self.max_loaded_at is None or timestamp > self.max_loaded_at:
            self.max_loaded_at = timestamp
        self.records_fetched += 1

    def observe_records(
        self,
        config: DatasetConfig,
        records: Iterable[dict],
    ) -> Iterator[dict]:
        """Yield the same records onward while capturing extract metadata."""
        for record in records:
            self.observe(config, record)
            yield record


@dataclass(frozen=True)
class ExtractResult:
    raw_path: str | None
    raw_key: str | None
    records_fetched: int
    max_loaded_at: datetime | None
    bytes_written: int
    duration_seconds: float
    data_interval_start: datetime
    data_interval_end: datetime
    effective_start: datetime


@dataclass(frozen=True)
class WriteResult:
    raw_path: str | None
    raw_key: str | None
    records_written: int
    bytes_written: int


def _coerce_utc_datetime(value: date | datetime | str) -> datetime:
    """Normalize date-ish values to timezone-aware UTC datetimes."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, datetime_time.min, tzinfo=timezone.utc)
    elif isinstance(value, str):
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise TypeError(f"Unsupported datetime type: {type(value).__name__}")

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def _format_soda_timestamp(value: date | datetime | str) -> str:
    """Format a UTC timestamp for a SODA query literal."""
    return _coerce_utc_datetime(value).isoformat(timespec="milliseconds")


def _parse_soda_timestamp(value: object) -> datetime:
    if value is None:
        raise ValueError("SODA record is missing the configured timestamp field")
    return _coerce_utc_datetime(str(value))


def build_soda_session() -> requests.Session:
    """Build the side-effectful default HTTP session at the app boundary."""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    token = os.environ.get("DATASF_APP_TOKEN")
    if token:
        session.headers["X-App-Token"] = token
    return session


class SodaClient:
    """Small SODA API client for stable, timestamp-windowed pagination."""

    def __init__(self, session: requests.Session, timeout: int = 60) -> None:
        # Session is injected so tests can use a fake and callers control side effects.
        self._session = session
        self._timeout = timeout

    @staticmethod
    def where_clause(config: DatasetConfig, window: ExtractWindow) -> str:
        return (
            f"`{config.date_field}` >= "
            f"'{_format_soda_timestamp(window.effective_start)}' "
            f"AND `{config.date_field}` < "
            f"'{_format_soda_timestamp(window.data_interval_end)}'"
        )

    @staticmethod
    def order_clause(config: DatasetConfig) -> str:
        if config.order_field == config.date_field:
            return f"`{config.date_field}`"
        return f"`{config.date_field}`, `{config.order_field}`"

    def fetch_records(
        self,
        config: DatasetConfig,
        window: ExtractWindow,
    ) -> Iterator[dict]:
        """Yield records in the extraction window, paginated with a stable order."""
        offset = 0
        params = {
            "$where": self.where_clause(config, window),
            "$limit": config.page_size,
            "$offset": offset,
            "$order": self.order_clause(config),
        }

        while True:
            resp = self._session.get(config.endpoint, params=params, timeout=self._timeout)
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                return
            yield from batch
            offset += config.page_size
            if offset % 50_000 == 0:
                logger.info("fetch progress: %d records retrieved so far", offset)
            if len(batch) < config.page_size:
                return
            params["$offset"] = offset


def _raw_s3_key(config: DatasetConfig, window: ExtractWindow) -> str:
    """Build the deterministic raw object key for a dataset interval rerun."""
    window_start = window.data_interval_start.strftime("%Y%m%dT%H%M%SZ")
    window_end = window.data_interval_end.strftime("%Y%m%dT%H%M%SZ")
    return (
        f"raw/{config.name}/"
        f"data_interval_start={window_start}/"
        f"data_interval_end={window_end}/"
        "records.ndjson"
    )


class S3NdjsonWriter:
    """Stream records through a temp NDJSON file, then upload once to S3."""

    def __init__(self, bucket: str, s3_client) -> None:
        # The S3 client is injected so tests never accidentally touch AWS.
        self.bucket = bucket
        self._s3 = s3_client

    @classmethod
    def from_env(cls) -> S3NdjsonWriter:
        """Build the side-effectful default S3 writer at the app boundary."""
        import boto3

        bucket = os.environ.get("AWS_S3_BUCKET")
        if not bucket:
            raise RuntimeError("AWS_S3_BUCKET must be set")
        return cls(bucket=bucket, s3_client=boto3.client("s3"))

    def write_records(
        self,
        config: DatasetConfig,
        window: ExtractWindow,
        records: Iterable[dict],
    ) -> WriteResult:
        key = _raw_s3_key(config, window)
        tmp_path: Path | None = None
        records_written = 0
        bytes_written = 0

        try:
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as fp:
                tmp_path = Path(fp.name)
                for record in records:
                    line = json.dumps(record, separators=(",", ":"))
                    fp.write(line)
                    fp.write("\n")
                    records_written += 1
                    bytes_written += len(line) + 1

            if records_written == 0:
                return WriteResult(None, None, 0, 0)

            self._s3.upload_file(
                str(tmp_path),
                self.bucket,
                key,
                ExtraArgs={"ContentType": "application/x-ndjson"},
            )
            return WriteResult(
                f"s3://{self.bucket}/{key}",
                key,
                records_written,
                bytes_written,
            )
        finally:
            if tmp_path is not None:
                tmp_path.unlink(missing_ok=True)


def extract_to_raw(
    config: DatasetConfig,
    window: ExtractWindow,
    *,
    client: SodaClient,
    writer: S3NdjsonWriter,
) -> ExtractResult:

    t0 = time.monotonic()
    accumulator = ExtractAccumulator()

    # This wrapper is intentionally the only side-channel in the stream: the
    # writer still consumes records once, while we retain max_loaded_at metadata.
    records = accumulator.observe_records(config, client.fetch_records(config, window))

    write_result = writer.write_records(config, window, records)
    duration_seconds = time.monotonic() - t0

    if write_result.records_written != accumulator.records_fetched:
        raise RuntimeError(
            "record count mismatch while writing "
            f"{config.name}: saw {accumulator.records_fetched}, "
            f"wrote {write_result.records_written}"
        )

    logger.info(
        "completed %s: %d records, max loaded_at %s, wrote %d bytes to %s in %.1fs",
        config.name,
        accumulator.records_fetched,
        accumulator.max_loaded_at,
        write_result.bytes_written,
        write_result.raw_path,
        duration_seconds,
    )
    return ExtractResult(
        raw_path=write_result.raw_path,
        raw_key=write_result.raw_key,
        records_fetched=accumulator.records_fetched,
        max_loaded_at=accumulator.max_loaded_at,
        bytes_written=write_result.bytes_written,
        duration_seconds=round(duration_seconds, 2),
        data_interval_start=window.data_interval_start,
        data_interval_end=window.data_interval_end,
        effective_start=window.effective_start,
    )


def cli(config: DatasetConfig) -> None:
    """Command-line entry point for a single interval extract."""
    import argparse
    import logging

    from dotenv import load_dotenv

    parser = argparse.ArgumentParser(
        description=f"Fetch SF {config.name} from the DataSF SODA API."
    )
    parser.add_argument(
        "--window-start",
        type=_coerce_utc_datetime,
        default=datetime.combine(config.epoch, datetime_time.min, tzinfo=timezone.utc),
        help="Inclusive extraction window start. Defaults to the dataset epoch.",
    )
    parser.add_argument(
        "--window-end",
        type=_coerce_utc_datetime,
        default=datetime.now(timezone.utc),
        help="Exclusive extraction window end. Defaults to now.",
    )
    parser.add_argument(
        "--lookback-hours",
        type=float,
        default=0.0,
        help="Optional lookback to subtract from window start.",
    )
    args = parser.parse_args()

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    window = ExtractWindow(
        data_interval_start=args.window_start,
        data_interval_end=args.window_end,
        lookback=timedelta(hours=args.lookback_hours),
    )
    client = SodaClient(session=build_soda_session())
    writer = S3NdjsonWriter.from_env()
    extract_to_raw(config, window, client=client, writer=writer)
