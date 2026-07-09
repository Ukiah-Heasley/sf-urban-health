"""Generic DataSF SODA API extraction into immutable raw NDJSON on S3.

This module owns the source-system side of ingestion only:

1. Build a half-open extraction window from Airflow or CLI input.
2. Page through a DataSF SODA endpoint in a stable order.
3. Stream each record into one newline-delimited JSON raw object in S3.
4. Return run metadata that the DAG can expose through XCom.

It deliberately stops at the raw layer. Bronze Parquet promotion and dbt
transform work belong downstream of the ingest asset.
"""
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

from scripts.time_utils import coerce_utc_datetime

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatasetConfig:
    """Static metadata for one DataSF dataset.

    Inputs:
    - ``name`` is this project's short dataset slug and raw S3 namespace.
    - ``dataset_id`` is the Socrata resource identifier in the DataSF URL.
    - ``date_field`` is the source timestamp used for interval filtering.
    - ``order_field`` is a stable tie-breaker used while paginating.
    - ``epoch`` is the earliest sensible default for CLI backfills.
    - ``page_size`` controls how many records each SODA request asks for.

    Output:
    - ``endpoint`` derives the HTTPS JSON API URL used by ``SodaClient``.
    """

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
    """Half-open source extraction interval for one Airflow run.

    Inputs:
    - ``data_interval_start`` is the scheduled interval's inclusive start.
    - ``data_interval_end`` is the scheduled interval's exclusive end.
    - ``lookback`` optionally widens the lower bound for late-arriving data.

    Outputs:
    - ``effective_start`` is the actual inclusive lower bound sent to SODA.
    - Stored datetimes are normalized to timezone-aware UTC so comparisons and
      query formatting use one convention.
    """

    data_interval_start: datetime
    data_interval_end: datetime
    lookback: timedelta = timedelta(0)

    def __post_init__(self) -> None:
        if self.data_interval_start is None or self.data_interval_end is None:
            raise ValueError("data_interval_start and data_interval_end must be set")

        start = coerce_utc_datetime(self.data_interval_start)
        end = coerce_utc_datetime(self.data_interval_end)

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
    """Collect extract metadata without materializing the whole response.

    Input:
    - Receives one source record at a time from ``SodaClient.fetch_records``.

    Outputs:
    - ``records_fetched`` counts records observed in the stream.
    - ``max_loaded_at`` records the largest source timestamp observed. In this
      interval design it is descriptive metadata, not the state that controls
      the next run.
    """

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
        """Yield input records unchanged while updating extract metadata."""
        for record in records:
            self.observe(config, record)
            yield record


@dataclass(frozen=True)
class ExtractResult:
    """Summary returned by ``extract_to_raw`` for DAG XCom and logs.

    Inputs are the extraction config, Airflow interval, SODA response stream,
    and raw writer. Outputs include the raw S3 location when records were
    written, row and byte counts, timing, interval bounds, and observed source
    freshness metadata.
    """

    raw_path: str | None
    raw_key: str | None
    records_fetched: int
    max_loaded_at: datetime | None
    bytes_written: int
    duration_seconds: float
    data_interval_start: datetime
    data_interval_end: datetime
    effective_start: datetime
    started_at: datetime
    completed_at: datetime


@dataclass(frozen=True)
class WriteResult:
    """Summary returned by ``S3NdjsonWriter.write_records``.

    Outputs are ``None`` paths for empty extracts, otherwise the S3 URI, object
    key, record count, and byte count for the uploaded NDJSON object.
    """

    raw_path: str | None
    raw_key: str | None
    records_written: int
    bytes_written: int


def _format_soda_timestamp(value: date | datetime | str) -> str:
    """Format a UTC timestamp for a SODA calendar-date query literal."""
    return coerce_utc_datetime(value).replace(tzinfo=None).isoformat(timespec="milliseconds")


def _parse_soda_timestamp(value: object) -> datetime:
    """Parse the configured source timestamp from one SODA record.

    Input is the raw field value pulled from a JSON record. Output is a
    timezone-aware UTC ``datetime``. Missing timestamps fail the extract because
    the run metadata would otherwise be misleading.
    """

    if value is None:
        raise ValueError("SODA record is missing the configured timestamp field")
    return coerce_utc_datetime(str(value))


def build_soda_session() -> requests.Session:
    """Build the default HTTP session at the Airflow or CLI boundary.

    Inputs come from environment variables: ``DATASF_APP_TOKEN`` is optional
    and, when present, is attached as the SODA app token header.

    Output is a configured ``requests.Session`` with retries for transient API
    failures. Core extraction receives this session through ``SodaClient`` so
    tests can inject a fake session instead.
    """

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
    """SODA API client for stable, interval-bounded pagination.

    Input:
    - A caller-provided ``requests.Session`` plus a request timeout.

    Output:
    - ``fetch_records`` yields dictionaries from the DataSF JSON API one record
      at a time, while keeping only one API page in memory.
    """

    def __init__(self, session: requests.Session, timeout: int = 60) -> None:
        self._session = session
        self._timeout = timeout

    @staticmethod
    def where_clause(config: DatasetConfig, window: ExtractWindow) -> str:
        """
        Build the SODA $where SQL predicate for the extraction interval.
        Input is dataset metadata plus the Airflow window. Output is a
        half-open timestamp predicate:
        date_field >= effective_start AND date_field < data_interval_end.
        """

        return (
            f"`{config.date_field}` >= "
            f"'{_format_soda_timestamp(window.effective_start)}' "
            f"AND `{config.date_field}` < "
            f"'{_format_soda_timestamp(window.data_interval_end)}'"
        )

    @staticmethod
    def order_clause(config: DatasetConfig) -> str:
        """Build the SODA ``$order`` clause used for deterministic pagination.

        The timestamp field comes first so interval scans are stable. The
        dataset-specific tie-breaker follows unless it is the same field.
        """

        if config.order_field == config.date_field:
            return f"`{config.date_field}`"
        return f"`{config.date_field}`, `{config.order_field}`"

    def fetch_records(
        self,
        config: DatasetConfig,
        window: ExtractWindow,
    ) -> Iterator[dict]:
        """Yield all source records inside ``window``.

        Inputs:
        - ``config`` supplies the DataSF endpoint, page size, timestamp field,
          and ordering fields.
        - ``window`` supplies the inclusive lower bound and exclusive upper
          bound.

        Output:
        - An iterator of record dictionaries. The caller can stream this
          directly into a writer without loading the full extract into memory.
        """

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


def _s3_endpoint_url() -> str | None:
    """Return a custom S3 endpoint when configured for MinIO or other S3-compatible stores."""
    return os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("AWS_S3_ENDPOINT_URL") or None


def _build_s3_client():
    """Build a boto3 S3 client from environment credentials and optional endpoint."""
    import boto3

    kwargs: dict[str, str] = {}
    endpoint = _s3_endpoint_url()
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("s3", **kwargs)


def _raw_s3_key(config: DatasetConfig, window: ExtractWindow) -> str:
    """Build the raw S3 object key for one dataset interval.

    Inputs are the dataset slug and normalized interval bounds. Output is a
    deterministic key, so a retry of the same Airflow interval overwrites the
    same object instead of creating duplicate raw files.
    """

    window_start = window.data_interval_start.strftime("%Y%m%dT%H%M%SZ")
    window_end = window.data_interval_end.strftime("%Y%m%dT%H%M%SZ")
    return (
        f"raw/{config.name}/"
        f"data_interval_start={window_start}/"
        f"data_interval_end={window_end}/"
        "records.ndjson"
    )


class S3NdjsonWriter:
    """Write a record stream as one raw newline-delimited JSON object in S3.

    Inputs:
    - ``bucket`` is the destination raw bucket.
    - ``s3_client`` is an injected boto3-compatible client.

    Output:
    - ``write_records`` returns the raw S3 path, object key, record count, and
      byte count. Empty extracts return ``None`` paths and do not upload.
    """

    def __init__(self, bucket: str, s3_client) -> None:
        self.bucket = bucket
        self._s3 = s3_client

    @classmethod
    def from_env(cls) -> S3NdjsonWriter:
        """Build the default S3 writer at the Airflow or CLI boundary.

        Input comes from ``AWS_S3_BUCKET``, optional ``AWS_ENDPOINT_URL`` or
        ``AWS_S3_ENDPOINT_URL``, and boto3's normal credential lookup. Output is
        a writer with a real S3 client. Tests should construct
        ``S3NdjsonWriter`` directly with a fake client.
        """

        bucket = os.environ.get("AWS_S3_BUCKET")
        if not bucket:
            raise RuntimeError("AWS_S3_BUCKET must be set")
        return cls(bucket=bucket, s3_client=_build_s3_client())

    def write_records(
        self,
        config: DatasetConfig,
        window: ExtractWindow,
        records: Iterable[dict],
    ) -> WriteResult:
        """Stream records to a temp file, then upload one raw NDJSON object.

        Inputs:
        - ``config`` and ``window`` determine the raw object key.
        - ``records`` is consumed exactly once and can be a generator.

        Output:
        - ``WriteResult`` with the uploaded S3 path and byte/record counts, or
          ``None`` paths when the input stream is empty.
        """

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
    """Extract one DataSF interval and land it in the S3 raw layer.

    Inputs:
    - ``config`` identifies the source dataset and timestamp/order fields.
    - ``window`` defines the half-open interval to request from DataSF.
    - ``client`` reads records from SODA.
    - ``writer`` uploads the streamed records to raw S3.

    Output:
    - ``ExtractResult`` for Airflow XCom, logging, and downstream metadata.

    The pipeline is intentionally streaming:
    ``SodaClient.fetch_records`` -> ``ExtractAccumulator.observe_records`` ->
    ``S3NdjsonWriter.write_records``. Only one API page and one temp file are
    held at a time.
    """

    t0 = time.monotonic()
    started_at = datetime.now(timezone.utc)
    accumulator = ExtractAccumulator()

    records = accumulator.observe_records(config, client.fetch_records(config, window))

    write_result = writer.write_records(config, window, records)
    duration_seconds = time.monotonic() - t0
    completed_at = datetime.now(timezone.utc)

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
        started_at=started_at,
        completed_at=completed_at,
    )


def cli(config: DatasetConfig) -> None:
    """Command-line entry point for manually extracting one dataset interval.

    Inputs come from flags:
    - ``--window-start`` inclusive lower bound, defaulting to the dataset epoch.
    - ``--window-end`` exclusive upper bound, defaulting to now.
    - ``--lookback-hours`` optional widening of the lower bound.

    Output is the same raw S3 object and logs that the Airflow task would
    produce. This is useful for local backfills and smoke tests.
    """

    import argparse
    import logging

    from dotenv import load_dotenv

    parser = argparse.ArgumentParser(
        description=f"Fetch SF {config.name} from the DataSF SODA API."
    )
    parser.add_argument(
        "--window-start",
        type=coerce_utc_datetime,
        default=datetime.combine(config.epoch, datetime_time.min, tzinfo=timezone.utc),
        help="Inclusive extraction window start. Defaults to the dataset epoch.",
    )
    parser.add_argument(
        "--window-end",
        type=coerce_utc_datetime,
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
