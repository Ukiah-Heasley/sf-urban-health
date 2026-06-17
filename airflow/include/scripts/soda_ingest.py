"""Generic SODA API ingestion: paginate a DataSF endpoint -> S3 raw JSON."""
from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass, field
from datetime import date, datetime, time as datetime_time, timezone
from pathlib import Path
from typing import Iterable, Iterator, NamedTuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass
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


class RunResult(NamedTuple):
    s3_path: str | None
    max_watermark: datetime | None
    records_fetched: int
    fetch_duration_seconds: float


class WriteResult(NamedTuple):
    s3_path: str | None
    records_written: int
    bytes_written: int


def _coerce_datetime(value: date | datetime | str) -> datetime:
    """Convert date-ish values into the naive timestamps DataSF/Snowflake use."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, datetime_time.min)
    elif isinstance(value, str):
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise TypeError(f"Unsupported watermark type: {type(value).__name__}")

    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _format_soda_timestamp(value: date | datetime | str) -> str:
    return _coerce_datetime(value).isoformat(timespec="milliseconds")


def _parse_soda_timestamp(value: object) -> datetime:
    if value is None:
        raise ValueError("SODA record is missing the configured watermark field")
    return _coerce_datetime(str(value))


def _session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    token = os.environ.get("DATASF_APP_TOKEN")
    if token:
        s.headers["X-App-Token"] = token
    return s


class SodaClient:
    """Small SODA API client for stable, timestamp-watermarked pagination."""

    def __init__(self, session: requests.Session | None = None, timeout: int = 60) -> None:
        self._session = session or _session()
        self._timeout = timeout

    @staticmethod
    def where_clause(
        config: DatasetConfig,
        since: date | datetime | str,
        *,
        include_since: bool = False,
    ) -> str:
        operator = ">=" if include_since else ">"
        return (
            f"`{config.date_field}` {operator} "
            f"'{_format_soda_timestamp(since)}'"
        )

    @staticmethod
    def order_clause(config: DatasetConfig) -> str:
        if config.order_field == config.date_field:
            return f"`{config.date_field}`"
        return f"`{config.date_field}`, `{config.order_field}`"

    def count_records(
        self,
        config: DatasetConfig,
        since: date | datetime | str,
        *,
        include_since: bool = False,
    ) -> int:
        """Return the total number of records after the configured watermark."""
        resp = self._session.get(
            config.endpoint,
            params={
                "$select": "count(*)",
                "$where": self.where_clause(config, since, include_since=include_since),
            },
            timeout=self._timeout,
        )
        resp.raise_for_status()
        return int(resp.json()[0]["count"])

    def fetch_records(
        self,
        config: DatasetConfig,
        since: date | datetime | str,
        *,
        include_since: bool = False,
    ) -> Iterator[dict]:
        """Yield records after the watermark, paginated with a stable order."""
        offset = 0
        where = self.where_clause(config, since, include_since=include_since)
        params = {
            "$where": where,
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


def count_records(
    config: DatasetConfig,
    since: date | datetime | str,
    *,
    include_since: bool = False,
) -> int:
    """Compatibility wrapper around :class:`SodaClient`."""
    return SodaClient().count_records(config, since, include_since=include_since)


def fetch_records(
    config: DatasetConfig,
    since: date | datetime | str,
    *,
    include_since: bool = False,
) -> Iterator[dict]:
    """Compatibility wrapper around :class:`SodaClient`."""
    yield from SodaClient().fetch_records(config, since, include_since=include_since)


def _s3_key(config: DatasetConfig, run_date: date) -> str:
    return f"raw/{config.name}/{run_date:%Y/%m/%d}/{config.name}.json"


class NdjsonS3Writer:
    """Stream records through a temp NDJSON file, then upload once to S3."""

    def __init__(self, bucket: str | None = None, s3_client=None) -> None:
        self.bucket = bucket or os.environ.get("AWS_S3_BUCKET")
        if not self.bucket:
            raise RuntimeError("AWS_S3_BUCKET must be set")
        if s3_client is None:
            import boto3

            s3_client = boto3.client("s3")
        self._s3 = s3_client

    def write_records(
        self,
        config: DatasetConfig,
        run_date: date,
        records: Iterable[dict],
    ) -> WriteResult:
        key = _s3_key(config, run_date)
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
                return WriteResult(None, 0, 0)

            self._s3.upload_file(
                str(tmp_path),
                self.bucket,
                key,
                ExtraArgs={"ContentType": "application/x-ndjson"},
            )
            return WriteResult(
                f"s3://{self.bucket}/{key}",
                records_written,
                bytes_written,
            )
        finally:
            if tmp_path is not None:
                tmp_path.unlink(missing_ok=True)


def _write_s3(config: DatasetConfig, records: Iterable[dict], run_date: date) -> str | None:
    """Compatibility wrapper around :class:`NdjsonS3Writer`."""
    return NdjsonS3Writer().write_records(config, run_date, records).s3_path


def run(
    config: DatasetConfig,
    run_date: date,
    since: date | datetime | str,
    *,
    include_since: bool = False,
    client: SodaClient | None = None,
    writer: NdjsonS3Writer | None = None,
) -> RunResult:
    client = client or SodaClient()
    writer = writer or NdjsonS3Writer()

    since_dt = _coerce_datetime(since)
    t0 = time.monotonic()
    records_seen = 0
    max_watermark: datetime | None = None

    def observed_records() -> Iterator[dict]:
        nonlocal max_watermark, records_seen
        for record in client.fetch_records(config, since_dt, include_since=include_since):
            watermark = _parse_soda_timestamp(record.get(config.date_field))
            if max_watermark is None or watermark > max_watermark:
                max_watermark = watermark
            records_seen += 1
            yield record

    write_result = writer.write_records(config, run_date, observed_records())
    fetch_duration = time.monotonic() - t0

    if write_result.records_written != records_seen:
        raise RuntimeError(
            "record count mismatch while writing "
            f"{config.name}: saw {records_seen}, wrote {write_result.records_written}"
        )

    if records_seen == 0:
        logger.info("completed %s: no records after %s", config.name, since_dt)
        return RunResult(
            s3_path=None,
            max_watermark=None,
            records_fetched=0,
            fetch_duration_seconds=round(fetch_duration, 2),
        )

    logger.info(
        "completed %s: %d records, watermark %s, wrote %d bytes to %s in %.1fs",
        config.name,
        records_seen,
        max_watermark,
        write_result.bytes_written,
        write_result.s3_path,
        fetch_duration,
    )
    return RunResult(
        s3_path=write_result.s3_path,
        max_watermark=max_watermark,
        records_fetched=records_seen,
        fetch_duration_seconds=round(fetch_duration, 2),
    )


def _parse_since_arg(value: str) -> datetime:
    return _coerce_datetime(value)


def cli(config: DatasetConfig) -> None:
    """Command-line entry point for a single dataset (backfill / ad-hoc run).

    Used by the dataset modules' ``__main__`` blocks. The Airflow ingest DAGs
    do not use this — they call ``run`` directly with a watermark read from
    Snowflake (see airflow/dags/dag_factory.py).
    """
    import argparse
    import logging

    from dotenv import load_dotenv

    parser = argparse.ArgumentParser(
        description=f"Fetch SF {config.name} from the DataSF SODA API."
    )
    parser.add_argument(
        "--run-date", type=date.fromisoformat, default=date.today(),
        help="Date to label the run (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--since",
        type=_parse_since_arg,
        default=datetime.combine(config.epoch, datetime_time.min),
        help="Fetch records on or after this date (YYYY-MM-DD). Defaults to the "
             "dataset epoch (full backfill). Use yearly chunks for large datasets.",
    )
    args = parser.parse_args()

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(config, run_date=args.run_date, since=args.since, include_since=True)
