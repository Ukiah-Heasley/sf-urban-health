"""Generic SODA API ingestion: paginate a DataSF endpoint → S3 raw JSON."""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import date
from typing import Iterator

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
    secret = os.environ.get("DATASF_SECRET_TOKEN")
    if token and secret:
        s.auth = (token, secret)
    elif token:
        s.headers["X-App-Token"] = token
    return s


def count_records(config: DatasetConfig, since: date) -> int:
    """Return the total number of records filed on or after `since`."""
    where = f"{config.date_field} >= '{since.isoformat()}T00:00:00.000'"
    resp = _session().get(
        config.endpoint,
        params={"$select": "count(*)", "$where": where},
        timeout=60,
    )
    resp.raise_for_status()
    return int(resp.json()[0]["count"])


def fetch_records(config: DatasetConfig, since: date, resume_offset: int = 0) -> Iterator[dict]:
    """Yield records filed on or after `since`, paginated.

    If `resume_offset` is given, skips that many records before yielding
    (resumes a previous partial fetch).
    """
    session = _session()
    offset = resume_offset
    where = f"{config.date_field} >= '{since.isoformat()}T00:00:00.000'"
    while True:
        params = {
            "$where": where,
            "$limit": config.page_size,
            "$offset": offset,
            "$order": config.order_field,
        }
        resp = session.get(config.endpoint, params=params, timeout=60)
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


def _s3_key(config: DatasetConfig, run_date: date, start_offset: int = 0) -> str:
    return f"raw/{config.name}/{run_date:%Y/%m/%d}/{config.name}_{start_offset}.json"


def _write_s3(config: DatasetConfig, records: list[dict], run_date: date, start_offset: int = 0) -> str:
    import boto3

    bucket = os.environ["AWS_S3_BUCKET"]
    key = _s3_key(config, run_date, start_offset)
    body = ("\n".join(json.dumps(r) for r in records) + "\n").encode()
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=body)
    return f"s3://{bucket}/{key}"


def _checkpoint_key(config: DatasetConfig) -> str:
    return f"checkpoints/{config.name}.json"


def _manifest_key(config: DatasetConfig) -> str:
    return f"checkpoints/{config.name}_runs.jsonl"


def _read_checkpoint(config: DatasetConfig) -> tuple[date, int] | None:
    """Return (since_date, resume_offset) or None if no checkpoint exists."""
    import boto3
    from botocore.exceptions import ClientError

    bucket = os.environ["AWS_S3_BUCKET"]
    try:
        obj = boto3.client("s3").get_object(Bucket=bucket, Key=_checkpoint_key(config))
        data = json.loads(obj["Body"].read())
        return date.fromisoformat(data["last_loaded_date"]), data.get("resume_offset", 0)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise


def _write_checkpoint(config: DatasetConfig, run_date: date, resume_offset: int | None = None) -> None:
    import boto3

    bucket = os.environ["AWS_S3_BUCKET"]
    data: dict = {"last_loaded_date": run_date.isoformat()}
    if resume_offset:
        data["resume_offset"] = resume_offset
    boto3.client("s3").put_object(
        Bucket=bucket, Key=_checkpoint_key(config), Body=json.dumps(data).encode()
    )


def _write_run_manifest(
    config: DatasetConfig,
    run_date: date,
    since: date,
    record_count: int,
    complete: bool = True,
) -> None:
    import boto3
    from botocore.exceptions import ClientError

    bucket = os.environ["AWS_S3_BUCKET"]
    client = boto3.client("s3")
    entry = json.dumps({
        "run_date": run_date.isoformat(),
        "since": since.isoformat(),
        "records_fetched": record_count,
        "status": "success" if complete else "incomplete",
    })
    existing = b""
    try:
        obj = client.get_object(Bucket=bucket, Key=_manifest_key(config))
        existing = obj["Body"].read()
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchKey":
            raise
    client.put_object(Bucket=bucket, Key=_manifest_key(config), Body=existing + (entry + "\n").encode())


def run(config: DatasetConfig, run_date: date | None = None) -> str:
    run_date = run_date or date.today()
    if not os.environ.get("AWS_S3_BUCKET"):
        raise RuntimeError("AWS_S3_BUCKET must be set")
    checkpoint = _read_checkpoint(config)
    since, resume_offset = checkpoint if checkpoint else (config.epoch, 0)
    if resume_offset:
        logger.info("resuming from offset %d (since %s)", resume_offset, since)
    expected = count_records(config, since)
    logger.info("API reports %d %s records since %s", expected, config.name, since)
    records = list(fetch_records(config, since, resume_offset=resume_offset))
    logger.info("fetched %d records", len(records))
    dest = _write_s3(config, records, run_date, start_offset=resume_offset)
    total_fetched = resume_offset + len(records)
    if total_fetched < expected:
        logger.warning(
            "incomplete fetch: %d of %d records total — "
            "saving offset %d, next run resumes from there",
            total_fetched, expected, total_fetched,
        )
        _write_checkpoint(config, since, resume_offset=total_fetched)
    else:
        _write_checkpoint(config, run_date)
    _write_run_manifest(config, run_date, since, total_fetched, complete=total_fetched >= expected)
    logger.info("wrote %s to %s", config.name, dest)
    return dest
