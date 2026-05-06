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


def fetch_records(config: DatasetConfig, since: date) -> Iterator[dict]:
    """Yield records filed on or after `since`, paginated."""
    session = _session()
    offset = 0
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


def _s3_key(config: DatasetConfig, run_date: date) -> str:
    return f"raw/{config.name}/{run_date:%Y/%m/%d}/{config.name}.json"


def _write_s3(config: DatasetConfig, records: list[dict], run_date: date) -> str:
    import boto3

    bucket = os.environ["AWS_S3_BUCKET"]
    key = _s3_key(config, run_date)
    body = ("\n".join(json.dumps(r) for r in records) + "\n").encode()
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=body)
    return f"s3://{bucket}/{key}"


def run(config: DatasetConfig, run_date: date, since: date) -> tuple[str, date]:
    if not os.environ.get("AWS_S3_BUCKET"):
        raise RuntimeError("AWS_S3_BUCKET must be set")
    expected = count_records(config, since)
    logger.info("API reports %d %s records since %s", expected, config.name, since)
    records = list(fetch_records(config, since))
    logger.info("fetched %d records", len(records))
    dest = _write_s3(config, records, run_date)
    max_wm = date.fromisoformat(max(r[config.date_field] for r in records)[:10])
    logger.info("wrote %d %s records to %s (watermark: %s)", len(records), config.name, dest, max_wm)
    return dest, max_wm
