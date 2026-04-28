"""Extract SF building permits from the DataSF SODA API and land them as raw JSON.

Writes to s3://$AWS_S3_BUCKET/raw/permits/YYYY/MM/DD/permits.json.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date
from typing import Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

SODA_ENDPOINT = "https://data.sfgov.org/resource/i98e-djp9.json"
PAGE_SIZE = 1000


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


def count_permits(since: date) -> int:
    """Return the total number of permits filed on or after `since`."""
    where = f"filed_date >= '{since.isoformat()}T00:00:00.000'"
    resp = _session().get(
        SODA_ENDPOINT,
        params={"$select": "count(*)", "$where": where},
        timeout=60,
    )
    resp.raise_for_status()
    return int(resp.json()[0]["count"])


def fetch_permits(since: date, resume_offset: int = 0) -> Iterator[dict]:
    """Yield permit records filed on or after `since`, paginated.

    If `resume_offset` is given, skips that many records before yielding
    (resumes a previous partial fetch).
    """
    session = _session()
    offset = resume_offset
    where = f"filed_date >= '{since.isoformat()}T00:00:00.000'"
    while True:
        params = {
            "$where": where,
            "$limit": PAGE_SIZE,
            "$offset": offset,
            "$order": "permit_number",
        }
        resp = session.get(SODA_ENDPOINT, params=params, timeout=60)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            return
        yield from batch
        offset += PAGE_SIZE
        if offset % 50_000 == 0:
            logger.info("fetch progress: %d records retrieved so far", offset)
        if len(batch) < PAGE_SIZE:
            return


def _s3_key(run_date: date, start_offset: int = 0) -> str:
    return f"raw/permits/{run_date:%Y/%m/%d}/permits_{start_offset}.json"


def _write_s3(records: list[dict], run_date: date, start_offset: int = 0) -> str:
    import boto3

    bucket = os.environ["AWS_S3_BUCKET"]
    key = _s3_key(run_date, start_offset)
    body = ("\n".join(json.dumps(r) for r in records) + "\n").encode()
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=body)
    return f"s3://{bucket}/{key}"


_CHECKPOINT_KEY = "checkpoints/permits.json"
_MANIFEST_KEY = "checkpoints/permits_runs.jsonl"
_EPOCH = date(2013, 1, 1)


def _read_checkpoint() -> tuple[date, int] | None:
    """Return (since_date, resume_offset) or None if no checkpoint exists."""
    import boto3
    from botocore.exceptions import ClientError

    bucket = os.environ["AWS_S3_BUCKET"]
    try:
        obj = boto3.client("s3").get_object(Bucket=bucket, Key=_CHECKPOINT_KEY)
        data = json.loads(obj["Body"].read())
        return date.fromisoformat(data["last_loaded_date"]), data.get("resume_offset", 0)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise


def _write_checkpoint(run_date: date, resume_offset: int | None = None) -> None:
    import boto3

    bucket = os.environ["AWS_S3_BUCKET"]
    data: dict = {"last_loaded_date": run_date.isoformat()}
    if resume_offset:
        data["resume_offset"] = resume_offset
    boto3.client("s3").put_object(
        Bucket=bucket, Key=_CHECKPOINT_KEY, Body=json.dumps(data).encode()
    )


def _write_run_manifest(run_date: date, since: date, record_count: int, complete: bool = True) -> None:
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
        obj = client.get_object(Bucket=bucket, Key=_MANIFEST_KEY)
        existing = obj["Body"].read()
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchKey":
            raise
    body = existing + (entry + "\n").encode()
    client.put_object(Bucket=bucket, Key=_MANIFEST_KEY, Body=body)


def run(run_date: date | None = None) -> str:
    run_date = run_date or date.today()
    if not os.environ.get("AWS_S3_BUCKET"):
        raise RuntimeError("AWS_S3_BUCKET must be set")
    checkpoint = _read_checkpoint()
    since, resume_offset = checkpoint if checkpoint else (_EPOCH, 0)
    if resume_offset:
        logger.info("resuming from offset %d (since %s)", resume_offset, since)
    expected = count_permits(since)
    logger.info("API reports %d permit records since %s", expected, since)
    records = list(fetch_permits(since, resume_offset=resume_offset))
    logger.info("fetched %d records", len(records))
    dest = _write_s3(records, run_date, start_offset=resume_offset)
    total_fetched = resume_offset + len(records)
    if total_fetched < expected:
        logger.warning(
            "incomplete fetch: %d of %d records total — "
            "saving offset %d, next run resumes from there",
            total_fetched, expected, total_fetched,
        )
        _write_checkpoint(since, resume_offset=total_fetched)
    else:
        _write_checkpoint(run_date)
    _write_run_manifest(run_date, since, total_fetched, complete=total_fetched >= expected)
    logger.info("wrote permits to %s", dest)
    return dest


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch SF building permits from DataSF.")
    parser.add_argument("--run-date", type=date.fromisoformat, default=None,
                        help="Date to run for (YYYY-MM-DD). Defaults to today.")
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(run_date=args.run_date)
