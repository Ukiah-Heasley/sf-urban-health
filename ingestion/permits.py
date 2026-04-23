"""Extract SF building permits from the DataSF SODA API and land them as raw JSON.

Writes to s3://$AWS_S3_BUCKET/raw/permits/YYYY/MM/DD/permits.json when AWS
credentials are present, otherwise writes to ./data/raw/permits/YYYY/MM/DD/
for local development.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, timedelta
from pathlib import Path
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
    if token:
        s.headers["X-App-Token"] = token
    return s


def fetch_permits(since: date) -> Iterator[dict]:
    """Yield permit records filed on or after `since`, paginated."""
    session = _session()
    offset = 0
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
        if len(batch) < PAGE_SIZE:
            return
        offset += PAGE_SIZE


def _s3_key(run_date: date) -> str:
    return f"raw/permits/{run_date:%Y/%m/%d}/permits.json"


def _write_local(records: list[dict], run_date: date, root: Path | None = None) -> Path:
    if root is None:
        root = Path(os.environ.get("LOCAL_DATA_DIR", "./data"))
    path = root / _s3_key(run_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    return path


def _load_duckdb(data_root: Path) -> Path:
    import duckdb

    db_path = data_root / "permits.db"
    con = duckdb.connect(str(db_path))
    glob = str(data_root / "raw/permits/**/*.json")
    con.execute(f"""
        CREATE OR REPLACE TABLE permits AS
        SELECT * FROM read_ndjson_auto('{glob}', filename=true)
    """)
    row = con.execute("SELECT COUNT(*) FROM permits").fetchone()
    count = row[0] if row else 0
    con.close()
    logger.info("duckdb: loaded %d rows into %s", count, db_path)
    return db_path


def _write_s3(records: list[dict], run_date: date) -> str:
    import boto3

    bucket = os.environ["AWS_S3_BUCKET"]
    key = _s3_key(run_date)
    body = ("\n".join(json.dumps(r) for r in records) + "\n").encode()
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=body)
    return f"s3://{bucket}/{key}"


def run(run_date: date | None = None, lookback_days: int = 7) -> str:
    run_date = run_date or date.today()
    since = run_date - timedelta(days=lookback_days)
    records = list(fetch_permits(since))
    logger.info("fetched %d permit records since %s", len(records), since)
    if os.environ.get("AWS_S3_BUCKET"):
        dest = _write_s3(records, run_date)
    else:
        root = Path(os.environ.get("LOCAL_DATA_DIR", "./data"))
        dest = str(_write_local(records, run_date, root))
        _load_duckdb(root)
    logger.info("wrote permits to %s", dest)
    return dest


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch SF building permits from DataSF.")
    parser.add_argument("--run-date", type=date.fromisoformat, default=None,
                        help="Date to run for (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--lookback-days", type=int, default=7,
                        help="How many days back to fetch permits for (default: 7).")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(run_date=args.run_date, lookback_days=args.lookback_days)
