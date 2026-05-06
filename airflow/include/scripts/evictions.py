"""Extract SF eviction notices from the DataSF SODA API and land them as raw JSON.

Writes to s3://$AWS_S3_BUCKET/raw/evictions/YYYY/MM/DD/evictions.json.
"""
from __future__ import annotations

from datetime import date

try:
    from scripts.soda_ingest import DatasetConfig, run as _run
except ImportError:
    from soda_ingest import DatasetConfig, run as _run  # type: ignore[no-redef]  # standalone

EVICTIONS_CONFIG = DatasetConfig(
    name="evictions",
    dataset_id="5cei-gny5",
    date_field="data_loaded_at",
    order_field="eviction_id",
    epoch=date(1997, 1, 1),
)


def run(run_date: date, since: date) -> tuple[str, date]:
    return _run(EVICTIONS_CONFIG, run_date, since)


if __name__ == "__main__":
    import argparse
    import logging

    from dotenv import load_dotenv

    parser = argparse.ArgumentParser(description="Fetch SF eviction notices from DataSF.")
    parser.add_argument("--run-date", type=date.fromisoformat, default=date.today(),
                        help="Date to label the run (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--since", type=date.fromisoformat, default=EVICTIONS_CONFIG.epoch,
                        help="Fetch records on or after this date (YYYY-MM-DD). Defaults to epoch (full backfill).")
    args = parser.parse_args()

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(run_date=args.run_date, since=args.since)
