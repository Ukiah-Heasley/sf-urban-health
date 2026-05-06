"""Extract SF Police Department incident reports from the DataSF SODA API and land them as raw JSON.

Writes to s3://$AWS_S3_BUCKET/raw/incidents/YYYY/MM/DD/incidents.json.
"""
from __future__ import annotations

from datetime import date

try:
    from scripts.soda_ingest import DatasetConfig, run as _run
except ImportError:
    from soda_ingest import DatasetConfig, run as _run  # type: ignore[no-redef]  # standalone

INCIDENTS_CONFIG = DatasetConfig(
    name="incidents",
    dataset_id="wg3w-h783",
    date_field="data_loaded_at",
    order_field="row_id",
    epoch=date(2018, 1, 1),
)


def run(run_date: date, since: date) -> tuple[str, date]:
    return _run(INCIDENTS_CONFIG, run_date, since)


if __name__ == "__main__":
    import argparse
    import logging

    from dotenv import load_dotenv

    parser = argparse.ArgumentParser(description="Fetch SF incident reports from DataSF.")
    parser.add_argument("--run-date", type=date.fromisoformat, default=date.today(),
                        help="Date to label the run (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--since", type=date.fromisoformat, default=INCIDENTS_CONFIG.epoch,
                        help="Fetch records on or after this date (YYYY-MM-DD). Defaults to epoch (full backfill). Use yearly chunks for large datasets.")
    args = parser.parse_args()

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(run_date=args.run_date, since=args.since)
