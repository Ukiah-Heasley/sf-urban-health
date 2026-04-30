"""Extract SF building permits from the DataSF SODA API and land them as raw JSON.

Writes to s3://$AWS_S3_BUCKET/raw/permits/YYYY/MM/DD/permits_<offset>.json.
"""
from __future__ import annotations

from datetime import date

try:
    from scripts.soda_ingest import DatasetConfig, run as _run
except ImportError:
    from soda_ingest import DatasetConfig, run as _run  # type: ignore[no-redef]  # standalone

PERMITS_CONFIG = DatasetConfig(
    name="permits",
    dataset_id="i98e-djp9",
    date_field="filed_date",
    order_field="permit_number",
    epoch=date(2013, 1, 1),
)


def run(run_date: date | None = None) -> str:
    return _run(PERMITS_CONFIG, run_date)


if __name__ == "__main__":
    import argparse
    import logging

    from dotenv import load_dotenv

    parser = argparse.ArgumentParser(description="Fetch SF building permits from DataSF.")
    parser.add_argument("--run-date", type=date.fromisoformat, default=None,
                        help="Date to run for (YYYY-MM-DD). Defaults to today.")
    args = parser.parse_args()

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(run_date=args.run_date)
