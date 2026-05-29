"""Extract SF building permits from the DataSF SODA API and land them as raw JSON.

Writes to s3://$AWS_S3_BUCKET/raw/permits/YYYY/MM/DD/permits.json.
"""
from __future__ import annotations

from datetime import date

try:
    from scripts.soda_ingest import DatasetConfig, cli
except ImportError:
    from soda_ingest import DatasetConfig, cli  # type: ignore[no-redef]  # standalone

PERMITS_CONFIG = DatasetConfig(
    name="permits",
    dataset_id="i98e-djp9",
    date_field="data_loaded_at",
    order_field="permit_number",
    epoch=date(2013, 1, 1),
)


if __name__ == "__main__":
    cli(PERMITS_CONFIG)
