"""Extract SF Police Department incident reports from the DataSF SODA API and land them as raw JSON.

Writes to s3://$AWS_S3_BUCKET/raw/incidents/YYYY/MM/DD/incidents.json.
"""
from __future__ import annotations

from datetime import date

try:
    from scripts.soda_ingest import DatasetConfig, cli
except ImportError:
    from soda_ingest import DatasetConfig, cli  # type: ignore[no-redef]  # standalone

INCIDENTS_CONFIG = DatasetConfig(
    name="incidents",
    dataset_id="wg3w-h783",
    date_field="data_loaded_at",
    order_field="row_id",
    epoch=date(2018, 1, 1),
)


if __name__ == "__main__":
    cli(INCIDENTS_CONFIG)
