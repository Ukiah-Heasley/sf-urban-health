"""Dataset config and CLI entry point for SF incident-report raw extracts.

Input:    DataSF SODA resource wg3w-h783, filtered by data_loaded_at.
Output:   Raw interval NDJSON under s3://$AWS_S3_BUCKET/raw/incidents/.
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
