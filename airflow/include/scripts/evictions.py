"""Dataset config and CLI entry point for SF eviction-notice raw extracts.

Input:    DataSF SODA resource 5cei-gny5, filtered by data_loaded_at.
Output:   Raw interval NDJSON under s3://$AWS_S3_BUCKET/raw/evictions/.
"""

from __future__ import annotations

from datetime import date

try:
    from scripts.soda_ingest import DatasetConfig, cli
except ImportError:
    from soda_ingest import DatasetConfig, cli  # type: ignore[no-redef]  # standalone

EVICTIONS_CONFIG = DatasetConfig(
    name="evictions",
    dataset_id="5cei-gny5",
    date_field="data_loaded_at",
    order_field="eviction_id",
    epoch=date(1997, 1, 1),
)


if __name__ == "__main__":
    cli(EVICTIONS_CONFIG)
