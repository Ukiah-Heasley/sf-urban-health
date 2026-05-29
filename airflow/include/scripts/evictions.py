"""Extract SF eviction notices from the DataSF SODA API and land them as raw JSON.

Writes to s3://$AWS_S3_BUCKET/raw/evictions/YYYY/MM/DD/evictions.json.
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
