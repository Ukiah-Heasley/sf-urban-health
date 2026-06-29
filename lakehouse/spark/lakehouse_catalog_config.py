"""Lakehouse Spark catalog configuration helpers (local MinIO and AWS Glue)."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping, Sequence

DEFAULT_CATALOG = "hadoop"
DEFAULT_BUCKET = "lakehouse"
DEFAULT_BRONZE_BASE_URI = f"s3a://{DEFAULT_BUCKET}/lake/parquet/bronze"
SPARK_CATALOG = "spark_catalog"


class LakehouseCatalogConfigError(ValueError):
    """Invalid lakehouse catalog configuration."""


def catalog_mode(env: Mapping[str, str] | None = None) -> str:
    source = os.environ if env is None else env
    mode = source.get("LAKEHOUSE_CATALOG", DEFAULT_CATALOG).strip().lower()
    if mode not in {"hadoop", "glue"}:
        raise LakehouseCatalogConfigError(
            f"LAKEHOUSE_CATALOG must be 'hadoop' or 'glue', got {mode!r}"
        )
    return mode


def bronze_base_uri(env: Mapping[str, str] | None = None) -> str:
    source = os.environ if env is None else env
    explicit = source.get("LAKEHOUSE_BRONZE_BASE_URI")
    if explicit:
        return explicit.rstrip("/")
    bucket = source.get("LAKEHOUSE_BUCKET", DEFAULT_BUCKET)
    return f"s3a://{bucket}/lake/parquet/bronze"


def bronze_dataset_prefix(dataset: str, env: Mapping[str, str] | None = None) -> str:
    return f"{bronze_base_uri(env)}/{dataset.strip('/')}/"


def warehouse_uri(env: Mapping[str, str] | None = None) -> str:
    source = os.environ if env is None else env
    mode = catalog_mode(source)
    if mode == "glue":
        warehouse = source.get("LAKEHOUSE_WAREHOUSE_URI", "").strip()
        if not warehouse:
            raise LakehouseCatalogConfigError(
                "LAKEHOUSE_WAREHOUSE_URI is required when LAKEHOUSE_CATALOG=glue"
            )
        return warehouse
    bucket = source.get("LAKEHOUSE_BUCKET", DEFAULT_BUCKET)
    return f"s3a://{bucket}/warehouse"


def spark_thrift_conf_args(env: Mapping[str, str] | None = None) -> list[str]:
    source = os.environ if env is None else env
    mode = catalog_mode(source)
    warehouse = warehouse_uri(source)
    confs: dict[str, str] = {
        "spark.sql.catalog.spark_catalog.warehouse": warehouse,
        "spark.sql.warehouse.dir": warehouse,
    }

    if mode == "glue":
        confs["spark.sql.catalog.spark_catalog.type"] = "glue"
        confs[
            "spark.sql.catalog.spark_catalog.io-impl"
        ] = "org.apache.iceberg.aws.s3.S3FileIO"
    else:
        confs["spark.sql.catalog.spark_catalog.type"] = "hadoop"
        confs["spark.hadoop.fs.s3a.endpoint"] = source.get(
            "MINIO_ENDPOINT", "http://minio:9000"
        )
        confs["spark.hadoop.fs.s3a.access.key"] = source.get(
            "MINIO_ROOT_USER", "minioadmin"
        )
        confs["spark.hadoop.fs.s3a.secret.key"] = source.get(
            "MINIO_ROOT_PASSWORD", "minioadmin"
        )
        confs["spark.hadoop.fs.s3a.path.style.access"] = "true"
        confs["spark.hadoop.fs.s3a.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem"
        confs["spark.hadoop.fs.s3a.connection.ssl.enabled"] = "false"

    return ["--master", "local[*]"] + [
        f"--conf={key}={value}" for key, value in confs.items()
    ]


def bootstrap_sql(env: Mapping[str, str] | None = None) -> str:
    _ = env
    return (
        "CREATE NAMESPACE IF NOT EXISTS default; "
        "CREATE NAMESPACE IF NOT EXISTS sf_urban_health;"
    )


def _emit_shell_args(args: Sequence[str]) -> None:
    for arg in args:
        print(arg)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--shell-args",
        action="store_true",
        help="Print spark-thrift --conf arguments, one per line",
    )
    parser.add_argument(
        "--bootstrap-sql",
        action="store_true",
        help="Print bootstrap CREATE NAMESPACE SQL",
    )
    parser.add_argument(
        "--warehouse-uri",
        action="store_true",
        help="Print resolved Iceberg warehouse URI",
    )
    parsed = parser.parse_args(list(argv) if argv is not None else None)

    try:
        if parsed.shell_args:
            _emit_shell_args(spark_thrift_conf_args())
        elif parsed.bootstrap_sql:
            print(bootstrap_sql())
        elif parsed.warehouse_uri:
            print(warehouse_uri())
        else:
            parser.error("specify --shell-args, --bootstrap-sql, or --warehouse-uri")
    except LakehouseCatalogConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
