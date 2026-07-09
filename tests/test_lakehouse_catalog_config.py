"""Unit tests for lakehouse Spark catalog configuration helpers."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lakehouse" / "spark"))

from lakehouse_catalog_config import (  # noqa: E402
    LakehouseCatalogConfigError,
    bronze_base_uri,
    bronze_dataset_prefix,
    catalog_mode,
    spark_sql_warehouse_dir,
    spark_thrift_conf_args,
    warehouse_uri,
)

CATALOG_CONFIG = Path(__file__).resolve().parents[1] / "lakehouse" / "spark" / "lakehouse_catalog_config.py"


def test_catalog_mode_defaults_to_hadoop() -> None:
    assert catalog_mode({}) == "hadoop"


def test_catalog_mode_accepts_glue() -> None:
    assert catalog_mode({"LAKEHOUSE_CATALOG": "glue"}) == "glue"


def test_catalog_mode_rejects_unknown_value() -> None:
    with pytest.raises(LakehouseCatalogConfigError, match="must be 'hadoop' or 'glue'"):
        catalog_mode({"LAKEHOUSE_CATALOG": "polaris"})


def test_bronze_base_uri_defaults_to_local_minio_shape() -> None:
    assert bronze_base_uri({}) == "s3a://lakehouse/lake/parquet/bronze"


def test_bronze_base_uri_honors_explicit_override() -> None:
    env = {"LAKEHOUSE_BRONZE_BASE_URI": "s3a://sf-urban-health/lake/parquet/bronze/"}
    assert bronze_base_uri(env) == "s3a://sf-urban-health/lake/parquet/bronze"


def test_bronze_dataset_prefix_trims_and_appends_dataset() -> None:
    env = {"LAKEHOUSE_BRONZE_BASE_URI": "s3a://bucket/lake/parquet/bronze/"}
    assert bronze_dataset_prefix("permits", env) == "s3a://bucket/lake/parquet/bronze/permits/"


def test_warehouse_uri_hadoop_uses_s3a_bucket_path() -> None:
    assert warehouse_uri({"LAKEHOUSE_BUCKET": "lakehouse"}) == "s3a://lakehouse/warehouse"


def test_warehouse_uri_glue_requires_explicit_uri() -> None:
    with pytest.raises(LakehouseCatalogConfigError, match="LAKEHOUSE_WAREHOUSE_URI"):
        warehouse_uri({"LAKEHOUSE_CATALOG": "glue"})


def test_warehouse_uri_glue_uses_s3_scheme() -> None:
    env = {
        "LAKEHOUSE_CATALOG": "glue",
        "LAKEHOUSE_WAREHOUSE_URI": "s3://sf-urban-health/warehouse",
    }
    assert warehouse_uri(env) == "s3://sf-urban-health/warehouse"


def test_spark_sql_warehouse_dir_glue_uses_s3a_scheme() -> None:
    env = {
        "LAKEHOUSE_CATALOG": "glue",
        "LAKEHOUSE_WAREHOUSE_URI": "s3://sf-urban-health/warehouse",
    }
    assert spark_sql_warehouse_dir(env) == "s3a://sf-urban-health/warehouse"


def test_spark_thrift_conf_args_hadoop_includes_minio_s3a_settings() -> None:
    args = spark_thrift_conf_args(
        {
            "LAKEHOUSE_CATALOG": "hadoop",
            "LAKEHOUSE_BUCKET": "lakehouse",
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ROOT_USER": "minioadmin",
            "MINIO_ROOT_PASSWORD": "minioadmin",
        }
    )
    joined = " ".join(args)
    assert "--conf=spark.sql.catalog.spark_catalog.type=hadoop" in joined
    assert "--conf=spark.sql.catalog.spark_catalog.warehouse=s3a://lakehouse/warehouse" in joined
    assert "--conf=spark.hadoop.fs.s3a.endpoint=http://minio:9000" in joined
    assert "--conf=spark.sql.parquet.compression.codec=snappy" in joined
    assert (
        "--conf=spark.sql.catalog.spark_catalog.table-default.write.parquet.compression-codec=snappy"
        in joined
    )
    assert (
        "--conf=spark.sql.catalog.spark_catalog.table-override.write.parquet.compression-codec=snappy"
        in joined
    )
    assert "--conf=spark.sql.codegen.wholeStage=false" in joined
    assert "--conf=spark.sql.codegen.factoryMode=NO_CODEGEN" in joined


def test_spark_thrift_conf_args_glue_uses_glue_and_s3_file_io() -> None:
    args = spark_thrift_conf_args(
        {
            "LAKEHOUSE_CATALOG": "glue",
            "LAKEHOUSE_WAREHOUSE_URI": "s3://sf-urban-health/warehouse",
        }
    )
    joined = " ".join(args)
    assert "--conf=spark.sql.catalog.spark_catalog.type=glue" in joined
    assert (
        "--conf=spark.sql.catalog.spark_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
        in joined
    )
    assert (
        "--conf=spark.sql.catalog.spark_catalog.warehouse=s3://sf-urban-health/warehouse"
        in joined
    )
    assert "--conf=spark.sql.warehouse.dir=s3a://sf-urban-health/warehouse" in joined
    assert "--conf=spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" in joined
    assert "--conf=spark.hadoop.fs.s3a.aws.credentials.provider=" in joined
    assert "--conf=spark.sql.parquet.compression.codec=snappy" in joined
    assert (
        "--conf=spark.sql.catalog.spark_catalog.table-default.write.parquet.compression-codec=snappy"
        in joined
    )
    assert (
        "--conf=spark.sql.catalog.spark_catalog.table-override.write.parquet.compression-codec=snappy"
        in joined
    )
    assert "--conf=spark.sql.codegen.wholeStage=false" in joined
    assert "--conf=spark.sql.codegen.factoryMode=NO_CODEGEN" in joined
    assert "s3a.endpoint" not in joined


def test_cli_shell_args_emits_spark_conf_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LAKEHOUSE_CATALOG", "hadoop")
    monkeypatch.setenv("LAKEHOUSE_BUCKET", "lakehouse")
    result = subprocess.run(
        [sys.executable, str(CATALOG_CONFIG), "--shell-args"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "--conf=spark.sql.catalog.spark_catalog.type=hadoop" in result.stdout
