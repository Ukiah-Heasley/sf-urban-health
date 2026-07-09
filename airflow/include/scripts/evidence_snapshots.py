"""Export Evidence Parquet snapshots from lakehouse gold tables."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.lakehouse_local import (
    LakehouseLocalError,
    load_lakehouse_env,
    repo_root,
    require_local_lakehouse_stack,
    spark_thrift_host,
    spark_thrift_port,
)


class EvidenceSnapshotsError(RuntimeError):
    """Raised when snapshot export cannot complete."""


class SnapshotSource(str, Enum):
    SPARK_GOLD = "spark_gold"


@dataclass(frozen=True)
class SnapshotSpec:
    filename: str
    table_name: str
    source: SnapshotSource
    spark_query: str


GOLD_TABLES: tuple[str, ...] = (
    "housing_production",
    "permit_pipeline",
    "evictions",
    "public_safety",
    "pipeline_health",
    "data_trust",
)


def spark_schema() -> str:
    return os.environ.get("DBT_SPARK_SCHEMA", "sf_urban_health")


def spark_user() -> str:
    return os.environ.get("DBT_SPARK_USER", "spark")


def default_output_dir() -> Path:
    return repo_root() / "reports" / "sources" / "sf_urban_health" / "data"


def gold_table_query(table_name: str, schema: str | None = None) -> str:
    if table_name not in GOLD_TABLES:
        raise EvidenceSnapshotsError(f"unknown Evidence gold table: {table_name!r}")
    resolved = schema or spark_schema()
    return f"SELECT * FROM {resolved}.{table_name}"


def housing_production_query(schema: str | None = None) -> str:
    return gold_table_query("housing_production", schema)


def snapshot_specs(schema: str | None = None) -> tuple[SnapshotSpec, ...]:
    return tuple(
        SnapshotSpec(
            filename=f"{table_name}.parquet",
            table_name=table_name,
            source=SnapshotSource.SPARK_GOLD,
            spark_query=gold_table_query(table_name, schema),
        )
        for table_name in GOLD_TABLES
    )


def write_parquet_atomic(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    try:
        pq.write_table(table, temp_path)
        temp_path.replace(path)
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


def fetch_spark_table(cursor, query: str) -> pa.Table:
    try:
        cursor.execute(query)
    except Exception as exc:
        raise EvidenceSnapshotsError(f"Spark query failed: {query}\n{exc}") from exc

    if cursor.description is None:
        return pa.table({})

    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    if not rows:
        return pa.table({name: pa.array([], type=pa.null()) for name in columns})
    records = [dict(zip(columns, row, strict=True)) for row in rows]
    return pa.Table.from_pylist(records)


def open_spark_connection():
    from pyhive import hive

    host = spark_thrift_host()
    port = spark_thrift_port()
    try:
        return hive.Connection(
            host=host,
            port=port,
            auth="NOSASL",
            username=spark_user(),
            database=spark_schema(),
        )
    except Exception as exc:
        raise EvidenceSnapshotsError(
            "Spark Thrift Server is not reachable at "
            f"{host}:{port}. Start the stack with `make spark-up` or `make spark-up-aws`."
        ) from exc


def _uses_glue_catalog() -> bool:
    return os.environ.get("LAKEHOUSE_CATALOG", "").lower() == "glue"


def export_snapshots(
    output_dir: Path | None = None,
    *,
    skip_stack_check: bool = False,
) -> dict[str, int]:
    """Write Evidence snapshot Parquet files and return row counts by filename."""
    load_lakehouse_env()
    if not skip_stack_check and not _uses_glue_catalog():
        require_local_lakehouse_stack()

    destination = output_dir or default_output_dir()
    counts: dict[str, int] = {}
    specs = snapshot_specs()

    connection = open_spark_connection()
    try:
        cursor = connection.cursor()
        try:
            for spec in specs:
                target = destination / spec.filename
                table = fetch_spark_table(cursor, spec.spark_query)
                write_parquet_atomic(target, table)
                counts[spec.filename] = table.num_rows
                print(f"wrote {spec.filename} ({table.num_rows} rows) -> {target}")
        finally:
            cursor.close()
    finally:
        connection.close()

    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Export Evidence Parquet snapshots from lakehouse gold tables."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for snapshot Parquet files (default: reports/sources/sf_urban_health/data/)",
    )
    args = parser.parse_args(argv)
    try:
        export_snapshots(args.output_dir)
    except (EvidenceSnapshotsError, LakehouseLocalError) as exc:
        raise SystemExit(str(exc)) from exc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
