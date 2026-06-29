"""Export Evidence Parquet snapshots from the local lakehouse gold path."""
from __future__ import annotations

import argparse
import os
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
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
    GENERATED = "generated"


@dataclass(frozen=True)
class SnapshotSpec:
    filename: str
    source: SnapshotSource
    spark_query: str | None = None


_OBSERVABILITY_SEED = 42

_NEIGHBORHOODS = {
    "Mission": "9",
    "South Of Market": "6",
    "Bayview Hunters Point": "10",
    "Sunset/Parkside": "4",
    "Financial District/South Beach": "3",
}
_USE_TRANSITIONS = ["new_residential", "unit_addition", "renovation_same_use"]


def spark_schema() -> str:
    return os.environ.get("DBT_SPARK_SCHEMA", "sf_urban_health")


def spark_user() -> str:
    return os.environ.get("DBT_SPARK_USER", "spark")


def default_output_dir() -> Path:
    return repo_root() / "reports" / "sources" / "sf_urban_health" / "data"


def housing_production_query(schema: str | None = None) -> str:
    resolved = schema or spark_schema()
    return f"SELECT * FROM {resolved}.housing_production"


def snapshot_specs(schema: str | None = None) -> tuple[SnapshotSpec, ...]:
    return (
        SnapshotSpec(
            filename="mart_housing_production.parquet",
            source=SnapshotSource.SPARK_GOLD,
            spark_query=housing_production_query(schema),
        ),
        SnapshotSpec(
            filename="mart_pipeline_health.parquet",
            source=SnapshotSource.GENERATED,
        ),
        SnapshotSpec(
            filename="mart_data_trust.parquet",
            source=SnapshotSource.GENERATED,
        ),
    )


def _months(n: int) -> list[date]:
    start = date(date.today().year - 2, 1, 1)
    return [
        date(start.year + (start.month - 1 + index) // 12, (start.month - 1 + index) % 12 + 1, 1)
        for index in range(n)
    ]


def build_pipeline_health_snapshot(rng: random.Random | None = None) -> pa.Table:
    generator = rng or random.Random(_OBSERVABILITY_SEED)
    rows: list[dict[str, object]] = []
    dags = ["ingest_permits", "ingest_evictions", "ingest_incidents", "promote_raw_to_bronze"]
    for day_offset in range(30):
        run_date = date.today() - timedelta(days=29 - day_offset)
        for dag_id in dags:
            failed = 1 if generator.random() < 0.05 else 0
            total = 1
            success = total - failed
            duration = generator.uniform(40, 360)
            rows.append(
                {
                    "run_date": run_date,
                    "dag_id": dag_id,
                    "total_runs": total,
                    "successful_runs": success,
                    "failed_runs": failed,
                    "success_rate_pct": round(success * 100.0 / total, 1),
                    "avg_duration_seconds": round(duration, 1),
                    "p95_duration_seconds": round(duration * 1.2, 1),
                    "total_records_ingested": (
                        generator.randint(0, 5000)
                        if dag_id != "promote_raw_to_bronze"
                        else 0
                    ),
                    "avg_records_per_run": (
                        generator.randint(0, 5000)
                        if dag_id != "promote_raw_to_bronze"
                        else 0
                    ),
                }
            )
    return pa.Table.from_pylist(rows)


def build_data_trust_snapshot(rng: random.Random | None = None) -> pa.Table:
    generator = rng or random.Random(_OBSERVABILITY_SEED)
    today = date.today()
    rows = [
        ("Permits", "ingest_permits", "stg_permits"),
        ("Evictions", "ingest_evictions", "stg_evictions"),
        ("Incidents", "ingest_incidents", "stg_incidents"),
    ]
    output: list[dict[str, object]] = []
    for dataset_name, dag_id, model_name in rows:
        pass_rate = round(generator.uniform(92, 100), 1)
        trust = round(0.4 * 100 + 0.6 * pass_rate)
        output.append(
            {
                "dataset_name": dataset_name,
                "dag_id": dag_id,
                "model_name": model_name,
                "last_loaded_date": today,
                "days_since_last_load": 0,
                "freshness_status": "fresh",
                "test_pass_rate_7d": pass_rate,
                "total_tests_7d": generator.randint(8, 20),
                "failed_tests_7d": 0,
                "last_test_failure_at": datetime(today.year, 1, 1),
                "trust_score": trust,
                "trust_status": "trusted" if trust >= 90 else "degraded",
            }
        )
    return pa.Table.from_pylist(output)


def _generated_snapshot_table(
    spec: SnapshotSpec,
    rng: random.Random | None = None,
) -> pa.Table:
    if spec.filename == "mart_pipeline_health.parquet":
        return build_pipeline_health_snapshot(rng)
    if spec.filename == "mart_data_trust.parquet":
        return build_data_trust_snapshot(rng)
    raise EvidenceSnapshotsError(f"no generated snapshot builder for {spec.filename!r}")


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
            f"{host}:{port}. Start the stack with `make spark-up`."
        ) from exc


def export_snapshots(
    output_dir: Path | None = None,
    *,
    skip_stack_check: bool = False,
) -> dict[str, int]:
    """Write Evidence snapshot Parquet files and return row counts by filename."""
    load_lakehouse_env()
    if not skip_stack_check:
        require_local_lakehouse_stack()

    destination = output_dir or default_output_dir()
    counts: dict[str, int] = {}
    specs = snapshot_specs()
    observability_rng = random.Random(_OBSERVABILITY_SEED)

    connection = open_spark_connection()
    try:
        cursor = connection.cursor()
        try:
            for spec in specs:
                target = destination / spec.filename
                if spec.source is SnapshotSource.SPARK_GOLD:
                    if spec.spark_query is None:
                        raise EvidenceSnapshotsError(
                            f"missing Spark query for snapshot {spec.filename!r}"
                        )
                    table = fetch_spark_table(cursor, spec.spark_query)
                else:
                    table = _generated_snapshot_table(spec, observability_rng)
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
        description="Export Evidence Parquet snapshots from the local lakehouse."
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
