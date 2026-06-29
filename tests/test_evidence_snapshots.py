from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
import pytest

from scripts.evidence_snapshots import (
    EvidenceSnapshotsError,
    SnapshotSource,
    build_data_trust_snapshot,
    build_pipeline_health_snapshot,
    default_output_dir,
    export_snapshots,
    housing_production_query,
    main,
    open_spark_connection,
    snapshot_specs,
    write_parquet_atomic,
)
from scripts.lakehouse_local import LakehouseLocalError


def test_snapshot_specs_include_existing_filenames() -> None:
    filenames = {spec.filename for spec in snapshot_specs()}
    assert filenames == {
        "mart_housing_production.parquet",
        "mart_pipeline_health.parquet",
        "mart_data_trust.parquet",
    }


def test_housing_snapshot_query_targets_gold_table() -> None:
    assert housing_production_query("sf_urban_health") == (
        "SELECT * FROM sf_urban_health.housing_production"
    )
    housing_spec = next(
        spec for spec in snapshot_specs() if spec.filename == "mart_housing_production.parquet"
    )
    assert housing_spec.source is SnapshotSource.SPARK_GOLD
    assert housing_spec.spark_query == housing_production_query()


def test_observability_snapshots_are_generated_not_queried() -> None:
    observability = {
        spec.filename: spec.source
        for spec in snapshot_specs()
        if spec.filename != "mart_housing_production.parquet"
    }
    assert observability == {
        "mart_pipeline_health.parquet": SnapshotSource.GENERATED,
        "mart_data_trust.parquet": SnapshotSource.GENERATED,
    }


def test_generated_observability_snapshots_have_expected_columns() -> None:
    pipeline = build_pipeline_health_snapshot()
    assert pipeline.num_rows == 120
    assert "run_date" in pipeline.column_names
    assert "dag_id" in pipeline.column_names

    trust = build_data_trust_snapshot()
    assert trust.num_rows == 3
    assert "dataset_name" in trust.column_names
    assert "trust_score" in trust.column_names


def test_generated_observability_snapshots_are_repeatable() -> None:
    assert build_pipeline_health_snapshot().to_pylist() == (
        build_pipeline_health_snapshot().to_pylist()
    )
    assert build_data_trust_snapshot().to_pylist() == (
        build_data_trust_snapshot().to_pylist()
    )


def test_write_parquet_atomic_writes_to_target(tmp_path: Path) -> None:
    table = build_data_trust_snapshot()
    target = tmp_path / "mart_data_trust.parquet"
    write_parquet_atomic(target, table)
    assert target.is_file()
    assert not (tmp_path / ".mart_data_trust.parquet.tmp").exists()
    loaded = pq.read_table(target)
    assert loaded.num_rows == table.num_rows


def test_default_output_dir_points_at_evidence_data_directory() -> None:
    output_dir = default_output_dir()
    assert output_dir.name == "data"
    assert output_dir.parent.name == "sf_urban_health"
    assert output_dir.parent.parent.name == "sources"


def test_export_snapshots_honors_output_dir_override(tmp_path: Path) -> None:
    fake_cursor = MagicMock()
    fake_cursor.description = [("filed_month",), ("neighborhood",), ("permits_filed",)]
    fake_cursor.fetchall.return_value = [(None, "Mission", 3)]

    fake_connection = MagicMock()
    fake_connection.cursor.return_value = fake_cursor

    with (
        patch("scripts.evidence_snapshots.load_lakehouse_env"),
        patch("scripts.evidence_snapshots.require_local_lakehouse_stack"),
        patch("scripts.evidence_snapshots.open_spark_connection", return_value=fake_connection),
    ):
        counts = export_snapshots(tmp_path, skip_stack_check=True)

    assert counts["mart_housing_production.parquet"] == 1
    assert counts["mart_pipeline_health.parquet"] == 120
    assert counts["mart_data_trust.parquet"] == 3
    assert (tmp_path / "mart_housing_production.parquet").is_file()
    assert (tmp_path / "mart_pipeline_health.parquet").is_file()
    assert (tmp_path / "mart_data_trust.parquet").is_file()
    fake_cursor.execute.assert_called_once_with(housing_production_query())


def test_fetch_spark_table_raises_clear_error_on_query_failure() -> None:
    from scripts.evidence_snapshots import fetch_spark_table

    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError("table not found")

    with pytest.raises(EvidenceSnapshotsError, match="Spark query failed"):
        fetch_spark_table(cursor, housing_production_query())


def test_open_spark_connection_raises_clear_error_when_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DBT_SPARK_HOST", "127.0.0.1")
    monkeypatch.setenv("DBT_SPARK_PORT", "10000")

    with patch("pyhive.hive.Connection", side_effect=OSError("connection refused")):
        with pytest.raises(EvidenceSnapshotsError, match="Spark Thrift Server is not reachable"):
            open_spark_connection()


def test_main_reports_lakehouse_preflight_error_without_traceback(tmp_path: Path) -> None:
    with (
        patch("scripts.evidence_snapshots.load_lakehouse_env"),
        patch(
            "scripts.evidence_snapshots.require_local_lakehouse_stack",
            side_effect=LakehouseLocalError("Spark Thrift Server is not reachable"),
        ),
    ):
        with pytest.raises(SystemExit, match="Spark Thrift Server is not reachable"):
            main(["--output-dir", str(tmp_path)])
