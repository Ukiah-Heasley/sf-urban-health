from __future__ import annotations

import textwrap
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from scripts import lakehouse_contracts as lc
from scripts.lakehouse_load import (
    build_s3_client,
    local_minio_endpoint_url,
    s3_endpoint_url,
    storage_from_lakehouse_env,
)
from scripts.lakehouse_local import (
    LakehouseLocalError,
    require_local_lakehouse_stack,
    reset_lakehouse_bucket,
)


def test_s3_endpoint_url_prefers_aws_endpoint_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.delenv("AWS_S3_ENDPOINT_URL", raising=False)
    assert s3_endpoint_url() == "http://minio:9000"


def test_s3_endpoint_url_falls_back_to_aws_s3_endpoint_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("AWS_S3_ENDPOINT_URL", "http://localhost:9000")
    assert s3_endpoint_url() == "http://localhost:9000"


def test_build_s3_client_passes_endpoint_url(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_client(service_name: str, **kwargs):
        captured["service_name"] = service_name
        captured["kwargs"] = kwargs
        return MagicMock()

    monkeypatch.setitem(__import__("sys").modules, "boto3", MagicMock(client=_fake_client))
    build_s3_client(endpoint_url="http://localhost:9000")
    assert captured["service_name"] == "s3"
    assert captured["kwargs"]["endpoint_url"] == "http://localhost:9000"


def test_local_minio_endpoint_url_uses_host_and_port(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_S3_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("MINIO_HOST", "127.0.0.1")
    monkeypatch.setenv("MINIO_API_PORT", "9010")
    assert local_minio_endpoint_url() == "http://127.0.0.1:9010"


def test_local_minio_endpoint_url_honors_explicit_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://override:9000")
    monkeypatch.setenv("MINIO_HOST", "127.0.0.1")
    monkeypatch.setenv("MINIO_API_PORT", "9010")
    assert local_minio_endpoint_url() == "http://override:9000"


def test_storage_from_lakehouse_env_uses_minio_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LAKE_LOCAL_ROOT", raising=False)
    monkeypatch.delenv("AWS_S3_BUCKET", raising=False)
    monkeypatch.setenv("LAKEHOUSE_BUCKET", "lakehouse")
    monkeypatch.setenv("MINIO_ROOT_USER", "minioadmin")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "minioadmin")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_S3_ENDPOINT_URL", raising=False)

    fake_client = MagicMock()
    with patch("scripts.lakehouse_load.build_s3_client", return_value=fake_client) as builder:
        storage = storage_from_lakehouse_env()

    builder.assert_called_once_with(
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
    assert storage.bucket == "lakehouse"
    assert storage.s3_client is fake_client


def test_storage_from_lakehouse_env_uses_minio_api_port(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LAKE_LOCAL_ROOT", raising=False)
    monkeypatch.delenv("AWS_S3_BUCKET", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_S3_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("MINIO_HOST", "127.0.0.1")
    monkeypatch.setenv("MINIO_API_PORT", "9010")

    fake_client = MagicMock()
    with patch("scripts.lakehouse_load.build_s3_client", return_value=fake_client) as builder:
        storage_from_lakehouse_env()

    builder.assert_called_once_with(
        endpoint_url="http://127.0.0.1:9010",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )


def test_max_loaded_at_from_fixture_is_within_interval() -> None:
    from scripts.lakehouse_fixture_prepare import (
        _INTERVAL_END,
        _INTERVAL_START,
        _max_loaded_at_from_fixture,
    )

    fixture_path = (
        Path(__file__).resolve().parent / "fixtures" / "lakehouse" / "permits.ndjson"
    )
    max_loaded_at = _max_loaded_at_from_fixture(fixture_path)
    assert _INTERVAL_START <= max_loaded_at < _INTERVAL_END
    assert max_loaded_at == datetime(2024, 3, 16, 5, 0, tzinfo=timezone.utc)


def test_require_local_lakehouse_stack_probes_resolved_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9010")
    monkeypatch.delenv("MINIO_API_PORT", raising=False)
    monkeypatch.delenv("AWS_S3_ENDPOINT_URL", raising=False)

    probed: list[tuple[str, int]] = []
    fake_storage = MagicMock()
    fake_storage.bucket = "lakehouse"
    fake_storage.s3_client = MagicMock()
    fake_storage.s3_client.head_bucket.return_value = {}

    def _record_probe(host: str, port: int, **kwargs) -> bool:
        probed.append((host, port))
        return True

    with (
        patch("scripts.lakehouse_local.storage_from_lakehouse_env", return_value=fake_storage),
        patch("scripts.lakehouse_local._port_open", side_effect=_record_probe),
        patch("scripts.lakehouse_local.load_lakehouse_env"),
    ):
        require_local_lakehouse_stack()

    assert probed[0] == ("localhost", 9010)
    assert (probed[0][0], probed[0][1]) != ("localhost", 9000)


def test_require_local_lakehouse_stack_reports_resolved_endpoint_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9010")
    monkeypatch.delenv("MINIO_API_PORT", raising=False)

    fake_storage = MagicMock()
    fake_storage.bucket = "lakehouse"
    fake_storage.s3_client = MagicMock()

    with (
        patch("scripts.lakehouse_local.storage_from_lakehouse_env", return_value=fake_storage),
        patch("scripts.lakehouse_local._port_open", return_value=False),
        patch("scripts.lakehouse_local.load_lakehouse_env"),
    ):
        with pytest.raises(LakehouseLocalError, match="http://localhost:9010"):
            require_local_lakehouse_stack()


def test_reset_lakehouse_bucket_deletes_all_objects() -> None:
    client = MagicMock()
    client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "raw/permits/records.ndjson"},
            {"Key": "lake/parquet/bronze/permits/records.parquet"},
        ],
        "IsTruncated": False,
    }
    client.delete_objects.return_value = {
        "Deleted": [
            {"Key": "raw/permits/records.ndjson"},
            {"Key": "lake/parquet/bronze/permits/records.parquet"},
        ],
        "Errors": [],
    }
    from scripts.lakehouse_load import StorageConfig

    storage = StorageConfig(bucket="lakehouse", local_root=None, s3_client=client)
    deleted = reset_lakehouse_bucket(storage)
    assert deleted == 2
    client.delete_objects.assert_called_once()


def test_iceberg_contract_without_catalog_fields_fails(tmp_path) -> None:
    payload = yaml.safe_load(
        textwrap.dedent(
            """
            version: 1
            layer: silver
            name: permits_current
            owner: analytics-engineering
            description: sample iceberg contract
            table_format: iceberg
            grain:
              - permit_number
            partition_columns: []
            columns:
              - name: permit_number
                type: string
                nullable: false
            quality:
              checks: []
            compatibility:
              additive: true
              version_policy: semver
            """
        ).strip()
    )
    path = tmp_path / "permits_current.yml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False))
    with pytest.raises(lc.ContractValidationError, match="catalog_schema and catalog_name"):
        lc.load_contracts(tmp_path)
