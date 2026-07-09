from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
import yaml

from scripts import lakehouse_contracts as lc


@pytest.fixture
def contract_root() -> Path:
    return Path(__file__).resolve().parents[1] / "contracts" / "lakehouse"


def test_all_contracts_load(contract_root: Path) -> None:
    registry = lc.load_contracts(contract_root)
    assert len(registry.contracts) == 14


def test_logical_table_names_are_unique(contract_root: Path) -> None:
    registry = lc.load_contracts(contract_root)
    keys = list(registry.contracts)
    assert len(keys) == len(set(keys))


def test_bronze_metadata_columns_present_and_non_nullable(contract_root: Path) -> None:
    required_names = {column["name"] for column in lc.BRONZE_METADATA_COLUMNS}
    for contract in lc.bronze_contracts(contract_root):
        present = {column.name for column in contract.columns}
        assert required_names.issubset(present)
        for column in contract.columns:
            if column.name in required_names:
                assert column.nullable is False


def test_bronze_natural_keys_present(contract_root: Path) -> None:
    for contract in lc.bronze_contracts(contract_root):
        expected = lc.BRONZE_NATURAL_KEYS[contract.name]
        assert contract.natural_key == (expected,)
        key_column = next(
            column for column in contract.columns if column.name == expected
        )
        assert key_column.nullable is False


def test_table_prefix_returns_stable_layer_path(contract_root: Path) -> None:
    bucket = "sf-urban-health"
    for contract in lc.load_contracts(contract_root).contracts.values():
        prefix = lc.table_prefix(bucket, contract)
        assert prefix == f"s3://{bucket}/lake/parquet/{contract.layer}/{contract.name}/"


def test_render_path_template_substitutes_partitions(contract_root: Path) -> None:
    contract = lc.get_contract("bronze", "permits", contract_root)
    rendered = lc.render_path_template(
        contract,
        bucket="sf-urban-health",
        partitions={
            "data_interval_start": "20240101T060000Z",
            "data_interval_end": "20240102T060000Z",
        },
    )
    assert rendered == (
        "s3://sf-urban-health/lake/parquet/bronze/permits/"
        "data_interval_start=20240101T060000Z/data_interval_end=20240102T060000Z"
    )


def test_gold_grains_match_documentation(contract_root: Path) -> None:
    for contract in lc.gold_contracts(contract_root):
        assert contract.grain == lc.DOCUMENTED_GOLD_GRAINS[contract.name]


def test_get_contract_returns_single_table(contract_root: Path) -> None:
    contract = lc.get_contract("silver", "permits_current", contract_root)
    assert contract.name == "permits_current"
    assert contract.layer == "silver"
    assert contract.table_format == "iceberg"
    assert contract.catalog_relation == "sf_urban_health.permits_current"
    assert contract.path_template == ""


def test_permits_current_contract_nullable_lifecycle_fields(
    contract_root: Path,
) -> None:
    contract = lc.get_contract("silver", "permits_current", contract_root)
    by_name = {column.name: column for column in contract.columns}
    assert by_name["current_status"].nullable is True
    assert by_name["filed_at"].nullable is True
    assert by_name["completed_at"].nullable is True
    assert by_name["permit_number"].nullable is False
    assert by_name["_loaded_at"].nullable is False


def test_bronze_and_gold_helpers(contract_root: Path) -> None:
    bronze = lc.bronze_contracts(contract_root)
    gold = lc.gold_contracts(contract_root)
    assert {contract.name for contract in bronze} == {
        "permits",
        "evictions",
        "incidents",
    }
    assert {contract.name for contract in gold} == {
        "housing_production",
        "permit_pipeline",
        "evictions",
        "public_safety",
        "pipeline_health",
        "data_trust",
    }


@pytest.mark.parametrize(
    ("fixture_name", "expected_error"),
    [
        ("duplicate_layer_name.yml", "duplicate contract"),
        ("missing_bronze_metadata.yml", "missing metadata column"),
        ("wrong_natural_key.yml", "natural_key must be"),
        ("invalid_layer.yml", "invalid layer"),
        ("bad_path_template.yml", "path_template must include"),
        ("empty_columns.yml", "must declare columns"),
        ("wrong_gold_grain.yml", "does not match documented mart grain"),
        ("string_nullable.yml", "nullable must be a boolean"),
        ("scalar_grain.yml", "grain must be a list"),
        ("scalar_columns.yml", "columns must be a list"),
        ("non_mapping_column.yml", "column must be a mapping"),
    ],
)
def test_invalid_sample_contracts_fail_validation(
    tmp_path: Path,
    fixture_name: str,
    expected_error: str,
) -> None:
    if fixture_name == "duplicate_layer_name.yml":
        sample_a = _invalid_contract("valid_bronze.yml")
        sample_b = _invalid_contract("valid_bronze.yml")
        (tmp_path / "bronze").mkdir(parents=True, exist_ok=True)
        (tmp_path / "bronze" / "permits_a.yml").write_text(sample_a)
        (tmp_path / "bronze" / "permits_b.yml").write_text(sample_b)
    else:
        sample = _invalid_contract(fixture_name)
        path = tmp_path / fixture_name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(sample)
    with pytest.raises(lc.ContractValidationError, match=expected_error):
        lc.load_contracts(tmp_path)


def _invalid_contract(name: str) -> str:
    valid_bronze = textwrap.dedent(
        """
        version: 1
        layer: bronze
        name: permits
        owner: data-platform
        description: sample bronze contract
        grain:
          - permit_number
        path_template: lake/parquet/bronze/permits/data_interval_start={data_interval_start}/
        partition_columns:
          - data_interval_start
        natural_key:
          - permit_number
        columns:
          - name: _ingest_run_id
            type: string
            nullable: false
          - name: _raw_s3_path
            type: string
            nullable: false
          - name: _raw_s3_key
            type: string
            nullable: false
          - name: _data_interval_start
            type: timestamp
            nullable: false
          - name: _data_interval_end
            type: timestamp
            nullable: false
          - name: _effective_start
            type: timestamp
            nullable: false
          - name: _loaded_at
            type: timestamp
            nullable: false
          - name: _extracted_at
            type: timestamp
            nullable: false
          - name: _source_dataset_id
            type: string
            nullable: false
          - name: _record_hash
            type: string
            nullable: false
          - name: _raw_payload
            type: string
            nullable: false
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

    valid_gold = textwrap.dedent(
        """
        version: 1
        layer: gold
        name: housing_production
        owner: analytics-engineering
        description: sample gold contract
        table_format: iceberg
        catalog_schema: sf_urban_health
        catalog_name: housing_production
        grain:
          - filed_month
          - neighborhood
          - supervisor_district
          - use_transition
        partition_columns: []
        columns:
          - name: filed_month
            type: date
            nullable: false
        quality:
          checks: []
        compatibility:
          additive: true
          version_policy: semver
        """
    ).strip()

    if name in {"valid_bronze.yml", "duplicate_layer_name.yml"}:
        return valid_bronze

    if name == "missing_bronze_metadata.yml":
        payload = yaml.safe_load(valid_bronze)
        payload["columns"] = [
            column for column in payload["columns"] if column["name"] != "_record_hash"
        ]
        return yaml.safe_dump(payload, sort_keys=False)

    if name == "wrong_natural_key.yml":
        payload = yaml.safe_load(valid_bronze)
        payload["natural_key"] = ["row_id"]
        return yaml.safe_dump(payload, sort_keys=False)

    if name == "invalid_layer.yml":
        payload = yaml.safe_load(valid_bronze)
        payload["layer"] = "platinum"
        return yaml.safe_dump(payload, sort_keys=False)

    if name == "bad_path_template.yml":
        payload = yaml.safe_load(valid_bronze)
        payload["path_template"] = "lake/raw/permits/"
        return yaml.safe_dump(payload, sort_keys=False)

    if name == "empty_columns.yml":
        payload = yaml.safe_load(valid_bronze)
        payload["columns"] = []
        return yaml.safe_dump(payload, sort_keys=False)

    if name == "wrong_gold_grain.yml":
        payload = yaml.safe_load(valid_gold)
        payload["grain"] = ["filed_month"]
        return yaml.safe_dump(payload, sort_keys=False)

    if name == "string_nullable.yml":
        payload = yaml.safe_load(valid_bronze)
        payload["columns"][0]["nullable"] = "false"
        return yaml.safe_dump(payload, sort_keys=False)

    if name == "scalar_grain.yml":
        payload = yaml.safe_load(valid_bronze)
        payload["grain"] = "permit_number"
        return yaml.safe_dump(payload, sort_keys=False)

    if name == "scalar_columns.yml":
        payload = yaml.safe_load(valid_bronze)
        payload["columns"] = "permit_number"
        return yaml.safe_dump(payload, sort_keys=False)

    if name == "non_mapping_column.yml":
        payload = yaml.safe_load(valid_bronze)
        payload["columns"] = ["permit_number"]
        return yaml.safe_dump(payload, sort_keys=False)

    raise AssertionError(f"unknown invalid fixture: {name}")
