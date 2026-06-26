"""Load and validate lakehouse table contracts from YAML.

Contracts under ``contracts/lakehouse/`` describe planned parquet table layouts
for future loaders, dbt models, and quality checks. They do not execute loads.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml

VALID_LAYERS = frozenset({"bronze", "silver", "gold", "metadata"})

BRONZE_METADATA_COLUMNS: tuple[dict[str, Any], ...] = (
    {
        "name": "_ingest_run_id",
        "type": "string",
        "nullable": False,
        "description": "Airflow run identifier for the extract that produced this row.",
    },
    {
        "name": "_raw_s3_path",
        "type": "string",
        "nullable": False,
        "description": "Full S3 URI of the source raw NDJSON object.",
    },
    {
        "name": "_raw_s3_key",
        "type": "string",
        "nullable": False,
        "description": "Object key of the source raw NDJSON object.",
    },
    {
        "name": "_data_interval_start",
        "type": "timestamp",
        "nullable": False,
        "description": "Airflow data interval start (UTC, inclusive).",
    },
    {
        "name": "_data_interval_end",
        "type": "timestamp",
        "nullable": False,
        "description": "Airflow data interval end (UTC, exclusive).",
    },
    {
        "name": "_effective_start",
        "type": "timestamp",
        "nullable": False,
        "description": "Half-open extract lower bound after configured lookback.",
    },
    {
        "name": "_loaded_at",
        "type": "timestamp",
        "nullable": False,
        "description": "Source data_loaded_at timestamp observed on the record.",
    },
    {
        "name": "_extracted_at",
        "type": "timestamp",
        "nullable": False,
        "description": "UTC timestamp when the extract task materialized this row.",
    },
    {
        "name": "_source_dataset_id",
        "type": "string",
        "nullable": False,
        "description": "DataSF SODA resource identifier for the dataset.",
    },
    {
        "name": "_record_hash",
        "type": "string",
        "nullable": False,
        "description": "Deterministic hash of the canonical raw JSON payload.",
    },
    {
        "name": "_raw_payload",
        "type": "string",
        "nullable": False,
        "description": "Source-faithful JSON object for the record.",
    },
)

BRONZE_NATURAL_KEYS: dict[str, str] = {
    "permits": "permit_number",
    "evictions": "eviction_id",
    "incidents": "row_id",
}

DOCUMENTED_GOLD_GRAINS: dict[str, tuple[str, ...]] = {
    "mart_housing_production": (
        "filed_month",
        "neighborhood",
        "supervisor_district",
        "use_transition",
    ),
    "mart_evictions": (
        "filed_month",
        "neighborhood",
        "supervisor_district",
        "eviction_type",
    ),
    "mart_public_safety": (
        "incident_month",
        "neighborhood",
        "supervisor_district",
        "police_district",
        "incident_category",
    ),
    "mart_permit_pipeline": (
        "neighborhood",
        "supervisor_district",
        "lifecycle_stage",
        "age_bucket",
    ),
}

PATH_LAYER_NAME_RE = re.compile(
    r"/lake/parquet/(?P<layer>bronze|silver|gold|metadata)/(?P<name>[a-z0-9_]+)/"
)


class ContractValidationError(ValueError):
    """Raised when a contract document fails registry validation."""


@dataclass(frozen=True)
class ColumnContract:
    name: str
    type: str
    nullable: bool
    description: str | None = None


@dataclass(frozen=True)
class QualityContract:
    checks: tuple[Mapping[str, Any], ...] = ()


@dataclass(frozen=True)
class CompatibilityContract:
    additive: bool = True
    version_policy: str = "semver"


@dataclass(frozen=True)
class TableContract:
    version: int
    layer: str
    name: str
    owner: str
    description: str
    grain: tuple[str, ...]
    path_template: str
    partition_columns: tuple[str, ...]
    columns: tuple[ColumnContract, ...]
    quality: QualityContract
    compatibility: CompatibilityContract
    natural_key: tuple[str, ...] = ()
    table_format: str = "parquet"
    catalog_schema: str | None = None
    catalog_name: str | None = None
    source_path: Path | None = None

    @property
    def column_names(self) -> frozenset[str]:
        return frozenset(column.name for column in self.columns)

    @property
    def catalog_relation(self) -> str | None:
        if self.catalog_schema and self.catalog_name:
            return f"{self.catalog_schema}.{self.catalog_name}"
        return None


@dataclass
class ContractRegistry:
    contracts: dict[tuple[str, str], TableContract] = field(default_factory=dict)

    def get(self, layer: str, name: str) -> TableContract:
        try:
            return self.contracts[(layer, name)]
        except KeyError as exc:
            raise KeyError(f"unknown contract: layer={layer!r} name={name!r}") from exc

    def by_layer(self, layer: str) -> tuple[TableContract, ...]:
        return tuple(
            contract
            for (contract_layer, _), contract in sorted(self.contracts.items())
            if contract_layer == layer
        )


def _repo_root() -> Path:
    current = Path(__file__).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "contracts" / "lakehouse").is_dir():
            return candidate
    raise FileNotFoundError("could not locate contracts/lakehouse from module path")


def _default_contract_root() -> Path:
    return _repo_root() / "contracts" / "lakehouse"


def _require_bool(value: Any, *, source: str, field: str) -> bool:
    if not isinstance(value, bool):
        raise ContractValidationError(
            f"{source}: {field} must be a boolean, got {type(value).__name__}"
        )
    return value


def _require_list(value: Any, *, source: str, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise ContractValidationError(
            f"{source}: {field} must be a list, got {type(value).__name__}"
        )
    return value


def _require_mapping(value: Any, *, source: str, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ContractValidationError(
            f"{source}: {field} must be a mapping, got {type(value).__name__}"
        )
    return value


def _parse_column(raw: Any, *, source: str) -> ColumnContract:
    column = _require_mapping(raw, source=source, field="column")
    missing = [key for key in ("name", "type", "nullable") if key not in column]
    if missing:
        raise ContractValidationError(
            f"{source}: column missing required fields: {', '.join(missing)}"
        )
    return ColumnContract(
        name=str(column["name"]),
        type=str(column["type"]),
        nullable=_require_bool(column["nullable"], source=source, field="nullable"),
        description=str(column["description"]) if column.get("description") is not None else None,
    )


def _parse_contract(raw: Mapping[str, Any], *, source: Path) -> TableContract:
    table_format = str(raw.get("table_format", "parquet"))
    required = (
        "version",
        "layer",
        "name",
        "owner",
        "description",
        "grain",
        "partition_columns",
        "columns",
        "quality",
        "compatibility",
    )
    if table_format != "iceberg":
        required = (*required, "path_template")
    missing = [key for key in required if key not in raw]
    if missing:
        raise ContractValidationError(
            f"{source}: contract missing required fields: {', '.join(missing)}"
        )

    grain = _require_list(raw["grain"], source=str(source), field="grain")
    partition_columns = _require_list(
        raw["partition_columns"],
        source=str(source),
        field="partition_columns",
    )
    columns_raw = _require_list(raw["columns"], source=str(source), field="columns")
    if "natural_key" in raw:
        natural_key_raw = _require_list(
            raw["natural_key"],
            source=str(source),
            field="natural_key",
        )
    else:
        natural_key_raw = []

    columns = tuple(
        _parse_column(column, source=f"{source}:columns[{index}]")
        for index, column in enumerate(columns_raw)
    )
    quality_raw = _require_mapping(raw["quality"], source=str(source), field="quality")
    compatibility_raw = _require_mapping(
        raw["compatibility"],
        source=str(source),
        field="compatibility",
    )

    return TableContract(
        version=int(raw["version"]),
        layer=str(raw["layer"]),
        name=str(raw["name"]),
        owner=str(raw["owner"]),
        description=str(raw["description"]),
        grain=tuple(str(value) for value in grain),
        path_template=str(raw.get("path_template", "")),
        partition_columns=tuple(str(value) for value in partition_columns),
        columns=columns,
        quality=QualityContract(
            checks=tuple(
                dict(check)
                for check in _require_list(
                    quality_raw.get("checks", []),
                    source=f"{source}:quality.checks",
                    field="quality.checks",
                )
            )
        ),
        compatibility=CompatibilityContract(
            additive=_require_bool(
                compatibility_raw.get("additive", True),
                source=str(source),
                field="compatibility.additive",
            )
            if "additive" in compatibility_raw
            else True,
            version_policy=str(compatibility_raw.get("version_policy", "semver")),
        ),
        natural_key=tuple(str(value) for value in natural_key_raw),
        table_format=table_format,
        catalog_schema=str(raw["catalog_schema"]) if raw.get("catalog_schema") else None,
        catalog_name=str(raw["catalog_name"]) if raw.get("catalog_name") else None,
        source_path=source,
    )


def _scan_contract_paths(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*.yml") if path.is_file())


def _assert_path_template(contract: TableContract) -> None:
    if contract.table_format == "iceberg":
        return
    normalized = contract.path_template if contract.path_template.startswith("/") else f"/{contract.path_template}"
    match = PATH_LAYER_NAME_RE.search(normalized.replace("\\", "/"))
    if match is None:
        raise ContractValidationError(
            f"{contract.source_path}: path_template must include "
            f"/lake/parquet/{{layer}}/{{name}}/ semantics"
        )
    if match.group("layer") != contract.layer or match.group("name") != contract.name:
        raise ContractValidationError(
            f"{contract.source_path}: path_template layer/name "
            f"({match.group('layer')}/{match.group('name')}) "
            f"does not match contract ({contract.layer}/{contract.name})"
        )


def _assert_iceberg_catalog(contract: TableContract) -> None:
    if contract.table_format != "iceberg":
        return
    if not contract.catalog_schema or not contract.catalog_name:
        raise ContractValidationError(
            f"{contract.source_path}: iceberg contracts require catalog_schema and catalog_name"
        )
    if contract.catalog_name != contract.name:
        raise ContractValidationError(
            f"{contract.source_path}: catalog_name must match contract name "
            f"({contract.catalog_name!r} != {contract.name!r})"
        )


def _assert_bronze_metadata(contract: TableContract) -> None:
    by_name = {column.name: column for column in contract.columns}
    for required in BRONZE_METADATA_COLUMNS:
        name = required["name"]
        if name not in by_name:
            raise ContractValidationError(
                f"{contract.source_path}: bronze contract missing metadata column {name!r}"
            )
        actual = by_name[name]
        if actual.type != required["type"]:
            raise ContractValidationError(
                f"{contract.source_path}: bronze metadata column {name!r} "
                f"expected type {required['type']!r}, got {actual.type!r}"
            )
        if actual.nullable:
            raise ContractValidationError(
                f"{contract.source_path}: bronze metadata column {name!r} must be non-nullable"
            )


def _assert_bronze_natural_key(contract: TableContract) -> None:
    expected = BRONZE_NATURAL_KEYS.get(contract.name)
    if expected is None:
        raise ContractValidationError(
            f"{contract.source_path}: unknown bronze table {contract.name!r}"
        )
    if contract.natural_key != (expected,):
        raise ContractValidationError(
            f"{contract.source_path}: bronze natural_key must be [{expected!r}], "
            f"got {list(contract.natural_key)!r}"
        )
    key_column = next((column for column in contract.columns if column.name == expected), None)
    if key_column is None:
        raise ContractValidationError(
            f"{contract.source_path}: bronze contract missing natural key column {expected!r}"
        )
    if key_column.nullable:
        raise ContractValidationError(
            f"{contract.source_path}: bronze natural key column {expected!r} must be non-nullable"
        )


def _assert_gold_grain(contract: TableContract) -> None:
    expected = DOCUMENTED_GOLD_GRAINS.get(contract.name)
    if expected is None:
        return
    if contract.grain != expected:
        raise ContractValidationError(
            f"{contract.source_path}: gold grain {list(contract.grain)!r} "
            f"does not match documented mart grain {list(expected)!r}"
        )


def validate_contract(contract: TableContract) -> None:
    """Fail fast when a single contract violates registry rules."""

    if contract.layer not in VALID_LAYERS:
        raise ContractValidationError(
            f"{contract.source_path}: invalid layer {contract.layer!r}"
        )
    if not contract.columns:
        raise ContractValidationError(f"{contract.source_path}: contract must declare columns")
    if len(contract.column_names) != len(contract.columns):
        raise ContractValidationError(
            f"{contract.source_path}: duplicate column names are not allowed"
        )
    _assert_path_template(contract)
    _assert_iceberg_catalog(contract)
    if contract.layer == "bronze":
        _assert_bronze_metadata(contract)
        _assert_bronze_natural_key(contract)
    if contract.layer == "gold":
        _assert_gold_grain(contract)


def validate_registry(registry: ContractRegistry) -> None:
    """Validate the full registry, including uniqueness constraints."""

    seen: set[tuple[str, str]] = set()
    for key, contract in registry.contracts.items():
        if key in seen:
            raise ContractValidationError(f"duplicate contract key {key!r}")
        seen.add(key)
        if (contract.layer, contract.name) != key:
            raise ContractValidationError(
                f"{contract.source_path}: registry key {key!r} "
                f"does not match contract layer/name"
            )
        validate_contract(contract)


def load_contracts(root: Path | None = None) -> ContractRegistry:
    """Load every YAML contract under ``root`` and validate the registry."""

    contract_root = root or _default_contract_root()
    if not contract_root.is_dir():
        raise FileNotFoundError(f"contract root not found: {contract_root}")

    registry = ContractRegistry()
    for path in _scan_contract_paths(contract_root):
        raw = yaml.safe_load(path.read_text())
        if not isinstance(raw, Mapping):
            raise ContractValidationError(f"{path}: contract root must be a mapping")
        contract = _parse_contract(raw, source=path)
        key = (contract.layer, contract.name)
        if key in registry.contracts:
            raise ContractValidationError(
                f"duplicate contract for layer={contract.layer!r} name={contract.name!r}"
            )
        registry.contracts[key] = contract

    validate_registry(registry)
    return registry


def get_contract(layer: str, name: str, root: Path | None = None) -> TableContract:
    """Return one validated contract by layer and logical table name."""

    return load_contracts(root).get(layer, name)


def table_prefix(bucket: str, contract: TableContract) -> str:
    """Return the stable S3 prefix for a contract, excluding partition segments."""

    bucket = bucket.strip("/")
    return f"s3://{bucket}/lake/parquet/{contract.layer}/{contract.name}/"


def bronze_contracts(root: Path | None = None) -> tuple[TableContract, ...]:
    return load_contracts(root).by_layer("bronze")


def gold_contracts(root: Path | None = None) -> tuple[TableContract, ...]:
    return load_contracts(root).by_layer("gold")


def silver_contracts(root: Path | None = None) -> tuple[TableContract, ...]:
    return load_contracts(root).by_layer("silver")


def metadata_contracts(root: Path | None = None) -> tuple[TableContract, ...]:
    return load_contracts(root).by_layer("metadata")


def render_path_template(
    contract: TableContract,
    *,
    bucket: str,
    partitions: Mapping[str, str] | None = None,
) -> str:
    """Substitute partition placeholders and return a full S3 URI prefix."""

    rendered = contract.path_template
    for key, value in (partitions or {}).items():
        rendered = rendered.replace(f"{{{key}}}", value)
    bucket = bucket.strip("/")
    return f"s3://{bucket}/{rendered.strip('/')}"


def contracts_as_dicts(registry: ContractRegistry) -> list[dict[str, Any]]:
    """Serialize loaded contracts for debugging or downstream tooling."""

    payload: list[dict[str, Any]] = []
    for contract in registry.contracts.values():
        payload.append(
            {
                "version": contract.version,
                "layer": contract.layer,
                "name": contract.name,
                "owner": contract.owner,
                "grain": list(contract.grain),
                "path_template": contract.path_template,
                "table_format": contract.table_format,
                "catalog_schema": contract.catalog_schema,
                "catalog_name": contract.catalog_name,
                "natural_key": list(contract.natural_key),
                "columns": [
                    {
                        "name": column.name,
                        "type": column.type,
                        "nullable": column.nullable,
                        "description": column.description,
                    }
                    for column in contract.columns
                ],
            }
        )
    return payload
