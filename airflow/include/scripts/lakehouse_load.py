"""Promote raw S3 NDJSON extracts into typed bronze Parquet.

Reads one raw interval object per dataset, materializes contract-aligned bronze
Parquet under ``lake/parquet/bronze/``, and writes immutable file-manifest
metadata events to S3. Metadata compaction is handled separately.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterator, Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.lakehouse_contracts import (
    TableContract,
    get_contract,
    render_path_template,
)
from scripts.time_utils import coerce_utc_datetime

if TYPE_CHECKING:
    from scripts.lakehouse_metadata import IngestRunEvent

DEFAULT_PROMOTION_BATCH_SIZE = 1000


class LakehouseLoadError(RuntimeError):
    """Raised when promotion fails validation."""


@dataclass(frozen=True)
class StorageConfig:
    """Object storage backend for raw reads and lake writes."""

    bucket: str | None
    local_root: Path | None
    s3_client: Any | None = None

    def uri_for_key(self, key: str) -> str:
        normalized = key.lstrip("/")
        if self.local_root is not None:
            return str((self.local_root / normalized).resolve())
        if not self.bucket:
            raise LakehouseLoadError("bucket must be set when local_root is unset")
        return f"s3://{self.bucket}/{normalized}"

    def write_bytes(self, key: str, payload: bytes) -> int:
        normalized = key.lstrip("/")
        if self.local_root is not None:
            path = self.local_root / normalized
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
            return len(payload)
        if self.s3_client is None or not self.bucket:
            raise LakehouseLoadError("S3 client and bucket are required for remote writes")
        self.s3_client.put_object(Bucket=self.bucket, Key=normalized, Body=payload)
        return len(payload)

    def read_bytes(self, key: str) -> bytes:
        normalized = key.lstrip("/")
        if self.local_root is not None:
            return (self.local_root / normalized).read_bytes()
        if self.s3_client is None or not self.bucket:
            raise LakehouseLoadError("S3 client and bucket are required for remote reads")
        response = self.s3_client.get_object(Bucket=self.bucket, Key=normalized)
        return response["Body"].read()

    def read_text_lines(self, key: str) -> Iterator[str]:
        normalized = key.lstrip("/")
        if self.local_root is not None:
            with (self.local_root / normalized).open("rb") as handle:
                for raw_line in handle:
                    yield raw_line.decode("utf-8")
            return
        if self.s3_client is None or not self.bucket:
            raise LakehouseLoadError("S3 client and bucket are required for remote reads")
        response = self.s3_client.get_object(Bucket=self.bucket, Key=normalized)
        for raw_line in response["Body"].iter_lines():
            yield raw_line.decode("utf-8") if isinstance(raw_line, bytes) else raw_line

    def write_file(self, key: str, source_path: Path) -> int:
        normalized = key.lstrip("/")
        if self.local_root is not None:
            destination = self.local_root / normalized
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_path, destination)
            return destination.stat().st_size
        if self.s3_client is None or not self.bucket:
            raise LakehouseLoadError("S3 client and bucket are required for remote writes")
        self.s3_client.upload_file(str(source_path), self.bucket, normalized)
        return source_path.stat().st_size

    def list_keys(self, prefix: str) -> list[str]:
        normalized = prefix.lstrip("/")
        if self.local_root is not None:
            root = self.local_root / normalized
            if not root.exists():
                return []
            return sorted(
                str(path.relative_to(self.local_root)).replace("\\", "/")
                for path in root.rglob("*")
                if path.is_file()
            )
        if self.s3_client is None or not self.bucket:
            raise LakehouseLoadError("S3 client and bucket are required for remote listing")
        keys: list[str] = []
        continuation: str | None = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": normalized}
            if continuation:
                kwargs["ContinuationToken"] = continuation
            response = self.s3_client.list_objects_v2(**kwargs)
            for item in response.get("Contents", []):
                keys.append(str(item["Key"]))
            if not response.get("IsTruncated"):
                break
            continuation = response.get("NextContinuationToken")
        return sorted(keys)

    def delete_prefix(self, prefix: str) -> None:
        normalized = prefix.lstrip("/")
        if self.local_root is not None:
            target = self.local_root / normalized.rstrip("/")
            if target.exists():
                shutil.rmtree(target)
            return
        if self.s3_client is None or not self.bucket:
            raise LakehouseLoadError("S3 client and bucket are required for remote deletes")
        keys = self.list_keys(normalized)
        for index in range(0, len(keys), 1000):
            batch = [{"Key": key} for key in keys[index : index + 1000]]
            if not batch:
                continue
            response = self.s3_client.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": batch},
            )
            errors = response.get("Errors", [])
            if errors:
                first = errors[0]
                failed_key = str(first.get("Key", "unknown"))
                error_code = str(first.get("Code", ""))
                error_message = str(first.get("Message", ""))
                suffix = f" and {len(errors) - 1} more failed keys" if len(errors) > 1 else ""
                raise LakehouseLoadError(
                    f"failed to delete objects under prefix {normalized!r}: "
                    f"key {failed_key!r} failed with {error_code!r} ({error_message!r}){suffix}"
                )


@dataclass(frozen=True)
class PromoteResult:
    bronze_path: str | None
    bronze_key: str | None
    records_promoted: int
    file_size_bytes: int
    content_hash: str | None
    manifest_id: str | None
    manifest_event_key: str | None


_FIELD_MAPPERS: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {}


def _register_mapper(dataset_name: str):
    def decorator(func: Callable[[dict[str, Any]], dict[str, Any]]):
        _FIELD_MAPPERS[dataset_name] = func
        return func

    return decorator


def _parse_optional_timestamp(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    return coerce_utc_datetime(str(value))


def _parse_optional_date(value: object) -> date | None:
    if value is None or value == "":
        return None
    text = str(value)
    if "T" in text:
        return coerce_utc_datetime(text).date()
    return date.fromisoformat(text[:10])


def _parse_optional_bool(value: object) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    lowered = str(value).strip().lower()
    if lowered in {"true", "t", "1", "yes"}:
        return True
    if lowered in {"false", "f", "0", "no"}:
        return False
    return None


def _parse_optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _parse_optional_decimal(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value))


def _parse_optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _parse_optional_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text != "" else None


def _parse_loaded_at(payload: Mapping[str, Any], *, dataset_name: str) -> datetime:
    value = payload.get("data_loaded_at")
    if value is None or value == "":
        raise LakehouseLoadError(
            f"{dataset_name} record is missing required source timestamp data_loaded_at"
        )
    try:
        return coerce_utc_datetime(str(value))
    except (TypeError, ValueError) as exc:
        raise LakehouseLoadError(
            f"{dataset_name} record has invalid data_loaded_at: {value!r}"
        ) from exc


def canonical_record_hash(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def source_faithful_payload(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"))


def format_interval_partition(value: datetime) -> str:
    return coerce_utc_datetime(value).strftime("%Y%m%dT%H%M%SZ")


def bronze_object_key(
    contract: TableContract,
    *,
    data_interval_start: datetime,
    data_interval_end: datetime,
) -> str:
    prefix = render_path_template(
        contract,
        bucket="",
        partitions={
            "data_interval_start": format_interval_partition(data_interval_start),
            "data_interval_end": format_interval_partition(data_interval_end),
        },
    )
    key = prefix.removeprefix("s3:///").lstrip("/")
    return f"{key}/records.parquet"


def contract_type_to_pyarrow(type_name: str) -> pa.DataType:
    mapping = {
        "string": pa.string(),
        "timestamp": pa.timestamp("us", tz="UTC"),
        "date": pa.date32(),
        "boolean": pa.bool_(),
        "integer": pa.int64(),
        "decimal": pa.decimal128(18, 2),
        "double": pa.float64(),
    }
    try:
        return mapping[type_name]
    except KeyError as exc:
        raise LakehouseLoadError(f"unsupported contract column type: {type_name!r}") from exc


def pyarrow_schema_for_contract(contract: TableContract) -> pa.Schema:
    return pa.schema(
        [
            pa.field(column.name, contract_type_to_pyarrow(column.type), nullable=column.nullable)
            for column in contract.columns
        ]
    )


@_register_mapper("permits")
def _map_permits_fields(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "permit_number": _parse_optional_string(payload.get("permit_number")),
        "permit_type_code": _parse_optional_string(payload.get("permit_type")),
        "permit_type": _parse_optional_string(payload.get("permit_type_definition")),
        "status": _parse_optional_string(payload.get("status")),
        "filed_at": _parse_optional_timestamp(payload.get("filed_date")),
        "issued_at": _parse_optional_timestamp(payload.get("issued_date")),
        "status_date": _parse_optional_timestamp(payload.get("status_date")),
        "approved_at": _parse_optional_timestamp(payload.get("approved_date")),
        "last_activity_at": _parse_optional_timestamp(payload.get("last_permit_activity_date")),
        "is_adu": _parse_optional_bool(payload.get("adu")),
        "estimated_cost": _parse_optional_decimal(payload.get("estimated_cost")),
        "revised_cost": _parse_optional_decimal(payload.get("revised_cost")),
        "existing_units": _parse_optional_int(payload.get("existing_units")),
        "proposed_units": _parse_optional_int(payload.get("proposed_units")),
        "existing_stories": _parse_optional_int(payload.get("number_of_existing_stories")),
        "proposed_stories": _parse_optional_int(payload.get("number_of_proposed_stories")),
        "street_number": _parse_optional_string(payload.get("street_number")),
        "street_name": _parse_optional_string(payload.get("street_name")),
        "zipcode": _parse_optional_string(payload.get("zipcode")),
        "supervisor_district": _parse_optional_string(payload.get("supervisor_district")),
        "neighborhood": _parse_optional_string(payload.get("neighborhoods_analysis_boundaries")),
        "existing_use": _parse_optional_string(payload.get("existing_use")),
        "proposed_use": _parse_optional_string(payload.get("proposed_use")),
    }


@_register_mapper("evictions")
def _map_evictions_fields(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "eviction_id": _parse_optional_string(payload.get("eviction_id")),
        "filed_at": _parse_optional_date(payload.get("file_date")),
        "address": _parse_optional_string(payload.get("address")),
        "zipcode": _parse_optional_string(payload.get("zip")),
        "supervisor_district": _parse_optional_string(payload.get("supervisor_district")),
        "neighborhood": _parse_optional_string(payload.get("neighborhood")),
        "non_payment": _parse_optional_bool(payload.get("non_payment")),
        "breach": _parse_optional_bool(payload.get("breach")),
        "nuisance": _parse_optional_bool(payload.get("nuisance")),
        "illegal_use": _parse_optional_bool(payload.get("illegal_use")),
        "failure_to_sign_renewal": _parse_optional_bool(payload.get("failure_to_sign_renewal")),
        "access_denial": _parse_optional_bool(payload.get("access_denial")),
        "unapproved_subtenant": _parse_optional_bool(payload.get("unapproved_subtenant")),
        "late_payments": _parse_optional_bool(payload.get("late_payments")),
        "roommate_same_unit": _parse_optional_bool(payload.get("roommate_same_unit")),
        "other_cause": _parse_optional_bool(payload.get("other_cause")),
        "owner_move_in": _parse_optional_bool(payload.get("owner_move_in")),
        "demolition": _parse_optional_bool(payload.get("demolition")),
        "capital_improvement": _parse_optional_bool(payload.get("capital_improvement")),
        "substantial_rehab": _parse_optional_bool(payload.get("substantial_rehab")),
        "ellis_act_withdrawal": _parse_optional_bool(payload.get("ellis_act_withdrawal")),
        "condo_conversion": _parse_optional_bool(payload.get("condo_conversion")),
        "lead_remediation": _parse_optional_bool(payload.get("lead_remediation")),
        "development": _parse_optional_bool(payload.get("development")),
        "good_samaritan_ends": _parse_optional_bool(payload.get("good_samaritan_ends")),
    }


@_register_mapper("incidents")
def _map_incidents_fields(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "row_id": _parse_optional_string(payload.get("row_id")),
        "incident_id": _parse_optional_string(payload.get("incident_id")),
        "incident_number": _parse_optional_string(payload.get("incident_number")),
        "cad_number": _parse_optional_string(payload.get("cad_number")),
        "report_type_code": _parse_optional_string(payload.get("report_type_code")),
        "report_type": _parse_optional_string(payload.get("report_type_description")),
        "filed_online": _parse_optional_bool(payload.get("filed_online")),
        "incident_at": _parse_optional_timestamp(payload.get("incident_datetime")),
        "incident_date": _parse_optional_date(payload.get("incident_date")),
        "incident_time": _parse_optional_string(payload.get("incident_time")),
        "incident_year": _parse_optional_int(payload.get("incident_year")),
        "incident_day_of_week": _parse_optional_string(payload.get("incident_day_of_week")),
        "reported_at": _parse_optional_timestamp(payload.get("report_datetime")),
        "incident_code": _parse_optional_string(payload.get("incident_code")),
        "incident_category": _parse_optional_string(payload.get("incident_category")),
        "incident_subcategory": _parse_optional_string(payload.get("incident_subcategory")),
        "incident_description": _parse_optional_string(payload.get("incident_description")),
        "resolution": _parse_optional_string(payload.get("resolution")),
        "intersection": _parse_optional_string(payload.get("intersection")),
        "police_district": _parse_optional_string(payload.get("police_district")),
        "neighborhood": _parse_optional_string(payload.get("analysis_neighborhood")),
        "supervisor_district": _parse_optional_string(payload.get("supervisor_district")),
        "latitude": _parse_optional_float(payload.get("latitude")),
        "longitude": _parse_optional_float(payload.get("longitude")),
    }


def map_bronze_row(
    *,
    dataset_name: str,
    contract: TableContract,
    payload: dict[str, Any],
    ingest_run_id: str,
    raw_s3_path: str,
    raw_s3_key: str,
    data_interval_start: datetime,
    data_interval_end: datetime,
    effective_start: datetime,
    extracted_at: datetime,
    source_dataset_id: str,
) -> dict[str, Any]:
    mapper = _FIELD_MAPPERS.get(dataset_name)
    if mapper is None:
        raise LakehouseLoadError(f"unsupported dataset for bronze promotion: {dataset_name!r}")

    natural_key = contract.natural_key[0]
    typed_fields = mapper(payload)
    natural_value = typed_fields.get(natural_key)
    if natural_value is None:
        raise LakehouseLoadError(
            f"{dataset_name} record is missing required natural key {natural_key!r}"
        )

    loaded_at = _parse_loaded_at(payload, dataset_name=dataset_name)
    row = {
        "_ingest_run_id": ingest_run_id,
        "_raw_s3_path": raw_s3_path,
        "_raw_s3_key": raw_s3_key,
        "_data_interval_start": data_interval_start,
        "_data_interval_end": data_interval_end,
        "_effective_start": effective_start,
        "_loaded_at": loaded_at,
        "_extracted_at": extracted_at,
        "_source_dataset_id": source_dataset_id,
        "_record_hash": canonical_record_hash(payload),
        "_raw_payload": source_faithful_payload(payload),
    }
    row.update(typed_fields)
    return row


def iter_raw_records(storage: StorageConfig, raw_key: str) -> Iterator[dict[str, Any]]:
    for line in storage.read_text_lines(raw_key):
        stripped = line.strip()
        if not stripped:
            continue
        yield json.loads(stripped)


def build_manifest_id(*, layer: str, table_name: str, s3_key: str) -> str:
    material = f"{layer}:{table_name}:{s3_key}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def hash_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def s3_endpoint_url() -> str | None:
    """Return a custom S3 endpoint when configured for local MinIO or other S3-compatible stores."""
    return os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("AWS_S3_ENDPOINT_URL") or None


def local_minio_host() -> str:
    return os.environ.get("MINIO_HOST", "localhost")


def local_minio_api_port() -> int:
    return int(os.environ.get("MINIO_API_PORT", "9000"))


def local_minio_endpoint_url() -> str:
    """Resolve the local MinIO API endpoint from env, honoring explicit S3 overrides first."""
    explicit = s3_endpoint_url()
    if explicit:
        return explicit
    return f"http://{local_minio_host()}:{local_minio_api_port()}"


def build_s3_client(
    *,
    endpoint_url: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
) -> Any:
    import boto3

    kwargs: dict[str, Any] = {}
    resolved_endpoint = endpoint_url if endpoint_url is not None else s3_endpoint_url()
    if resolved_endpoint:
        kwargs["endpoint_url"] = resolved_endpoint
    if aws_access_key_id is not None:
        kwargs["aws_access_key_id"] = aws_access_key_id
    if aws_secret_access_key is not None:
        kwargs["aws_secret_access_key"] = aws_secret_access_key
    return boto3.client("s3", **kwargs)


def storage_from_env() -> StorageConfig:
    local_root = os.environ.get("LAKE_LOCAL_ROOT")
    if local_root:
        return StorageConfig(bucket=None, local_root=Path(local_root), s3_client=None)
    bucket = os.environ.get("AWS_S3_BUCKET")
    if not bucket:
        raise RuntimeError("AWS_S3_BUCKET or LAKE_LOCAL_ROOT must be set")
    return StorageConfig(bucket=bucket, local_root=None, s3_client=build_s3_client())


def storage_from_lakehouse_env() -> StorageConfig:
    """Build an S3-compatible client for the local MinIO lakehouse sandbox."""
    bucket = os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")
    access_key = os.environ.get(
        "MINIO_ROOT_USER",
        os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
    )
    secret_key = os.environ.get(
        "MINIO_ROOT_PASSWORD",
        os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )
    return StorageConfig(
        bucket=bucket,
        local_root=None,
        s3_client=build_s3_client(
            endpoint_url=local_minio_endpoint_url(),
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        ),
    )


def _write_bronze_batches(
    *,
    schema: pa.Schema,
    row_batches: Iterator[list[dict[str, Any]]],
) -> tuple[int, Path]:
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as handle:
            tmp_path = Path(handle.name)
        writer = pq.ParquetWriter(tmp_path, schema, use_dictionary=False)
        records_promoted = 0
        try:
            for batch in row_batches:
                if not batch:
                    continue
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                records_promoted += len(batch)
        finally:
            writer.close()
        return records_promoted, tmp_path
    except Exception:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)
        raise


def _iter_bronze_row_batches(
    *,
    storage: StorageConfig,
    raw_key: str,
    dataset_name: str,
    dataset_id: str,
    contract: TableContract,
    ingest_event: "IngestRunEvent",
    batch_size: int,
) -> Iterator[list[dict[str, Any]]]:
    batch: list[dict[str, Any]] = []
    for payload in iter_raw_records(storage, raw_key):
        batch.append(
            map_bronze_row(
                dataset_name=dataset_name,
                contract=contract,
                payload=payload,
                ingest_run_id=ingest_event.ingest_run_id,
                raw_s3_path=ingest_event.raw_s3_path or "",
                raw_s3_key=ingest_event.raw_s3_key or "",
                data_interval_start=ingest_event.data_interval_start,
                data_interval_end=ingest_event.data_interval_end,
                effective_start=ingest_event.effective_start,
                extracted_at=ingest_event.completed_at,
                source_dataset_id=dataset_id,
            )
        )
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def promote_raw_to_bronze(
    *,
    dataset_name: str,
    dataset_id: str,
    ingest_event: "IngestRunEvent",
    storage: StorageConfig | None = None,
    batch_size: int = DEFAULT_PROMOTION_BATCH_SIZE,
    contract_root: Path | None = None,
) -> PromoteResult:
    """Promote one raw interval into bronze Parquet and a file-manifest event."""
    from scripts.lakehouse_metadata import EVENT_VERSION, FileManifestEvent, write_file_manifest_event

    storage = storage or storage_from_env()
    contract = get_contract("bronze", dataset_name, contract_root)

    if ingest_event.records_fetched == 0:
        return PromoteResult(None, None, 0, 0, None, None, None)

    if not ingest_event.raw_s3_key or not ingest_event.raw_s3_path:
        raise LakehouseLoadError(
            f"{dataset_name} promotion expected a raw object when records_fetched > 0"
        )

    promotion_started_at = datetime.now(timezone.utc)
    bronze_key = bronze_object_key(
        contract,
        data_interval_start=ingest_event.data_interval_start,
        data_interval_end=ingest_event.data_interval_end,
    )
    schema = pyarrow_schema_for_contract(contract)
    batches = _iter_bronze_row_batches(
        storage=storage,
        raw_key=ingest_event.raw_s3_key,
        dataset_name=dataset_name,
        dataset_id=dataset_id,
        contract=contract,
        ingest_event=ingest_event,
        batch_size=batch_size,
    )
    tmp_path: Path | None = None
    try:
        records_promoted, tmp_path = _write_bronze_batches(
            schema=schema,
            row_batches=batches,
        )

        if records_promoted != ingest_event.records_fetched:
            raise LakehouseLoadError(
                f"{dataset_name} promoted {records_promoted} rows but expected "
                f"{ingest_event.records_fetched}"
            )

        file_size_bytes = storage.write_file(bronze_key, tmp_path)
        content_hash = hash_file(tmp_path)
    finally:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)

    bronze_path = storage.uri_for_key(bronze_key)
    promotion_completed_at = datetime.now(timezone.utc)
    manifest_id = build_manifest_id(layer="bronze", table_name=dataset_name, s3_key=bronze_key)
    manifest_event_key = write_file_manifest_event(
        storage,
        FileManifestEvent(
            event_type="file_manifest",
            event_version=EVENT_VERSION,
            manifest_id=manifest_id,
            layer="bronze",
            table_name=dataset_name,
            s3_path=bronze_path,
            s3_key=bronze_key,
            data_interval_start=ingest_event.data_interval_start,
            data_interval_end=ingest_event.data_interval_end,
            record_count=records_promoted,
            file_size_bytes=file_size_bytes,
            content_hash=content_hash,
            written_at=promotion_completed_at,
            promotion_started_at=promotion_started_at,
            promotion_completed_at=promotion_completed_at,
        ),
    )
    return PromoteResult(
        bronze_path=bronze_path,
        bronze_key=bronze_key,
        records_promoted=records_promoted,
        file_size_bytes=file_size_bytes,
        content_hash=content_hash,
        manifest_id=manifest_id,
        manifest_event_key=manifest_event_key,
    )
