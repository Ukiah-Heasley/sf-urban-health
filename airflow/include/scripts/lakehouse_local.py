"""Local MinIO lakehouse helpers for fixture preparation and Spark registration."""

from __future__ import annotations

import os
import socket
import subprocess
import time
from pathlib import Path
from urllib.parse import urlparse

from scripts.lakehouse_load import (
    LakehouseLoadError,
    StorageConfig,
    local_minio_api_port,
    local_minio_endpoint_url,
    storage_from_lakehouse_env,
)


class LakehouseLocalError(RuntimeError):
    """Raised when the local lakehouse sandbox is unavailable or misconfigured."""


def repo_root() -> Path:
    current = Path(__file__).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "lakehouse" / "docker-compose.yml").is_file():
            return candidate
    raise FileNotFoundError(
        "could not locate lakehouse/docker-compose.yml from module path"
    )


def load_lakehouse_env() -> None:
    env_path = repo_root() / "lakehouse" / ".env"
    if env_path.is_file():
        from dotenv import load_dotenv

        load_dotenv(env_path)


def minio_api_port() -> int:
    return local_minio_api_port()


def spark_thrift_port() -> int:
    return int(
        os.environ.get("SPARK_THRIFT_PORT", os.environ.get("DBT_SPARK_PORT", "10000"))
    )


def spark_thrift_host() -> str:
    return os.environ.get("DBT_SPARK_HOST", "localhost")


def _port_open(host: str, port: int, *, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _endpoint_host_port(endpoint_url: str) -> tuple[str, int]:
    parsed = urlparse(endpoint_url)
    if not parsed.hostname:
        raise LakehouseLocalError(f"invalid MinIO endpoint URL: {endpoint_url!r}")
    port = parsed.port
    if port is None:
        port = 443 if parsed.scheme == "https" else 80
    return parsed.hostname, port


def require_local_lakehouse_stack() -> StorageConfig:
    """Fail fast when local MinIO or Spark Thrift is not reachable."""
    load_lakehouse_env()
    storage = storage_from_lakehouse_env()
    if storage.s3_client is None or not storage.bucket:
        raise LakehouseLocalError("local lakehouse storage is not configured")

    minio_endpoint = local_minio_endpoint_url()
    minio_host, minio_port = _endpoint_host_port(minio_endpoint)
    if not _port_open(minio_host, minio_port):
        raise LakehouseLocalError(
            f"MinIO is not reachable at {minio_endpoint}. Start the stack with `make spark-up`."
        )

    try:
        storage.s3_client.head_bucket(Bucket=storage.bucket)
    except Exception as exc:
        raise LakehouseLocalError(
            f"MinIO bucket {storage.bucket!r} is not available at {minio_endpoint}. "
            "Start the stack with `make spark-up`."
        ) from exc

    if not _port_open(spark_thrift_host(), spark_thrift_port()):
        raise LakehouseLocalError(
            "Spark Thrift Server is not reachable at "
            f"{spark_thrift_host()}:{spark_thrift_port()}. Start the stack with `make spark-up`."
        )
    return storage


def reset_lakehouse_bucket(storage: StorageConfig) -> int:
    """Delete every object in the configured local lakehouse bucket."""
    if storage.s3_client is None or not storage.bucket:
        raise LakehouseLoadError("S3 client and bucket are required for remote deletes")
    keys = storage.list_keys("")
    deleted = 0
    for index in range(0, len(keys), 1000):
        batch = [{"Key": key} for key in keys[index : index + 1000]]
        if not batch:
            continue
        response = storage.s3_client.delete_objects(
            Bucket=storage.bucket,
            Delete={"Objects": batch},
        )
        errors = response.get("Errors", [])
        if errors:
            first = errors[0]
            raise LakehouseLoadError(
                "failed to reset local lakehouse bucket "
                f"{storage.bucket!r}: key {first.get('Key')!r} failed with "
                f"{first.get('Code')!r} ({first.get('Message')!r})"
            )
        deleted += len(response.get("Deleted", batch))
    return deleted


def _lakehouse_compose_cmd(*args: str) -> list[str]:
    compose = repo_root() / "lakehouse" / "docker-compose.yml"
    env_file = repo_root() / "lakehouse" / ".env"
    return [
        "docker",
        "compose",
        "-f",
        str(compose),
        "--profile",
        "local",
        "--env-file",
        str(env_file),
        *args,
    ]


def _wait_for_spark_thrift(*, timeout_seconds: float = 180.0) -> None:
    from pyhive import hive

    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if not _port_open(spark_thrift_host(), spark_thrift_port(), timeout=2.0):
            time.sleep(2.0)
            continue
        try:
            connection = hive.Connection(
                host=spark_thrift_host(),
                port=spark_thrift_port(),
                auth="NOSASL",
                database="sf_urban_health",
            )
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            connection.close()
            return
        except Exception as exc:
            last_error = exc
            time.sleep(3.0)
    detail = str(last_error) if last_error else "Spark Thrift port never opened"
    raise LakehouseLocalError(f"Spark Thrift Server did not become ready: {detail}")


def refresh_spark_catalog_after_bucket_reset(*, timeout_seconds: float = 180.0) -> None:
    """Restart Spark Thrift so entrypoint bootstrap recreates catalog namespaces."""
    env = os.environ.copy()
    env["LAKEHOUSE_CATALOG"] = "hadoop"
    result = subprocess.run(
        _lakehouse_compose_cmd("restart", "spark-thrift"),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    if result.returncode != 0:
        detail = (
            result.stderr.strip() or result.stdout.strip() or "unknown compose failure"
        )
        raise LakehouseLocalError(f"failed to restart spark-thrift: {detail}")
    _wait_for_spark_thrift(timeout_seconds=timeout_seconds)
