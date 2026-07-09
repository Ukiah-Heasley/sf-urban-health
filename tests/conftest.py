"""Shared pytest fixtures.

Most ingest paths fail-fast on missing env vars (`AWS_S3_BUCKET`,
`DATASF_APP_TOKEN`). Tests run inside a hermetic environment with no
real credentials, so the autouse fixture below sets safe placeholders
before any test imports the module under test.
"""

from __future__ import annotations

import os

import pytest


@pytest.fixture(autouse=True)
def _set_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_S3_BUCKET", "test-bucket")
    # Tokens are optional in the producer, but setting them deterministically
    # avoids any code path that reaches the real network.
    monkeypatch.setenv("DATASF_APP_TOKEN", "test-token")
    os.environ.pop("DATASF_SECRET_TOKEN", None)
