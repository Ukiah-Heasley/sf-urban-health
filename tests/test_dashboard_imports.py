"""Verify the dashboard package imports cleanly without a live warehouse.

`dashboard/data/cache.py` wraps each `query_arrow` call in try/except and
falls back to an empty DataFrame, so module import succeeds even when
warehouse credentials are missing — exactly what we want at CI time.

Implementation notes
--------------------
* The dashboard pulls in `pyarrow` and `polars`, whose precompiled wheels can
  be platform-fragile (e.g. native macOS 26 arm64 binaries lag behind the Linux
  ones). We therefore run the import inside a subprocess so a wheel-level crash
  surfaces as a test failure rather than killing the entire pytest collection
  phase.
* Set `SKIP_DASHBOARD_TESTS=1` to skip these locally on a known-bad venv.
* Skipped automatically when the heavy dashboard deps aren't installed
  (the import probe at the top of each test handles it).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent

_DASHBOARD_DEPS = ("dash", "polars", "pyarrow")


def _skip_unless_dashboard_deps_present() -> None:
    """Probe each heavy dep in a subprocess.

    Importing them in-process risks segfaulting on platform-fragile wheels
    (numpy + pyarrow on bleeding-edge macOS/arm64, etc.). The subprocess
    isolates the probe so pytest survives.
    """
    if os.environ.get("SKIP_DASHBOARD_TESTS") == "1":
        pytest.skip("dashboard tests disabled via SKIP_DASHBOARD_TESTS=1")

    for mod in _DASHBOARD_DEPS:
        result = subprocess.run(
            [sys.executable, "-c", f"import {mod}"],
            capture_output=True,
            timeout=30,
        )
        if result.returncode != 0:
            pytest.skip(
                f"dashboard dependency {mod!r} unavailable or unloadable on this platform "
                f"(set SKIP_DASHBOARD_TESTS=1 to silence). "
                f"stderr: {result.stderr.decode(errors='replace')[:300]}"
            )


def _run(snippet: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [sys.executable, "-c", snippet],
        cwd=_REPO_ROOT,
        capture_output=True,
        timeout=60,
    )


def test_dashboard_app_imports_without_warehouse():
    _skip_unless_dashboard_deps_present()
    result = _run(
        "import dashboard.app as m; "
        "assert hasattr(m, 'app'), 'dashboard.app must expose `app`'; "
        "assert hasattr(m, 'server'), 'dashboard.app must expose `server`'"
    )
    assert (
        result.returncode == 0
    ), f"dashboard.app failed to import:\n{result.stderr.decode(errors='replace')}"


def test_transform_modules_import_pure():
    """The transform modules are pure-polars helpers; they must always import."""
    _skip_unless_dashboard_deps_present()
    result = _run(
        "import importlib; "
        "importlib.import_module('dashboard.data.transforms'); "
        "importlib.import_module('dashboard.data.evictions_transforms'); "
        "importlib.import_module('dashboard.data.incidents_transforms'); "
        "importlib.import_module('dashboard.data.pipeline_transforms'); "
        "importlib.import_module('dashboard.data.trust_transforms')"
    )
    assert result.returncode == 0, (
        f"dashboard.data transform modules failed to import:\n"
        f"{result.stderr.decode(errors='replace')}"
    )
