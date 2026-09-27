"""Local startup checks used by operators and deployment probes."""

from __future__ import annotations

import os
import platform
import sys
from pathlib import Path
from typing import TypedDict

from dawnwatcher.config import Settings


class HealthReport(TypedDict):
    """Serializable startup-check result."""

    status: str
    python: str
    platform: str
    timezone: str
    data_dir: str
    writable: bool
    checks: list[str]


def _is_writable(directory: Path) -> bool:
    return os.access(directory, os.W_OK)


def run_startup_checks(settings: Settings) -> HealthReport:
    """Validate the local prerequisites required by the Phase 0 runtime."""
    directories = settings.ensure_runtime_directories()
    checks: list[str] = []

    python_ok = sys.version_info[:2] == (3, 12)
    checks.append("python_3_12" if python_ok else "python_version_mismatch")

    writable = all(_is_writable(directory) for directory in directories)
    checks.append("runtime_directories_writable" if writable else "runtime_directory_not_writable")

    return {
        "status": "ok" if python_ok and writable else "error",
        "python": platform.python_version(),
        "platform": platform.platform(),
        "timezone": settings.timezone,
        "data_dir": str(settings.data_dir.resolve()),
        "writable": writable,
        "checks": checks,
    }
