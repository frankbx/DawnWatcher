"""Consistent online SQLite backup and verification."""

from __future__ import annotations

import hashlib
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4


@dataclass(frozen=True, slots=True)
class BackupResult:
    """Metadata for a verified backup artifact."""

    path: str
    size_bytes: int
    sha256: str
    integrity: str


def sqlite_integrity_check(database_path: Path) -> tuple[bool, str]:
    """Run SQLite's full integrity check against an existing database."""
    if not database_path.is_file():
        raise FileNotFoundError(database_path)
    connection = sqlite3.connect(f"{database_path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        rows = [str(row[0]) for row in connection.execute("PRAGMA integrity_check")]
    finally:
        connection.close()
    result = "; ".join(rows)
    return result == "ok", result


def online_backup(
    source_path: Path,
    destination_path: Path,
    *,
    busy_timeout_ms: int = 5_000,
) -> BackupResult:
    """Create and verify a consistent backup without stopping the application."""
    source = source_path.resolve()
    destination = destination_path.resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    if source == destination:
        raise ValueError("backup destination must differ from the source database")
    if destination.exists():
        raise FileExistsError(destination)

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.tmp")
    source_connection = sqlite3.connect(
        f"{source.as_uri()}?mode=ro",
        uri=True,
        timeout=busy_timeout_ms / 1_000,
    )
    destination_connection = sqlite3.connect(temporary)
    try:
        source_connection.backup(destination_connection, pages=256, sleep=0.05)
        destination_connection.commit()
    finally:
        destination_connection.close()
        source_connection.close()

    try:
        is_valid, integrity = sqlite_integrity_check(temporary)
        if not is_valid:
            raise RuntimeError(f"backup integrity check failed: {integrity}")
        digest = hashlib.sha256(temporary.read_bytes()).hexdigest()
        size = temporary.stat().st_size
        os.link(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)

    return BackupResult(
        path=str(destination),
        size_bytes=size,
        sha256=digest,
        integrity=integrity,
    )
