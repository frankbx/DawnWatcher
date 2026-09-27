"""Schema migration and SQLite durability tests."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine

from dawnwatcher.config import Settings
from dawnwatcher.storage.schema import inspect_schema


def test_migration_applies_expected_schema(
    database_settings: Settings,
    database_engine: Engine,
) -> None:
    status = inspect_schema(database_settings, database_engine)

    assert status.ok is True
    assert status.revision == "0001_phase1"
    assert status.integrity == "ok"
    assert status.journal_mode == "wal"
    assert status.foreign_keys is True
    assert status.synchronous == 2
    assert Path(status.database_path).is_file()
