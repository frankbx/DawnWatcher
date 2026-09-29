"""Programmatic Alembic migration and schema health helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import cast

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import Engine, text

from regimebeacon.config import Settings
from regimebeacon.storage.database import create_database_engine


@dataclass(frozen=True, slots=True)
class SchemaStatus:
    """Database revision, integrity, and durability settings."""

    ok: bool
    database_path: str
    revision: str | None
    expected_revision: str
    integrity: str
    journal_mode: str
    foreign_keys: bool
    synchronous: int


def _script_location() -> Path:
    return Path(__file__).with_name("alembic")


def alembic_config(settings: Settings) -> Config:
    """Build an Alembic configuration independent of the current directory."""
    config = Config()
    config.set_main_option("script_location", str(_script_location()))
    config.set_main_option("sqlalchemy.url", settings.database_url.replace("%", "%%"))
    return config


def expected_revision(settings: Settings) -> str:
    """Return the migration head shipped with this application version."""
    scripts = ScriptDirectory.from_config(alembic_config(settings))
    head = scripts.get_current_head()
    if head is None:
        raise RuntimeError("no Alembic migration head is available")
    return head


def upgrade_database(settings: Settings, revision: str = "head") -> None:
    """Upgrade the configured database to a migration revision."""
    settings.ensure_runtime_directories()
    command.upgrade(alembic_config(settings), revision)


def inspect_schema(settings: Settings, engine: Engine | None = None) -> SchemaStatus:
    """Check the schema revision, SQLite integrity, and critical PRAGMAs."""
    owned_engine = engine is None
    active_engine = engine or create_database_engine(settings)
    expected = expected_revision(settings)
    try:
        with active_engine.connect() as connection:
            has_version_table = connection.scalar(
                text(
                    "SELECT count(*) FROM sqlite_master "
                    "WHERE type = 'table' AND name = 'alembic_version'"
                )
            )
            revision = (
                connection.scalar(text("SELECT version_num FROM alembic_version"))
                if has_version_table
                else None
            )
            integrity_rows = cast(
                list[str],
                connection.exec_driver_sql("PRAGMA integrity_check").scalars().all(),
            )
            integrity = "; ".join(integrity_rows)
            journal_mode = str(connection.scalar(text("PRAGMA journal_mode")))
            foreign_keys = bool(connection.scalar(text("PRAGMA foreign_keys")))
            synchronous = int(connection.scalar(text("PRAGMA synchronous")) or 0)
    finally:
        if owned_engine:
            active_engine.dispose()

    ok = (
        revision == expected
        and integrity == "ok"
        and journal_mode.lower() == "wal"
        and foreign_keys
        and synchronous == 2
    )
    return SchemaStatus(
        ok=ok,
        database_path=str(settings.database_path.resolve()),
        revision=str(revision) if revision is not None else None,
        expected_revision=expected,
        integrity=integrity,
        journal_mode=journal_mode,
        foreign_keys=foreign_keys,
        synchronous=synchronous,
    )
