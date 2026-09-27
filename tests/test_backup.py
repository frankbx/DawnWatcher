"""Online backup and restore-readability tests."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy.orm import Session, sessionmaker

from dawnwatcher.config import Settings
from dawnwatcher.storage.backup import online_backup, sqlite_integrity_check
from dawnwatcher.workflows.job_runs import create_job_run


def test_online_backup_is_complete_and_verified(
    database_settings: Settings,
    session_factory_fixture: sessionmaker[Session],
    tmp_path: Path,
) -> None:
    with session_factory_fixture.begin() as session:
        create_job_run(
            session,
            idempotency_key="backup-test",
            job_type="test",
            scheduled_for=datetime(2026, 9, 28, tzinfo=UTC),
        )

    destination = tmp_path / "remote" / "backup.sqlite3"
    result = online_backup(database_settings.database_path, destination)

    assert Path(result.path) == destination
    assert result.integrity == "ok"
    assert len(result.sha256) == 64
    assert result.size_bytes > 0
    assert sqlite_integrity_check(destination) == (True, "ok")

    connection = sqlite3.connect(destination)
    try:
        count = connection.execute("SELECT count(*) FROM job_run").fetchone()[0]
    finally:
        connection.close()
    assert count == 1

    with pytest.raises(FileExistsError):
        online_backup(database_settings.database_path, destination)
