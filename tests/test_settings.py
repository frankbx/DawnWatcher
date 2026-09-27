"""Tests for typed application settings."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from dawnwatcher.config import Environment, Settings


def test_default_settings() -> None:
    settings = Settings(_env_file=None)

    assert settings.environment is Environment.DEVELOPMENT
    assert settings.timezone == "Asia/Shanghai"
    assert settings.data_dir == Path("data")


def test_invalid_timezone_is_rejected() -> None:
    with pytest.raises(ValidationError, match="unknown IANA timezone"):
        Settings(timezone="Mars/Olympus", _env_file=None)


def test_database_filename_cannot_escape_managed_directory() -> None:
    with pytest.raises(ValidationError, match="plain filename"):
        Settings(database_filename="../outside.sqlite3", _env_file=None)


def test_runtime_directories_are_scoped_to_data_dir(tmp_path: Path) -> None:
    settings = Settings(data_dir=tmp_path, _env_file=None)

    assert settings.runtime_directories == tuple(
        tmp_path / name for name in ("db", "raw", "reports", "backups")
    )
