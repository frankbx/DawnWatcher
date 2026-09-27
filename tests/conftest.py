"""Shared database fixtures."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from dawnwatcher.config import Settings
from dawnwatcher.storage.database import create_database_engine, create_session_factory
from dawnwatcher.storage.schema import upgrade_database


@pytest.fixture
def database_settings(tmp_path: Path) -> Settings:
    return Settings(data_dir=tmp_path, _env_file=None)


@pytest.fixture
def database_engine(database_settings: Settings) -> Iterator[Engine]:
    upgrade_database(database_settings)
    engine = create_database_engine(database_settings)
    yield engine
    engine.dispose()


@pytest.fixture
def session_factory_fixture(database_engine: Engine) -> sessionmaker[Session]:
    return create_session_factory(database_engine)
