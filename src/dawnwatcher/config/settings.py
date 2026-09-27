"""Typed settings loaded from environment variables and an optional .env file."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class Environment(StrEnum):
    """Supported runtime environments."""

    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


class Settings(BaseSettings):
    """DawnWatcher runtime configuration."""

    model_config = SettingsConfigDict(
        env_prefix="DAWNWATCHER_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    environment: Environment = Environment.DEVELOPMENT
    log_level: LogLevel = "INFO"
    timezone: str = "Asia/Shanghai"
    data_dir: Path = Path("data")
    database_filename: str = "dawnwatcher.sqlite3"
    sqlite_busy_timeout_ms: int = Field(default=5_000, ge=100, le=60_000)

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        """Reject unknown IANA timezone names during startup."""
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown IANA timezone: {value}") from exc
        return value

    @field_validator("database_filename")
    @classmethod
    def validate_database_filename(cls, value: str) -> str:
        """Keep the SQLite file inside the managed data/db directory."""
        if not value or Path(value).name != value or value in {".", ".."}:
            raise ValueError("database_filename must be a plain filename")
        return value

    @property
    def runtime_directories(self) -> tuple[Path, ...]:
        """Return all directories the application expects to write to."""
        return tuple(self.data_dir / name for name in ("db", "raw", "reports", "backups"))

    @property
    def database_path(self) -> Path:
        """Return the local SQLite database path."""
        return self.data_dir / "db" / self.database_filename

    @property
    def database_url(self) -> str:
        """Return a SQLAlchemy URL for the local SQLite database."""
        return f"sqlite+pysqlite:///{self.database_path.resolve()}"

    def ensure_runtime_directories(self) -> tuple[Path, ...]:
        """Create the local runtime directory tree if it does not exist."""
        for directory in self.runtime_directories:
            directory.mkdir(parents=True, exist_ok=True)
        return self.runtime_directories

    def public_dict(self) -> dict[str, str]:
        """Return configuration that is safe to display in diagnostics."""
        return {
            "environment": self.environment.value,
            "log_level": self.log_level,
            "timezone": self.timezone,
            "data_dir": str(self.data_dir),
            "database_path": str(self.database_path),
            "sqlite_busy_timeout_ms": str(self.sqlite_busy_timeout_ms),
        }
