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
    """RegimeBeacon runtime configuration."""

    model_config = SettingsConfigDict(
        env_prefix="REGIMEBEACON_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    environment: Environment = Environment.DEVELOPMENT
    log_level: LogLevel = "INFO"
    timezone: str = "Asia/Shanghai"
    data_dir: Path = Path("data")
    database_filename: str = "regimebeacon.sqlite3"
    sqlite_busy_timeout_ms: int = Field(default=5_000, ge=100, le=60_000)
    market_request_timeout_seconds: float = Field(default=8.0, ge=1.0, le=30.0)
    market_batch_size: int = Field(default=50, ge=1, le=100)
    market_poll_interval_seconds: float = Field(default=15.0, ge=1.0, le=3_600.0)
    tushare_token_file: Path = Path("token")
    tushare_api_url: str = "https://api.tushare.pro"
    trading_calendar_exchange: Literal["SSE"] = "SSE"
    trading_calendar_refresh_hours: int = Field(default=24, ge=1, le=168)
    circuit_failure_threshold: int = Field(default=3, ge=1, le=20)
    circuit_cooldown_seconds: float = Field(default=60.0, ge=1.0, le=3_600.0)
    archive_raw_quotes: bool = True
    monitor_interval_seconds: float = Field(default=30.0, ge=5.0, le=3_600.0)
    heartbeat_stale_seconds: float = Field(default=60.0, ge=15.0, le=86_400.0)
    collection_gap_seconds: float = Field(default=60.0, ge=15.0, le=3_600.0)
    disk_critical_free_bytes: int = Field(default=1 * 1024**3, ge=1)
    disk_warning_free_bytes: int = Field(default=5 * 1024**3, ge=1)
    alert_channel: str = "feishu"
    alert_recipient: str = "operators"
    feishu_webhook_file: Path = Path("feishu_webhook")
    feishu_signing_secret_file: Path = Path("feishu_secret")
    feishu_request_timeout_seconds: float = Field(default=8.0, ge=1.0, le=30.0)
    notification_poll_interval_seconds: float = Field(default=5.0, ge=1.0, le=3_600.0)
    notification_batch_size: int = Field(default=20, ge=1, le=100)
    notification_lease_seconds: int = Field(default=30, ge=5, le=300)
    notification_retry_base_seconds: int = Field(default=30, ge=1, le=3_600)
    notification_retry_max_seconds: int = Field(default=1_800, ge=1, le=86_400)
    acceptance_min_valid_quote_rate_pct: float = Field(default=99.5, ge=0, le=100)
    acceptance_min_successful_run_rate_pct: float = Field(default=99.0, ge=0, le=100)
    acceptance_max_p95_latency_ms: float = Field(default=15_000, ge=1)
    acceptance_max_gap_seconds: float = Field(default=60, ge=1)

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

    @field_validator("disk_warning_free_bytes")
    @classmethod
    def validate_disk_thresholds(cls, value: int, info: object) -> int:
        """Keep the warning threshold above the critical threshold."""
        data = getattr(info, "data", {})
        critical = data.get("disk_critical_free_bytes")
        if critical is not None and value <= critical:
            raise ValueError("disk_warning_free_bytes must exceed disk_critical_free_bytes")
        return value

    @field_validator("notification_retry_max_seconds")
    @classmethod
    def validate_notification_retry_thresholds(cls, value: int, info: object) -> int:
        """Keep the retry cap at or above the first retry delay."""
        data = getattr(info, "data", {})
        base = data.get("notification_retry_base_seconds")
        if base is not None and value < base:
            raise ValueError("notification_retry_max_seconds cannot be less than the retry base")
        return value

    @property
    def runtime_directories(self) -> tuple[Path, ...]:
        """Return all directories the application expects to write to."""
        return tuple(self.data_dir / name for name in ("db", "raw", "reports", "backups", "lake"))

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
            "market_request_timeout_seconds": str(self.market_request_timeout_seconds),
            "market_batch_size": str(self.market_batch_size),
            "market_poll_interval_seconds": str(self.market_poll_interval_seconds),
            "tushare_token_file": str(self.tushare_token_file),
            "tushare_api_url": self.tushare_api_url,
            "trading_calendar_exchange": self.trading_calendar_exchange,
            "trading_calendar_refresh_hours": str(self.trading_calendar_refresh_hours),
            "circuit_failure_threshold": str(self.circuit_failure_threshold),
            "circuit_cooldown_seconds": str(self.circuit_cooldown_seconds),
            "archive_raw_quotes": str(self.archive_raw_quotes),
            "monitor_interval_seconds": str(self.monitor_interval_seconds),
            "heartbeat_stale_seconds": str(self.heartbeat_stale_seconds),
            "collection_gap_seconds": str(self.collection_gap_seconds),
            "disk_warning_free_bytes": str(self.disk_warning_free_bytes),
            "disk_critical_free_bytes": str(self.disk_critical_free_bytes),
            "alert_channel": self.alert_channel,
            "alert_recipient": self.alert_recipient,
            "feishu_webhook_file": str(self.feishu_webhook_file),
            "feishu_signing_secret_file": str(self.feishu_signing_secret_file),
            "feishu_request_timeout_seconds": str(self.feishu_request_timeout_seconds),
            "notification_poll_interval_seconds": str(self.notification_poll_interval_seconds),
            "notification_batch_size": str(self.notification_batch_size),
            "notification_lease_seconds": str(self.notification_lease_seconds),
            "notification_retry_base_seconds": str(self.notification_retry_base_seconds),
            "notification_retry_max_seconds": str(self.notification_retry_max_seconds),
            "acceptance_min_valid_quote_rate_pct": str(self.acceptance_min_valid_quote_rate_pct),
            "acceptance_min_successful_run_rate_pct": str(
                self.acceptance_min_successful_run_rate_pct
            ),
            "acceptance_max_p95_latency_ms": str(self.acceptance_max_p95_latency_ms),
            "acceptance_max_gap_seconds": str(self.acceptance_max_gap_seconds),
        }
