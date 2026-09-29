"""Structured application logging."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any, TextIO, cast

from regimebeacon.config import LogLevel

_STANDARD_RECORD_FIELDS = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
        "taskName",
    }
)


class JsonFormatter(logging.Formatter):
    """Render log records as one-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        """Serialize a log record without assuming extras are JSON-compatible."""
        event: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_RECORD_FIELDS and not key.startswith("_"):
                event[key] = value
        if record.exc_info:
            event["exception"] = self.formatException(record.exc_info)
        return json.dumps(event, ensure_ascii=False, default=str, separators=(",", ":"))


class _CurrentStdout:
    """Resolve stdout at write time so test capture and service redirection remain safe."""

    def write(self, message: str) -> int:
        return sys.stdout.write(message)

    def flush(self) -> None:
        sys.stdout.flush()


def configure_logging(level: LogLevel = "INFO") -> None:
    """Configure the root logger for deterministic structured output."""
    handler = logging.StreamHandler(cast(TextIO, _CurrentStdout()))
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    # httpx emits request URLs at INFO; those URLs may contain Feishu webhook secrets.
    # Keep provider request details available at DEBUG without leaking credentials by default.
    for client_logger in ("httpx", "httpcore"):
        logging.getLogger(client_logger).setLevel(logging.WARNING)
