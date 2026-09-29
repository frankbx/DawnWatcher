"""Tests for structured JSON logging."""

from __future__ import annotations

import json
import logging

from regimebeacon.logging import JsonFormatter


def test_json_formatter_preserves_structured_context() -> None:
    record = logging.LogRecord(
        name="regimebeacon.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="startup complete",
        args=(),
        exc_info=None,
    )
    record.run_id = "run-1"

    payload = json.loads(JsonFormatter().format(record))

    assert payload["level"] == "INFO"
    assert payload["message"] == "startup complete"
    assert payload["run_id"] == "run-1"
    assert payload["timestamp"].endswith("+00:00")
