"""Tests for the public command-line interface."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dawnwatcher.cli import build_parser, main


def test_cli_without_command_prints_help(capsys: object) -> None:
    assert main([]) == 0
    output = capsys.readouterr().out  # type: ignore[attr-defined]
    assert "Intraday monitoring" in output
    assert "doctor" in output


def test_doctor_creates_runtime_directories(
    monkeypatch: object,
    tmp_path: Path,
    capsys: object,
) -> None:
    monkeypatch.setenv("DAWNWATCHER_DATA_DIR", str(tmp_path))  # type: ignore[attr-defined]

    assert main(["doctor"]) == 0
    output = capsys.readouterr().out  # type: ignore[attr-defined]
    report = json.loads(output)

    assert report["status"] == "ok"
    assert report["writable"] is True
    for name in ("db", "raw", "reports", "backups"):
        assert (tmp_path / name).is_dir()


def test_config_uses_environment_overrides(
    monkeypatch: object,
    tmp_path: Path,
    capsys: object,
) -> None:
    monkeypatch.setenv("DAWNWATCHER_ENVIRONMENT", "test")  # type: ignore[attr-defined]
    monkeypatch.setenv("DAWNWATCHER_DATA_DIR", str(tmp_path))  # type: ignore[attr-defined]

    assert main(["config"]) == 0
    output = capsys.readouterr().out  # type: ignore[attr-defined]
    config = json.loads(output)

    assert config["environment"] == "test"
    assert config["data_dir"] == str(tmp_path)


def test_quote_watch_uses_configured_interval_by_default() -> None:
    args = build_parser().parse_args(["quotes", "watch", "600000.SH"])

    assert args.interval_seconds is None
    assert args.max_runs is None


def test_quote_watch_accepts_interval_override() -> None:
    args = build_parser().parse_args(
        ["quotes", "watch", "600000.SH", "--interval", "30", "--max-runs", "2"]
    )

    assert args.interval_seconds == 30.0
    assert args.max_runs == 2


def test_quote_watch_rejects_unsafe_interval() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["quotes", "watch", "600000.SH", "--interval", "0.1"])


def test_one_shot_collection_is_gated_unless_explicitly_overridden() -> None:
    regular = build_parser().parse_args(["quotes", "collect", "600000.SH"])
    diagnostic = build_parser().parse_args(
        ["quotes", "collect", "600000.SH", "--ignore-market-gate"]
    )

    assert regular.ignore_market_gate is False
    assert diagnostic.ignore_market_gate is True
