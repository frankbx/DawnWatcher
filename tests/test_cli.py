"""Tests for the public command-line interface."""

from __future__ import annotations

import json
from pathlib import Path

from dawnwatcher.cli import main


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
