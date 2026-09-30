"""Tests for the public command-line interface."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from regimebeacon import cli
from regimebeacon.cli import build_parser, main
from regimebeacon.market import ChinaAStockCalendar
from regimebeacon.storage.models import RuntimeHeartbeat


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
    monkeypatch.setenv("REGIMEBEACON_DATA_DIR", str(tmp_path))  # type: ignore[attr-defined]

    assert main(["doctor"]) == 0
    output = capsys.readouterr().out  # type: ignore[attr-defined]
    report = json.loads(output)

    assert report["status"] == "ok"
    assert report["writable"] is True
    for name in ("db", "raw", "reports", "backups", "lake"):
        assert (tmp_path / name).is_dir()


def test_config_uses_environment_overrides(
    monkeypatch: object,
    tmp_path: Path,
    capsys: object,
) -> None:
    monkeypatch.setenv("REGIMEBEACON_ENVIRONMENT", "test")  # type: ignore[attr-defined]
    monkeypatch.setenv("REGIMEBEACON_DATA_DIR", str(tmp_path))  # type: ignore[attr-defined]

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


def test_quote_watcher_ignores_sqlite_lock_on_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
    session_factory_fixture: sessionmaker[Session],
) -> None:
    def locked(*args: object, **kwargs: object) -> None:
        raise OperationalError(
            "UPDATE runtime_heartbeat", {}, sqlite3.OperationalError("database is locked")
        )

    monkeypatch.setattr(cli, "touch_runtime_heartbeat", locked)
    assert (
        cli._touch_quote_watcher_heartbeat(
            session_factory_fixture,
            instance_id="quote-watcher",
            interval_seconds=15,
            details={"symbol_count": 397},
        )
        is False
    )


def test_quote_watcher_lazily_registers_heartbeat_after_initial_lock(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    assert cli._touch_quote_watcher_heartbeat(
        session_factory_fixture,
        instance_id="late-registration",
        interval_seconds=15,
        details={"symbol_count": 397},
    )
    with session_factory_fixture() as session:
        heartbeat = session.scalar(
            select(RuntimeHeartbeat).where(RuntimeHeartbeat.instance_id == "late-registration")
        )
    assert heartbeat is not None
    assert heartbeat.status == "running"
    assert heartbeat.details == {"symbol_count": 397}


def test_one_shot_collection_is_gated_unless_explicitly_overridden() -> None:
    regular = build_parser().parse_args(["quotes", "collect", "600000.SH"])
    diagnostic = build_parser().parse_args(
        ["quotes", "collect", "600000.SH", "--ignore-market-gate"]
    )

    assert regular.ignore_market_gate is False
    assert diagnostic.ignore_market_gate is True


def test_opening_probe_has_no_persistence_or_archive_flags() -> None:
    args = build_parser().parse_args(["quotes", "probe-opening", "600000.SH", "510300.SH"])

    assert args.quote_command == "probe-opening"
    assert args.symbols == ["600000.SH", "510300.SH"]
    assert not hasattr(args, "no_persist")
    assert not hasattr(args, "no_archive")


def test_opening_probe_disables_both_writes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    monkeypatch.setenv("REGIMEBEACON_DATA_DIR", str(tmp_path / "data"))
    status = ChinaAStockCalendar({date(2026, 9, 28): True}).status_at(
        datetime(2026, 9, 28, 1, 20, tzinfo=UTC)
    )
    calls: list[dict[str, object]] = []

    async def fake_status(*args: object) -> tuple[object, dict[str, object]]:
        return status, {}

    async def fake_collect(*args: object, **kwargs: object) -> object:
        calls.append(kwargs)
        return SimpleNamespace(to_dict=lambda **kwargs: {"quality_counts": {}})

    monkeypatch.setattr(cli, "_load_market_session_status", fake_status)
    monkeypatch.setattr(cli, "_collect_quotes", fake_collect)

    assert main(["quotes", "probe-opening", "600000.SH"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["event"] == "market.opening_probe.completed"
    assert payload["persisted"] is False
    assert payload["archived"] is False
    assert calls[0]["archive_raw"] is False
    assert not (tmp_path / "data").exists()


def test_gate_override_still_cannot_archive_or_persist_opening_quotes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    class OpeningClock(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:
            local = datetime(2026, 9, 28, 9, 20, tzinfo=ZoneInfo("Asia/Shanghai"))
            return local.astimezone(tz) if tz is not None else local

    calls: list[dict[str, object]] = []

    async def fake_collect(*args: object, **kwargs: object) -> object:
        calls.append(kwargs)
        return SimpleNamespace(to_dict=lambda: {"quality_counts": {}})

    monkeypatch.setenv("REGIMEBEACON_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setattr(cli, "datetime", OpeningClock)
    monkeypatch.setattr(cli, "_collect_quotes", fake_collect)

    assert main(["quotes", "collect", "600000.SH", "--ignore-market-gate"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["opening_probe_mode"] is True
    assert payload["persisted"] is False
    assert payload["archived"] is False
    assert calls[0]["archive_raw"] is False
    assert not (tmp_path / "data").exists()


def test_stats_and_monitor_commands_parse() -> None:
    stats = build_parser().parse_args(["quotes", "stats", "--date", "2026-09-28"])
    monitor = build_parser().parse_args(["monitor", "watch", "--interval", "30"])

    assert stats.date.isoformat() == "2026-09-28"
    assert monitor.interval_seconds == 30.0


def test_notification_worker_commands_parse() -> None:
    test = build_parser().parse_args(["notifications", "test", "旺财旺财"])
    deliver = build_parser().parse_args(["notifications", "deliver", "--max-items", "5"])
    watch = build_parser().parse_args(
        ["notifications", "watch", "--interval", "10", "--max-runs", "2"]
    )

    assert test.text == "旺财旺财"
    assert deliver.max_items == 5
    assert watch.interval_seconds == 10.0
    assert watch.max_runs == 2


def test_minute_feature_commands_parse() -> None:
    build = build_parser().parse_args(
        [
            "features",
            "build",
            "600000.SH",
            "--date",
            "2026-09-28",
            "--market-benchmark",
            "000001.SH",
            "--lookback-days",
            "10",
            "--minimum-history-days",
            "3",
        ]
    )
    show = build_parser().parse_args(
        ["features", "show", "--date", "2026-09-28", "--symbol", "600000.SH"]
    )
    seal = build_parser().parse_args(
        [
            "features",
            "seal",
            "--date",
            "2026-09-28",
            "--session",
            "afternoon",
            "--output-root",
            "custom-lake",
        ]
    )
    merge_day = build_parser().parse_args(
        ["features", "merge-day", "--date", "2026-09-28", "--output-root", "daily-lake"]
    )

    assert build.symbols == ["600000.SH"]
    assert build.market_benchmark == "000001.SH"
    assert build.lookback_days == 10
    assert build.minimum_history_days == 3
    assert show.symbol == "600000.SH"
    assert seal.trading_session == "afternoon"
    assert seal.output_root == Path("custom-lake")
    assert merge_day.date.isoformat() == "2026-09-28"
    assert merge_day.output_root == Path("daily-lake")


def test_unattended_runtime_command_parses() -> None:
    args = build_parser().parse_args(
        [
            "runtime",
            "run",
            "--project-root",
            "/tmp/regimebeacon",
            "--poll-interval",
            "10",
            "--max-cycles",
            "1",
        ]
    )

    assert args.project_root == Path("/tmp/regimebeacon")
    assert args.poll_interval == 10
    assert args.max_cycles == 1
