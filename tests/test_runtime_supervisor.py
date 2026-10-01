"""Trading-day runtime planning and macOS launch-agent tests."""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from scripts.install_macos_launch_agent import build_launch_agent

from regimebeacon.config import Settings
from regimebeacon.market import ChinaAStockCalendar
from regimebeacon.runtime import RuntimeServicePlan, build_runtime_service_plans

_ZONE = ZoneInfo("Asia/Shanghai")
_TRADE_DATE = date(2026, 9, 30)


def test_runtime_plans_full_trading_day_services(tmp_path: Path) -> None:
    now = datetime(2026, 9, 30, 9, 30, tzinfo=_ZONE)
    plans = _plans(tmp_path, now=now, is_open=True)

    assert {plan.name for plan in plans} == {
        "quote_watcher",
        "monitor",
        "notification_worker",
        "market_analysis",
        "market_status",
        "minute_sealer",
    }
    quote_plan = next(plan for plan in plans if plan.name == "quote_watcher")
    assert "600000.SH" in quote_plan.command
    assert "000001.SZ" in quote_plan.command
    assert quote_plan.stop_at == datetime(2026, 9, 30, 15, 0, 30, tzinfo=_ZONE)
    sealer = next(plan for plan in plans if plan.name == "minute_sealer")
    assert sealer.result_marker is not None
    assert sealer.restart_policy == "on_failure"


def test_runtime_plans_separate_holdings_card_when_configured(tmp_path: Path) -> None:
    now = datetime(2026, 9, 30, 9, 30, tzinfo=_ZONE)
    settings = Settings(data_dir=tmp_path / "data", _env_file=None)
    status = ChinaAStockCalendar({_TRADE_DATE: True}).status_at(now)
    holdings = tmp_path / "holdings.json"
    plans = build_runtime_service_plans(
        settings=settings,
        status=status,
        local_now=now,
        project_root=tmp_path,
        pool_file=tmp_path / "pool.json",
        industry_map_file=tmp_path / "industry.json",
        symbols=("600000.SH", "002409.SZ"),
        market_benchmark="510300.SH",
        analysis_window_minutes=5,
        status_interval_minutes=15,
        holdings_file=holdings,
    )
    card = next(plan for plan in plans if plan.name == "holdings_report")
    assert card.command[1].endswith("watch_holdings.py")
    assert card.command[card.command.index("--holdings-file") + 1] == str(holdings)
    assert card.command[card.command.index("--interval-minutes") + 1] == "5"
    assert card.stop_at == datetime(2026, 9, 30, 15, 1, tzinfo=_ZONE)
    assert "002409.SZ" in next(plan for plan in plans if plan.name == "quote_watcher").command


def test_runtime_does_nothing_on_closed_date(tmp_path: Path) -> None:
    plans = _plans(
        tmp_path,
        now=datetime(2026, 9, 30, 10, 0, tzinfo=_ZONE),
        is_open=False,
    )

    assert plans == ()


def test_runtime_stops_collection_and_can_catch_up_sealing(tmp_path: Path) -> None:
    now = datetime(2026, 9, 30, 16, 0, tzinfo=_ZONE)
    plans = _plans(tmp_path, now=now, is_open=True)

    assert {plan.name for plan in plans} == {
        "monitor",
        "notification_worker",
        "minute_sealer",
    }
    marker = next(plan.result_marker for plan in plans if plan.name == "minute_sealer")
    assert marker is not None
    marker.parent.mkdir(parents=True)
    marker.write_text(json.dumps({"terminal": True, "success": False}), encoding="utf-8")

    assert {plan.name for plan in _plans(tmp_path, now=now, is_open=True)} == {
        "monitor",
        "notification_worker",
        "daily_acceptance",
    }
    acceptance_marker = marker.with_name("daily-acceptance-result.json")
    acceptance_marker.write_text(json.dumps({"terminal": True, "success": True}), encoding="utf-8")

    assert _plans(tmp_path, now=now, is_open=True) == ()


def test_daily_cache_starts_at_1730_and_stops_after_terminal_marker(tmp_path: Path) -> None:
    before = datetime(2026, 9, 30, 17, 29, tzinfo=_ZONE)
    start = datetime(2026, 9, 30, 17, 30, tzinfo=_ZONE)
    assert "daily_cache" not in {plan.name for plan in _plans(tmp_path, now=before, is_open=True)}
    plans = _plans(tmp_path, now=start, is_open=True)
    cache = next(plan for plan in plans if plan.name == "daily_cache")
    assert cache.command[-2:] == ("--pool-file", str(tmp_path / "pool.json"))
    assert cache.stop_at == datetime(2026, 9, 30, 23, 50, tzinfo=_ZONE)
    assert cache.result_marker is not None
    cache.result_marker.parent.mkdir(parents=True, exist_ok=True)
    cache.result_marker.write_text(json.dumps({"terminal": True, "success": True}))
    assert "daily_cache" not in {plan.name for plan in _plans(tmp_path, now=start, is_open=True)}


def test_retryable_sealer_marker_starts_another_attempt(tmp_path: Path) -> None:
    now = datetime(2026, 9, 30, 16, 0, tzinfo=_ZONE)
    marker = tmp_path / "data" / "reports" / "runtime" / "2026-09-30" / "minute-sealer-result.json"
    marker.parent.mkdir(parents=True)
    marker.write_text(
        json.dumps({"terminal": False, "retryable": True, "attempt_count": 1}),
        encoding="utf-8",
    )

    assert "minute_sealer" in {plan.name for plan in _plans(tmp_path, now=now, is_open=True)}
    assert "daily_acceptance" not in {plan.name for plan in _plans(tmp_path, now=now, is_open=True)}


def test_acceptance_can_run_after_sealer_deadline(tmp_path: Path) -> None:
    now = datetime(2026, 9, 30, 23, 51, tzinfo=_ZONE)
    marker = tmp_path / "data" / "reports" / "runtime" / "2026-09-30" / "minute-sealer-result.json"
    marker.parent.mkdir(parents=True)
    marker.write_text(
        json.dumps({"terminal": True, "success": False, "failure_kind": "deadline_exceeded"}),
        encoding="utf-8",
    )

    assert {plan.name for plan in _plans(tmp_path, now=now, is_open=True)} == {
        "monitor",
        "notification_worker",
        "daily_acceptance",
    }


def test_unknown_calendar_starts_notification_worker(tmp_path: Path) -> None:
    now = datetime(2026, 9, 30, 10, 0, tzinfo=_ZONE)
    status = ChinaAStockCalendar({}).status_at(now)
    plans = build_runtime_service_plans(
        settings=Settings(data_dir=tmp_path / "data", _env_file=None),
        status=status,
        local_now=now,
        project_root=tmp_path,
        pool_file=tmp_path / "pool.json",
        industry_map_file=tmp_path / "industry.json",
        symbols=("600000.SH",),
        market_benchmark="510300.SH",
        analysis_window_minutes=15,
        status_interval_minutes=15,
    )

    assert {plan.name for plan in plans} == {"notification_worker"}


def test_runtime_has_no_services_before_preflight(tmp_path: Path) -> None:
    plans = _plans(
        tmp_path,
        now=datetime(2026, 9, 30, 8, 30, tzinfo=_ZONE),
        is_open=True,
    )

    assert plans == ()


@pytest.mark.parametrize(
    ("hour", "minute", "second", "expected"),
    [
        (8, 50, 0, {"monitor", "notification_worker"}),
        (9, 14, 29, {"monitor", "notification_worker"}),
        (
            9,
            14,
            30,
            {"monitor", "notification_worker", "minute_sealer"},
        ),
        (9, 29, 59, {"monitor", "notification_worker", "minute_sealer"}),
        (
            9,
            30,
            0,
            {
                "quote_watcher",
                "monitor",
                "notification_worker",
                "market_analysis",
                "market_status",
                "minute_sealer",
            },
        ),
        (
            15,
            0,
            29,
            {
                "quote_watcher",
                "monitor",
                "notification_worker",
                "market_analysis",
                "market_status",
                "minute_sealer",
            },
        ),
        (
            15,
            0,
            30,
            {"monitor", "notification_worker", "market_analysis", "market_status", "minute_sealer"},
        ),
        (15, 1, 0, {"monitor", "notification_worker", "minute_sealer"}),
        (15, 15, 0, {"monitor", "notification_worker", "minute_sealer"}),
    ],
)
def test_runtime_service_boundaries(
    tmp_path: Path, hour: int, minute: int, second: int, expected: set[str]
) -> None:
    now = datetime(2026, 9, 30, hour, minute, second, tzinfo=_ZONE)

    assert {plan.name for plan in _plans(tmp_path, now=now, is_open=True)} == expected


def test_terminal_result_keeps_notification_worker_for_two_minute_drain(
    tmp_path: Path,
) -> None:
    report_dir = tmp_path / "data" / "reports" / "runtime" / "2026-09-30"
    report_dir.mkdir(parents=True)
    completed_at = datetime(2026, 9, 30, 16, 0, tzinfo=_ZONE)
    for name in ("minute-sealer-result.json", "daily-acceptance-result.json"):
        marker = report_dir / name
        marker.write_text(json.dumps({"terminal": True, "success": True}), encoding="utf-8")
        timestamp = completed_at.timestamp()
        os.utime(marker, (timestamp, timestamp))

    during_drain = _plans(tmp_path, now=datetime(2026, 9, 30, 16, 1, tzinfo=_ZONE), is_open=True)
    after_drain = _plans(tmp_path, now=datetime(2026, 9, 30, 16, 3, tzinfo=_ZONE), is_open=True)

    assert {plan.name for plan in during_drain} == {"notification_worker"}
    assert during_drain[0].stop_at == datetime(2026, 9, 30, 16, 2, tzinfo=_ZONE)
    assert after_drain == ()


def test_macos_launch_agent_runs_one_keepalive_supervisor(tmp_path: Path) -> None:
    python = tmp_path / ".venv" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.write_text("", encoding="utf-8")
    for relative in (
        "token",
        "config/stock_pools/initial-v1/pool.json",
        "config/stock_pools/initial-v1/all_symbols.txt",
        "config/stock_pools/initial-v1/industry_benchmarks.json",
    ):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("test\n", encoding="utf-8")

    payload = build_launch_agent(tmp_path)

    assert payload["RunAtLoad"] is True
    assert payload["KeepAlive"] is True
    assert payload["WorkingDirectory"] == str(tmp_path.resolve())
    assert payload["ProgramArguments"][1:4] == ["-m", "regimebeacon", "runtime"]
    assert "token" not in " ".join(payload["ProgramArguments"])


def _plans(tmp_path: Path, *, now: datetime, is_open: bool) -> tuple[RuntimeServicePlan, ...]:
    settings = Settings(data_dir=tmp_path / "data", _env_file=None)
    status = ChinaAStockCalendar({_TRADE_DATE: is_open}).status_at(now)
    return build_runtime_service_plans(
        settings=settings,
        status=status,
        local_now=now,
        project_root=tmp_path,
        pool_file=tmp_path / "pool.json",
        industry_map_file=tmp_path / "industry.json",
        symbols=("600000.SH", "000001.SZ"),
        market_benchmark="510300.SH",
        analysis_window_minutes=15,
        status_interval_minutes=15,
    )
