"""Trading-day runtime planning and macOS launch-agent tests."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

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
    assert sealer.restart_policy == "never"


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
    marker.write_text("{}\n", encoding="utf-8")

    assert _plans(tmp_path, now=now, is_open=True) == ()


def test_runtime_has_no_services_before_preflight(tmp_path: Path) -> None:
    plans = _plans(
        tmp_path,
        now=datetime(2026, 9, 30, 8, 30, tzinfo=_ZONE),
        is_open=True,
    )

    assert plans == ()


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
