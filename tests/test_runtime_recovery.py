"""Failure-path tests for unattended runtime persistence and alerting."""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from regimebeacon.config import Settings
from regimebeacon.market import ChinaAStockCalendar
from regimebeacon.market.gate import TushareTradingSessionGate
from regimebeacon.runtime import RuntimeServicePlan, TradingDayRuntimeSupervisor
from regimebeacon.storage.models import NotificationOutbox

_ZONE = ZoneInfo("Asia/Shanghai")
_DATE = date(2026, 9, 28)


def test_transient_sealer_attempts_survive_supervisor_restarts(
    database_settings: Settings,
    session_factory_fixture: sessionmaker[Session],
    tmp_path: Path,
) -> None:
    now = datetime(2026, 9, 28, 16, 0, tzinfo=_ZONE)
    marker = tmp_path / "runtime" / "minute-sealer-result.json"
    plan = RuntimeServicePlan(
        name="minute_sealer",
        trade_date=_DATE,
        command=(sys.executable, "-c", "import sys; sys.exit(75)"),
        log_path=tmp_path / "runtime" / "minute-sealer.log",
        restart_policy="on_failure",
        stop_at=now + timedelta(hours=1),
        result_marker=marker,
    )

    async def exercise() -> None:
        for attempt in range(1, 4):
            supervisor = _supervisor(database_settings, session_factory_fixture, tmp_path)
            assert supervisor._attempt_count(plan) == attempt - 1
            await supervisor._start_process(plan, now)
            await supervisor._managed[plan.name].process.wait()
            await supervisor._collect_exited(now + timedelta(seconds=1))
            result = json.loads(marker.read_text(encoding="utf-8"))
            assert result["attempt_count"] == attempt
            assert result["terminal"] is (attempt == 3)
            assert result["retryable"] is (attempt < 3)

    asyncio.run(exercise())
    with session_factory_fixture() as session:
        alerts = list(
            session.scalars(
                select(NotificationOutbox).where(
                    NotificationOutbox.event_type == "runtime.daily_service.failed"
                )
            )
        )
    assert len(alerts) == 1
    assert alerts[0].payload["details"]["attempt_count"] == 3


def test_orphaned_running_marker_does_not_consume_retry_budget(
    database_settings: Settings,
    session_factory_fixture: sessionmaker[Session],
    tmp_path: Path,
) -> None:
    marker = tmp_path / "runtime" / "minute-sealer-result.json"
    marker.parent.mkdir(parents=True)
    marker.write_text(
        json.dumps(
            {
                "service": "minute_sealer",
                "trade_date": _DATE.isoformat(),
                "return_code": None,
                "attempt_count": 2,
                "terminal": False,
                "failure_kind": None,
                "retryable": True,
            }
        ),
        encoding="utf-8",
    )
    plan = RuntimeServicePlan(
        name="minute_sealer",
        trade_date=_DATE,
        command=(sys.executable, "-c", "pass"),
        log_path=tmp_path / "runtime" / "minute-sealer.log",
        restart_policy="on_failure",
        stop_at=datetime(2026, 9, 28, 23, 50, tzinfo=_ZONE),
        result_marker=marker,
    )

    supervisor = _supervisor(database_settings, session_factory_fixture, tmp_path)
    assert supervisor._attempt_count(plan) == 1


def test_unknown_calendar_alert_and_recovery_are_idempotent(
    database_settings: Settings,
    session_factory_fixture: sessionmaker[Session],
    tmp_path: Path,
) -> None:
    supervisor = _supervisor(database_settings, session_factory_fixture, tmp_path)
    now = datetime(2026, 9, 28, 10, 0, tzinfo=_ZONE)
    unknown = ChinaAStockCalendar({}).status_at(now)
    recovered = ChinaAStockCalendar({_DATE: True}).status_at(now)

    supervisor._sync_calendar_notice(unknown, now)
    supervisor._sync_calendar_notice(unknown, now)
    supervisor._sync_calendar_notice(recovered, now)
    supervisor._sync_calendar_notice(recovered, now)

    with session_factory_fixture() as session:
        events = list(
            session.scalars(select(NotificationOutbox).order_by(NotificationOutbox.event_type))
        )
    assert [item.event_type for item in events] == [
        "runtime.calendar.recovered",
        "runtime.calendar.unknown",
    ]


def test_intentional_shutdown_does_not_consume_sealer_attempt(
    database_settings: Settings,
    session_factory_fixture: sessionmaker[Session],
    tmp_path: Path,
) -> None:
    now = datetime(2026, 9, 28, 16, 0, tzinfo=_ZONE)
    marker = tmp_path / "runtime" / "minute-sealer-result.json"
    plan = RuntimeServicePlan(
        name="minute_sealer",
        trade_date=_DATE,
        command=(sys.executable, "-c", "import time; time.sleep(60)"),
        log_path=tmp_path / "runtime" / "minute-sealer.log",
        restart_policy="on_failure",
        stop_at=now + timedelta(hours=1),
        result_marker=marker,
    )

    async def exercise() -> None:
        supervisor = _supervisor(database_settings, session_factory_fixture, tmp_path)
        await supervisor._start_process(plan, now)
        await supervisor._stop_process("minute_sealer", reason="runtime supervisor shutdown")

    asyncio.run(exercise())
    result = json.loads(marker.read_text(encoding="utf-8"))
    assert result["attempt_count"] == 0
    assert result["terminal"] is False
    assert result["failure_kind"] == "interrupted"
    restarted = _supervisor(database_settings, session_factory_fixture, tmp_path)
    assert restarted._attempt_count(plan) == 0


def test_sealer_deadline_is_terminal_and_alerted(
    database_settings: Settings,
    session_factory_fixture: sessionmaker[Session],
    tmp_path: Path,
) -> None:
    now = datetime(2026, 9, 28, 23, 50, tzinfo=_ZONE)
    marker = tmp_path / "runtime" / "minute-sealer-result.json"
    plan = RuntimeServicePlan(
        name="minute_sealer",
        trade_date=_DATE,
        command=(sys.executable, "-c", "import time; time.sleep(60)"),
        log_path=tmp_path / "runtime" / "minute-sealer.log",
        restart_policy="on_failure",
        stop_at=now,
        result_marker=marker,
    )

    async def exercise() -> None:
        supervisor = _supervisor(database_settings, session_factory_fixture, tmp_path)
        supervisor._now_provider = lambda: now
        await supervisor._start_process(plan, now - timedelta(seconds=1))
        await supervisor._stop_process("minute_sealer", reason="outside scheduled runtime window")

    asyncio.run(exercise())
    result = json.loads(marker.read_text(encoding="utf-8"))
    assert result["terminal"] is True
    assert result["failure_kind"] == "deadline_exceeded"
    with session_factory_fixture() as session:
        alert = session.scalar(
            select(NotificationOutbox).where(
                NotificationOutbox.event_type == "runtime.daily_service.failed"
            )
        )
    assert alert is not None
    assert alert.payload["details"]["return_code"] == 124


def _supervisor(
    settings: Settings,
    factory: sessionmaker[Session],
    root: Path,
) -> TradingDayRuntimeSupervisor:
    pool = root / "pool.json"
    industry = root / "industry.json"
    symbols = root / "symbols.txt"
    pool.write_text("{}", encoding="utf-8")
    industry.write_text("{}", encoding="utf-8")
    symbols.write_text("600000.SH\n", encoding="utf-8")
    gate = TushareTradingSessionGate(
        factory,
        timezone="Asia/Shanghai",
        exchange="SSE",
        token_file=root / "missing-token",
        api_url="https://api.tushare.test",
        timeout_seconds=2,
        refresh_hours=24,
    )
    return TradingDayRuntimeSupervisor(
        settings,
        factory,
        gate,
        project_root=root,
        pool_file=pool,
        industry_map_file=industry,
        symbols_file=symbols,
        now_provider=lambda: datetime(2026, 9, 28, 16, 0, tzinfo=_ZONE),
    )
