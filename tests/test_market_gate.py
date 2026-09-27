"""Cached-calendar runtime gate behavior."""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from pathlib import Path

from sqlalchemy.orm import Session, sessionmaker

from dawnwatcher.market import MarketPhase, TradingCalendarRecord
from dawnwatcher.market.gate import TushareTradingSessionGate
from dawnwatcher.storage.trading_calendar import upsert_calendar_records


def test_fresh_cached_date_does_not_require_token_file(
    tmp_path: Path,
    session_factory_fixture: sessionmaker[Session],
) -> None:
    trade_date = date(2026, 9, 24)
    with session_factory_fixture.begin() as session:
        upsert_calendar_records(
            session,
            (TradingCalendarRecord("SSE", trade_date, True, date(2026, 9, 23)),),
            fetched_at=datetime.now(UTC),
        )
    gate = TushareTradingSessionGate(
        session_factory_fixture,
        timezone="Asia/Shanghai",
        exchange="SSE",
        token_file=tmp_path / "missing-token",
        api_url="https://api.tushare.test",
        timeout_seconds=2,
        refresh_hours=24,
    )

    status = asyncio.run(gate.status_at(datetime(2026, 9, 24, 2, 0, tzinfo=UTC)))

    assert status.phase is MarketPhase.MORNING_CONTINUOUS
    assert status.collect_quotes is True


def test_unknown_date_fails_closed_when_refresh_is_unavailable(
    tmp_path: Path,
    session_factory_fixture: sessionmaker[Session],
) -> None:
    gate = TushareTradingSessionGate(
        session_factory_fixture,
        timezone="Asia/Shanghai",
        exchange="SSE",
        token_file=tmp_path / "missing-token",
        api_url="https://api.tushare.test",
        timeout_seconds=2,
        refresh_hours=24,
    )

    status = asyncio.run(gate.status_at(datetime(2027, 1, 4, 2, 0, tzinfo=UTC)))

    assert status.phase is MarketPhase.CALENDAR_UNKNOWN
    assert status.collect_quotes is False
