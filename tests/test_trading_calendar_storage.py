"""SQLite persistence tests for cached Tushare trade_cal rows."""

from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy.orm import Session, sessionmaker

from dawnwatcher.market import MarketPhase, TradingCalendarRecord
from dawnwatcher.storage.trading_calendar import (
    calendar_coverage,
    load_market_calendar,
    upsert_calendar_records,
)


def test_calendar_rows_are_upserted_and_loaded(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    fetched_at = datetime(2026, 9, 20, tzinfo=UTC)
    records = (
        TradingCalendarRecord("SSE", date(2026, 9, 24), True, date(2026, 9, 23)),
        TradingCalendarRecord("SSE", date(2026, 9, 25), False, date(2026, 9, 24)),
    )
    with session_factory_fixture.begin() as session:
        assert upsert_calendar_records(session, records, fetched_at=fetched_at) == 2
        coverage = calendar_coverage(session, exchange="SSE")
        calendar = load_market_calendar(
            session,
            exchange="SSE",
            timezone="Asia/Shanghai",
        )

    assert coverage.start_date == date(2026, 9, 24)
    assert coverage.end_date == date(2026, 9, 25)
    assert coverage.row_count == 2
    assert (
        calendar.status_at(
            datetime(2026, 9, 24, 2, 0, tzinfo=UTC).astimezone(calendar.timezone)
        ).phase
        is MarketPhase.MORNING_CONTINUOUS
    )

    corrected = (TradingCalendarRecord("SSE", date(2026, 9, 25), True, date(2026, 9, 24)),)
    with session_factory_fixture.begin() as session:
        upsert_calendar_records(session, corrected, fetched_at=datetime(2026, 9, 21, tzinfo=UTC))
        calendar = load_market_calendar(
            session,
            exchange="SSE",
            timezone="Asia/Shanghai",
        )

    assert calendar.is_trading_day(date(2026, 9, 25)) is True
