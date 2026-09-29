"""Persistence and synchronization helpers for the Tushare trading calendar."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session, sessionmaker

from regimebeacon.market import ChinaAStockCalendar, TradingCalendarRecord
from regimebeacon.providers.tushare_calendar import TushareCalendarClient
from regimebeacon.storage.models import TradingCalendarDay


@dataclass(frozen=True, slots=True)
class TradingCalendarCoverage:
    """Local Tushare calendar coverage and refresh metadata."""

    exchange: str
    start_date: date | None
    end_date: date | None
    row_count: int
    last_fetched_at: datetime | None

    def includes(self, value: date) -> bool:
        return (
            self.start_date is not None
            and self.end_date is not None
            and (self.start_date <= value <= self.end_date)
        )


def calendar_coverage(session: Session, *, exchange: str) -> TradingCalendarCoverage:
    """Return cached range and latest upstream refresh time."""
    start_date, end_date, row_count, last_fetched_at = session.execute(
        select(
            func.min(TradingCalendarDay.cal_date),
            func.max(TradingCalendarDay.cal_date),
            func.count(TradingCalendarDay.id),
            func.max(TradingCalendarDay.source_fetched_at),
        ).where(TradingCalendarDay.exchange == exchange)
    ).one()
    return TradingCalendarCoverage(
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
        row_count=int(row_count),
        last_fetched_at=last_fetched_at,
    )


def load_market_calendar(
    session: Session,
    *,
    exchange: str,
    timezone: str,
) -> ChinaAStockCalendar:
    """Load the local Tushare calendar into an immutable runtime gate."""
    rows = session.execute(
        select(TradingCalendarDay.cal_date, TradingCalendarDay.is_open).where(
            TradingCalendarDay.exchange == exchange
        )
    ).all()
    return ChinaAStockCalendar(
        {cal_date: is_open for cal_date, is_open in rows},
        timezone=timezone,
    )


def upsert_calendar_records(
    session: Session,
    records: tuple[TradingCalendarRecord, ...],
    *,
    fetched_at: datetime,
) -> int:
    """Atomically replace cached fields for returned Tushare calendar rows."""
    if not records:
        return 0
    statement = insert(TradingCalendarDay).values(
        [
            {
                "exchange": record.exchange,
                "cal_date": record.cal_date,
                "is_open": record.is_open,
                "pretrade_date": record.pretrade_date,
                "source": "tushare",
                "source_fetched_at": fetched_at,
            }
            for record in records
        ]
    )
    statement = statement.on_conflict_do_update(
        index_elements=["exchange", "cal_date"],
        set_={
            "is_open": statement.excluded.is_open,
            "pretrade_date": statement.excluded.pretrade_date,
            "source": statement.excluded.source,
            "source_fetched_at": statement.excluded.source_fetched_at,
            "updated_at": datetime.now(UTC),
        },
    )
    session.execute(statement)
    return len(records)


async def synchronize_trading_calendar(
    factory: sessionmaker[Session],
    client: TushareCalendarClient,
    *,
    exchange: str,
    start_date: date,
    end_date: date,
) -> TradingCalendarCoverage:
    """Fetch one Tushare range, validate it, and commit it as a single transaction."""
    records = await client.fetch_calendar(
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
    )
    fetched_at = datetime.now(UTC)
    with factory.begin() as session:
        upsert_calendar_records(session, records, fetched_at=fetched_at)
        return calendar_coverage(session, exchange=exchange)
