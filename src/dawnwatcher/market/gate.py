"""Self-refreshing runtime gate backed by cached Tushare trade_cal data."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from sqlalchemy.orm import Session, sessionmaker

from dawnwatcher.market.sessions import ChinaAStockCalendar, MarketSessionStatus
from dawnwatcher.providers.tushare_calendar import (
    TushareCalendarClient,
    read_tushare_token,
)
from dawnwatcher.storage.trading_calendar import (
    TradingCalendarCoverage,
    calendar_coverage,
    load_market_calendar,
    synchronize_trading_calendar,
)

logger = logging.getLogger(__name__)


class TushareTradingSessionGate:
    """Refresh Tushare at low frequency and classify ticks from local memory."""

    def __init__(
        self,
        factory: sessionmaker[Session],
        *,
        timezone: str,
        exchange: str,
        token_file: Path,
        api_url: str,
        timeout_seconds: float,
        refresh_hours: int,
    ) -> None:
        self._factory = factory
        self._timezone = timezone
        self._exchange = exchange
        self._token_file = token_file
        self._api_url = api_url
        self._timeout_seconds = timeout_seconds
        self._refresh_interval = timedelta(hours=refresh_hours)
        self._retry_interval = timedelta(minutes=15)
        self._calendar = ChinaAStockCalendar({}, timezone=timezone)
        self._coverage = TradingCalendarCoverage(exchange, None, None, 0, None)
        self._next_refresh_at = datetime.min.replace(tzinfo=UTC)
        self._refresh_lock = asyncio.Lock()
        self._reload()

    @property
    def coverage(self) -> TradingCalendarCoverage:
        return self._coverage

    async def status_at(self, observed_at: datetime) -> MarketSessionStatus:
        """Refresh when stale or uncovered, then fail closed if the date remains unknown."""
        preliminary = self._calendar.status_at(observed_at)
        now = datetime.now(UTC)
        if (
            not preliminary.calendar_date_known or self._cache_is_stale(now)
        ) and now >= self._next_refresh_at:
            await self._try_refresh(preliminary.trade_date, now=now)
        return self._calendar.status_at(observed_at)

    async def force_refresh(self, *, start_date: date, end_date: date) -> int:
        """Synchronize an explicit range and return the resulting cached row count."""
        async with self._refresh_lock:
            token = read_tushare_token(self._token_file)
            async with TushareCalendarClient(
                token=token,
                api_url=self._api_url,
                timeout_seconds=self._timeout_seconds,
            ) as client:
                await synchronize_trading_calendar(
                    self._factory,
                    client,
                    exchange=self._exchange,
                    start_date=start_date,
                    end_date=end_date,
                )
            self._reload()
            self._next_refresh_at = datetime.now(UTC) + self._refresh_interval
            return self._coverage.row_count

    async def _try_refresh(self, target_date: date, *, now: datetime) -> None:
        try:
            await self.force_refresh(
                start_date=date(target_date.year, 1, 1),
                end_date=date(target_date.year, 12, 31),
            )
        except Exception as exc:
            self._next_refresh_at = now + self._retry_interval
            logger.warning(
                "Tushare calendar refresh failed; retaining local cache",
                extra={"error_type": type(exc).__name__, "exchange": self._exchange},
            )

    def _cache_is_stale(self, now: datetime) -> bool:
        fetched_at = self._coverage.last_fetched_at
        if fetched_at is None:
            return True
        return now - fetched_at.astimezone(UTC) >= self._refresh_interval

    def _reload(self) -> None:
        with self._factory() as session:
            self._coverage = calendar_coverage(session, exchange=self._exchange)
            self._calendar = load_market_calendar(
                session,
                exchange=self._exchange,
                timezone=self._timezone,
            )
