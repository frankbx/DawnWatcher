"""China A-share intraday-session classification over a cached trading calendar."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo


class AuctionMode(StrEnum):
    """Matching mechanism active in a market phase."""

    NONE = "none"
    CALL = "call_auction"
    CONTINUOUS = "continuous_auction"


class MarketPhase(StrEnum):
    """Detailed A-share market phase for quote gating and downstream routing."""

    CALENDAR_UNKNOWN = "calendar_unknown"
    NON_TRADING_DAY = "non_trading_day"
    PRE_OPEN = "pre_open"
    OPENING_CALL_AUCTION = "opening_call_auction"
    OPENING_PAUSE = "opening_pause"
    MORNING_CONTINUOUS = "morning_continuous"
    MIDDAY_BREAK = "midday_break"
    AFTERNOON_CONTINUOUS = "afternoon_continuous"
    CLOSING_CALL_AUCTION = "closing_call_auction"
    CLOSING_FINAL_QUOTE = "closing_final_quote"
    POST_CLOSE = "post_close"

    @property
    def auction_mode(self) -> AuctionMode:
        if self in {self.OPENING_CALL_AUCTION, self.CLOSING_CALL_AUCTION}:
            return AuctionMode.CALL
        if self in {self.MORNING_CONTINUOUS, self.AFTERNOON_CONTINUOUS}:
            return AuctionMode.CONTINUOUS
        return AuctionMode.NONE

    @property
    def collects_quotes(self) -> bool:
        return self.auction_mode is not AuctionMode.NONE or self is self.CLOSING_FINAL_QUOTE

    @property
    def is_continuous(self) -> bool:
        return self in {self.MORNING_CONTINUOUS, self.AFTERNOON_CONTINUOUS}


@dataclass(frozen=True, slots=True)
class TradingCalendarRecord:
    """One normalized Tushare trade_cal row."""

    exchange: str
    cal_date: date
    is_open: bool
    pretrade_date: date | None


@dataclass(frozen=True, slots=True)
class MarketSessionStatus:
    """Classification of one instant in the configured exchange timezone."""

    observed_at: datetime
    trade_date: date
    phase: MarketPhase
    is_trading_day: bool
    calendar_date_known: bool
    reason: str

    @property
    def auction_mode(self) -> AuctionMode:
        return self.phase.auction_mode

    @property
    def collect_quotes(self) -> bool:
        return self.calendar_date_known and self.is_trading_day and self.phase.collects_quotes

    @property
    def collect_production_quotes(self) -> bool:
        """Opening-call samples are diagnostic only; keep the official close."""
        return self.collect_quotes and self.phase is not MarketPhase.OPENING_CALL_AUCTION

    def to_dict(self) -> dict[str, Any]:
        return {
            "observed_at": self.observed_at.isoformat(),
            "trade_date": self.trade_date.isoformat(),
            "phase": self.phase.value,
            "auction_mode": self.auction_mode.value,
            "is_trading_day": self.is_trading_day,
            "calendar_date_known": self.calendar_date_known,
            "collect_quotes": self.collect_quotes,
            "collect_production_quotes": self.collect_production_quotes,
            "reason": self.reason,
        }


class ChinaAStockCalendar:
    """Classify A-share sessions using locally cached Tushare calendar dates."""

    def __init__(
        self,
        trading_days: Mapping[date, bool],
        *,
        timezone: str = "Asia/Shanghai",
    ) -> None:
        self.timezone = ZoneInfo(timezone)
        self._trading_days = dict(trading_days)

    @property
    def coverage(self) -> tuple[date | None, date | None]:
        if not self._trading_days:
            return None, None
        return min(self._trading_days), max(self._trading_days)

    def is_trading_day(self, value: date) -> bool | None:
        """Return None when Tushare has not supplied this calendar date."""
        return self._trading_days.get(value)

    def status_at(self, observed_at: datetime) -> MarketSessionStatus:
        """Return the official trading phase for one timezone-aware instant."""
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        local = observed_at.astimezone(self.timezone)
        trade_date = local.date()
        is_trading_day = self.is_trading_day(trade_date)
        if is_trading_day is None:
            return MarketSessionStatus(
                observed_at=local,
                trade_date=trade_date,
                phase=MarketPhase.CALENDAR_UNKNOWN,
                is_trading_day=False,
                calendar_date_known=False,
                reason="date is missing from the local Tushare trade_cal cache",
            )
        if not is_trading_day:
            return MarketSessionStatus(
                observed_at=local,
                trade_date=trade_date,
                phase=MarketPhase.NON_TRADING_DAY,
                is_trading_day=False,
                calendar_date_known=True,
                reason="Tushare trade_cal marks this date closed",
            )
        phase = _phase_at(local.time())
        return MarketSessionStatus(
            observed_at=local,
            trade_date=trade_date,
            phase=phase,
            is_trading_day=True,
            calendar_date_known=True,
            reason=_PHASE_REASONS[phase],
        )


def _phase_at(value: time) -> MarketPhase:
    if value < time(9, 15):
        return MarketPhase.PRE_OPEN
    if value <= time(9, 25):
        return MarketPhase.OPENING_CALL_AUCTION
    if value < time(9, 30):
        return MarketPhase.OPENING_PAUSE
    if value <= time(11, 30):
        return MarketPhase.MORNING_CONTINUOUS
    if value < time(13, 0):
        return MarketPhase.MIDDAY_BREAK
    if value < time(14, 57):
        return MarketPhase.AFTERNOON_CONTINUOUS
    if value <= time(15, 0):
        return MarketPhase.CLOSING_CALL_AUCTION
    if value < time(15, 0, 30):
        return MarketPhase.CLOSING_FINAL_QUOTE
    return MarketPhase.POST_CLOSE


_PHASE_REASONS = {
    MarketPhase.PRE_OPEN: "before the opening call auction",
    MarketPhase.OPENING_CALL_AUCTION: "opening call auction",
    MarketPhase.OPENING_PAUSE: "pause between the opening call and continuous auction",
    MarketPhase.MORNING_CONTINUOUS: "morning continuous auction",
    MarketPhase.MIDDAY_BREAK: "midday trading break",
    MarketPhase.AFTERNOON_CONTINUOUS: "afternoon continuous auction",
    MarketPhase.CLOSING_CALL_AUCTION: "closing call auction",
    MarketPhase.CLOSING_FINAL_QUOTE: "brief final-price quote capture after the close",
    MarketPhase.POST_CLOSE: "after the closing call auction",
}
