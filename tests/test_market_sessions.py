"""Trading-calendar and A-share auction-phase boundary tests."""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from regimebeacon.market import AuctionMode, ChinaAStockCalendar, MarketPhase

_SHANGHAI = ZoneInfo("Asia/Shanghai")


@pytest.mark.parametrize(
    ("clock", "expected_phase", "auction_mode", "collect_quotes"),
    [
        ((9, 14, 59, 999999), MarketPhase.PRE_OPEN, AuctionMode.NONE, False),
        ((9, 15, 0, 0), MarketPhase.OPENING_CALL_AUCTION, AuctionMode.CALL, True),
        ((9, 25, 0, 0), MarketPhase.OPENING_CALL_AUCTION, AuctionMode.CALL, True),
        ((9, 25, 0, 1), MarketPhase.OPENING_PAUSE, AuctionMode.NONE, False),
        ((9, 30, 0, 0), MarketPhase.MORNING_CONTINUOUS, AuctionMode.CONTINUOUS, True),
        ((11, 30, 0, 0), MarketPhase.MORNING_CONTINUOUS, AuctionMode.CONTINUOUS, True),
        ((11, 30, 0, 1), MarketPhase.MIDDAY_BREAK, AuctionMode.NONE, False),
        ((13, 0, 0, 0), MarketPhase.AFTERNOON_CONTINUOUS, AuctionMode.CONTINUOUS, True),
        ((14, 57, 0, 0), MarketPhase.CLOSING_CALL_AUCTION, AuctionMode.CALL, True),
        ((15, 0, 0, 0), MarketPhase.CLOSING_CALL_AUCTION, AuctionMode.CALL, True),
        ((15, 0, 0, 1), MarketPhase.POST_CLOSE, AuctionMode.NONE, False),
    ],
)
def test_session_boundaries_distinguish_call_and_continuous_auction(
    clock: tuple[int, int, int, int],
    expected_phase: MarketPhase,
    auction_mode: AuctionMode,
    collect_quotes: bool,
) -> None:
    trade_date = date(2026, 9, 24)
    calendar = ChinaAStockCalendar({trade_date: True})
    observed_at = datetime(
        trade_date.year, trade_date.month, trade_date.day, *clock, tzinfo=_SHANGHAI
    )

    status = calendar.status_at(observed_at)

    assert status.phase is expected_phase
    assert status.auction_mode is auction_mode
    assert status.collect_quotes is collect_quotes


def test_tushare_closed_day_blocks_every_session() -> None:
    closed_date = date(2026, 9, 25)
    calendar = ChinaAStockCalendar({closed_date: False})

    status = calendar.status_at(datetime(2026, 9, 25, 10, 0, tzinfo=_SHANGHAI))

    assert status.phase is MarketPhase.NON_TRADING_DAY
    assert status.calendar_date_known is True
    assert status.is_trading_day is False
    assert status.collect_quotes is False


def test_missing_tushare_date_fails_closed() -> None:
    calendar = ChinaAStockCalendar({})

    status = calendar.status_at(datetime(2027, 1, 4, 10, 0, tzinfo=_SHANGHAI))

    assert status.phase is MarketPhase.CALENDAR_UNKNOWN
    assert status.calendar_date_known is False
    assert status.collect_quotes is False


def test_naive_observed_time_is_rejected() -> None:
    calendar = ChinaAStockCalendar({date(2026, 9, 24): True})

    with pytest.raises(ValueError, match="timezone-aware"):
        calendar.status_at(datetime(2026, 9, 24, 10, 0))
