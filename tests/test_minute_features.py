"""Auditable one-minute bars and intraday feature tests."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from dawnwatcher.domain import DataQualityState, Exchange, QuoteProvider
from dawnwatcher.storage.minute_features import build_minute_features, list_minute_features
from dawnwatcher.storage.models import (
    MarketCollectionRun,
    MinuteBar,
    MinuteFeature,
    ProviderQuoteSnapshot,
    ReconciledQuoteSnapshot,
)

_ZONE = ZoneInfo("Asia/Shanghai")
_TRADE_DATE = date(2026, 9, 28)
_STOCK = "600000.SH"
_MARKET = "000001.SH"
_INDUSTRY = "512800.SH"


def test_builds_minute_features_and_is_idempotent(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    with session_factory_fixture.begin() as session:
        _seed_relative_volume_history(session)
        _seed_current_snapshots(session)

    with session_factory_fixture.begin() as session:
        report = build_minute_features(
            session,
            trade_date=_TRADE_DATE,
            timezone="Asia/Shanghai",
            expected_interval_seconds=15,
            market_benchmark_symbol=_MARKET,
            industry_benchmarks={_STOCK: _INDUSTRY},
            relative_volume_lookback_days=2,
            relative_volume_minimum_history_days=2,
            symbols={_STOCK},
        )

    assert report.snapshot_count == 15
    assert report.symbol_count == 3
    assert report.minute_bar_count == 6
    assert report.feature_count == 6

    with session_factory_fixture() as session:
        rows = list_minute_features(
            session,
            trade_date=_TRADE_DATE,
            timezone="Asia/Shanghai",
            symbol=_STOCK,
        )
        target = next(row for row in rows if row["minute_start"].startswith("2026-09-28T09:30"))

    assert target["open"] == "10.000000"
    assert target["high"] == "10.300000"
    assert target["low"] == "9.900000"
    assert target["close"] == "10.200000"
    assert target["incremental_volume_shares"] == 400
    assert target["incremental_amount_cny"] == "4000.0000"
    assert target["vwap"] == "10.00000000"
    assert target["sample_count"] == 4
    assert target["coverage_ratio"] == "1.000000"
    assert Decimal(target["price_trend_bps"]) == Decimal("200")
    assert Decimal(target["vwap_deviation_bps"]) == Decimal("200")
    assert Decimal(target["relative_volume_ratio"]) == Decimal("1.33333333")
    assert target["relative_volume_history_days"] == 2
    assert Decimal(target["market_relative_strength_bps"]) == Decimal("100")
    assert Decimal(target["industry_relative_strength_bps"]) == Decimal("0")
    assert target["quality_flags"] == []

    with session_factory_fixture.begin() as session:
        build_minute_features(
            session,
            trade_date=_TRADE_DATE,
            timezone="Asia/Shanghai",
            expected_interval_seconds=15,
            market_benchmark_symbol=_MARKET,
            industry_benchmarks={_STOCK: _INDUSTRY},
            relative_volume_lookback_days=2,
            relative_volume_minimum_history_days=2,
            symbols={_STOCK},
        )
        current_bar_count = session.scalar(
            select(func.count()).select_from(MinuteBar).where(MinuteBar.trade_date == _TRADE_DATE)
        )
        current_feature_count = session.scalar(
            select(func.count())
            .select_from(MinuteFeature)
            .join(MinuteBar, MinuteFeature.minute_bar_id == MinuteBar.id)
            .where(MinuteBar.trade_date == _TRADE_DATE)
        )

    assert current_bar_count == 6
    assert current_feature_count == 6


def test_missing_inputs_are_null_and_explained(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    with session_factory_fixture.begin() as session:
        _seed_current_snapshots(session, symbols=(_STOCK,))
        build_minute_features(
            session,
            trade_date=_TRADE_DATE,
            timezone="Asia/Shanghai",
            expected_interval_seconds=15,
            relative_volume_lookback_days=5,
            relative_volume_minimum_history_days=3,
        )

    with session_factory_fixture() as session:
        rows = list_minute_features(
            session,
            trade_date=_TRADE_DATE,
            timezone="Asia/Shanghai",
            symbol=_STOCK,
        )

    opening = next(row for row in rows if row["minute_start"].startswith("2026-09-28T09:29"))
    assert opening["incremental_volume_shares"] is None
    assert opening["incremental_amount_cny"] is None
    assert opening["vwap"] is None
    assert opening["relative_volume_ratio"] is None
    assert opening["market_relative_strength_bps"] is None
    assert opening["industry_relative_strength_bps"] is None
    assert "cumulative_baseline_missing" in opening["quality_flags"]
    assert "relative_volume_current_unavailable" in opening["quality_flags"]
    assert "market_benchmark_not_configured" in opening["quality_flags"]
    assert "industry_benchmark_not_configured" in opening["quality_flags"]


def test_incremental_values_do_not_bridge_a_missing_minute(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    with session_factory_fixture.begin() as session:
        _add_snapshot_collection(
            session,
            symbols=(_STOCK,),
            index=0,
            local_at=datetime(2026, 9, 28, 9, 29, 50, tzinfo=_ZONE),
            prices={_STOCK: Decimal("10")},
        )
        _add_snapshot_collection(
            session,
            symbols=(_STOCK,),
            index=2,
            local_at=datetime(2026, 9, 28, 9, 31, 5, tzinfo=_ZONE),
            prices={_STOCK: Decimal("10.2")},
        )
        build_minute_features(
            session,
            trade_date=_TRADE_DATE,
            timezone="Asia/Shanghai",
            expected_interval_seconds=15,
        )

    with session_factory_fixture() as session:
        rows = list_minute_features(
            session,
            trade_date=_TRADE_DATE,
            timezone="Asia/Shanghai",
            symbol=_STOCK,
        )

    later = next(row for row in rows if row["minute_start"].startswith("2026-09-28T09:31"))
    assert later["incremental_volume_shares"] is None
    assert later["incremental_amount_cny"] is None
    assert later["vwap"] is None
    assert "cumulative_baseline_gap" in later["quality_flags"]


def test_incremental_build_uses_prior_minute_only_as_cumulative_baseline(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    with session_factory_fixture.begin() as session:
        _seed_current_snapshots(session)
        report = build_minute_features(
            session,
            trade_date=_TRADE_DATE,
            timezone="Asia/Shanghai",
            expected_interval_seconds=15,
            minute_start=datetime(2026, 9, 28, 9, 30, tzinfo=_ZONE),
        )

    assert report.minute_bar_count == 3
    with session_factory_fixture() as session:
        bars = list(session.scalars(select(MinuteBar)))
    assert len(bars) == 3
    assert {bar.minute_start.astimezone(_ZONE).strftime("%H:%M") for bar in bars} == {"09:30"}
    assert all(bar.volume_shares is not None for bar in bars)


def _seed_relative_volume_history(session: Session) -> None:
    for days_ago, volume in ((1, 200), (2, 400)):
        local_start = datetime(2026, 9, 28, 9, 30, tzinfo=_ZONE) - timedelta(days=days_ago)
        session.add(
            MinuteBar(
                provider=QuoteProvider.TENCENT,
                symbol=_STOCK,
                exchange=Exchange.SSE,
                trade_date=local_start.date(),
                minute_start=local_start.astimezone(UTC),
                minute_end=(local_start + timedelta(minutes=1)).astimezone(UTC),
                open=Decimal("9.9"),
                high=Decimal("10.0"),
                low=Decimal("9.9"),
                close=Decimal("10.0"),
                cumulative_volume_start=1000,
                cumulative_volume_end=1000 + volume,
                volume_shares=volume,
                cumulative_amount_start=Decimal("9000"),
                cumulative_amount_end=Decimal("10000"),
                amount_cny=Decimal("1000"),
                vwap=Decimal("10"),
                sample_count=4,
                expected_sample_count=4,
                coverage_ratio=Decimal("1"),
                first_quote_at=local_start.astimezone(UTC),
                last_quote_at=(local_start + timedelta(seconds=45)).astimezone(UTC),
                quality_flags=[],
            )
        )


def _seed_current_snapshots(
    session: Session,
    *,
    symbols: tuple[str, ...] = (_STOCK, _MARKET, _INDUSTRY),
) -> None:
    times = (
        datetime(2026, 9, 28, 9, 29, 50, tzinfo=_ZONE),
        datetime(2026, 9, 28, 9, 30, 5, tzinfo=_ZONE),
        datetime(2026, 9, 28, 9, 30, 20, tzinfo=_ZONE),
        datetime(2026, 9, 28, 9, 30, 35, tzinfo=_ZONE),
        datetime(2026, 9, 28, 9, 30, 50, tzinfo=_ZONE),
    )
    prices = {
        _STOCK: ("9.9", "10", "10.3", "9.9", "10.2"),
        _MARKET: ("3", "3", "3.01", "3.02", "3.03"),
        _INDUSTRY: ("4", "4", "4.02", "4.04", "4.08"),
    }
    for index, local_at in enumerate(times):
        _add_snapshot_collection(
            session,
            symbols=symbols,
            index=index,
            local_at=local_at,
            prices={symbol: Decimal(prices[symbol][index]) for symbol in symbols},
        )


def _add_snapshot_collection(
    session: Session,
    *,
    symbols: tuple[str, ...],
    index: int,
    local_at: datetime,
    prices: dict[str, Decimal],
) -> None:
    collection_id = str(uuid4())
    fetched_at = local_at.astimezone(UTC)
    session.add(
        MarketCollectionRun(
            id=collection_id,
            idempotency_key=f"minute-feature-{collection_id}",
            expected_trade_date=_TRADE_DATE,
            market_phase=None,
            requested_symbols=list(symbols),
            started_at=fetched_at,
            finished_at=fetched_at,
            provider_summaries={},
            quality_counts={"complete": len(symbols)},
        )
    )
    session.flush()
    for symbol in symbols:
        price = prices[symbol]
        exchange = Exchange.SZSE if symbol.endswith(".SZ") else Exchange.SSE
        session.add(
            ProviderQuoteSnapshot(
                collection_id=collection_id,
                provider=QuoteProvider.TENCENT,
                symbol=symbol,
                exchange=exchange,
                name=symbol,
                quote_at=fetched_at,
                fetched_at=fetched_at,
                open=price,
                previous_close=price,
                latest=price,
                high=price,
                low=price,
                volume_shares=1000 + index * 100,
                amount_cny=Decimal(9000 + index * 1000),
                bid1_price=price,
                bid1_volume_shares=100,
                ask1_price=price,
                ask1_volume_shares=100,
                volume_precision_shares=1,
                raw_field_count=50,
                validation_issues=[],
            )
        )
        session.add(
            ReconciledQuoteSnapshot(
                collection_id=collection_id,
                symbol=symbol,
                exchange=exchange,
                quality_state=DataQualityState.COMPLETE,
                selected_provider=QuoteProvider.TENCENT,
                comparisons=[],
                reasons=[],
            )
        )
