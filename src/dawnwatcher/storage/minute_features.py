"""Build auditable one-minute bars and decision-facing intraday features."""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from dawnwatcher.domain import DataQualityState, Exchange, QuoteProvider
from dawnwatcher.storage.audit import append_audit_event
from dawnwatcher.storage.models import (
    MinuteBar,
    MinuteFeature,
    ProviderQuoteSnapshot,
    ReconciledQuoteSnapshot,
)

_BPS = Decimal("10000")
_ONE = Decimal("1")
_USABLE_QUALITY_STATES = (
    DataQualityState.COMPLETE,
    DataQualityState.NEAR,
    DataQualityState.DEGRADED,
)


@dataclass(frozen=True, slots=True)
class MinuteFeatureBuildReport:
    """Summary of one idempotent minute-feature rebuild."""

    trade_date: date
    provider: str
    snapshot_count: int
    symbol_count: int
    minute_bar_count: int
    feature_count: int
    market_benchmark_symbol: str | None
    industry_mapping_count: int
    relative_volume_lookback_days: int
    relative_volume_minimum_history_days: int

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["trade_date"] = self.trade_date.isoformat()
        return payload


@dataclass(frozen=True, slots=True)
class _BarValue:
    provider: QuoteProvider
    symbol: str
    exchange: Exchange
    trade_date: date
    minute_start: datetime
    minute_end: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    cumulative_volume_start: int | None
    cumulative_volume_end: int
    volume_shares: int | None
    cumulative_amount_start: Decimal | None
    cumulative_amount_end: Decimal
    amount_cny: Decimal | None
    vwap: Decimal | None
    sample_count: int
    expected_sample_count: int
    coverage_ratio: Decimal
    first_quote_at: datetime
    last_quote_at: datetime
    quality_flags: tuple[str, ...]


def build_minute_features(
    session: Session,
    *,
    trade_date: date,
    timezone: str,
    expected_interval_seconds: float,
    market_benchmark_symbol: str | None = None,
    industry_benchmarks: dict[str, str] | None = None,
    relative_volume_lookback_days: int = 20,
    relative_volume_minimum_history_days: int = 5,
    symbols: set[str] | None = None,
    provider: QuoteProvider = QuoteProvider.TENCENT,
    minute_start: datetime | None = None,
) -> MinuteFeatureBuildReport:
    """Rebuild minute bars and features from validated persisted snapshots.

    Relative volume compares the current minute's incremental volume with the
    same local clock minute over preceding stored trading dates.  Relative
    strength is the minute return in basis points minus the corresponding
    benchmark minute return.
    """
    if expected_interval_seconds <= 0:
        raise ValueError("expected interval seconds must be positive")
    if relative_volume_lookback_days < 1:
        raise ValueError("relative-volume lookback must be at least one day")
    if not 1 <= relative_volume_minimum_history_days <= relative_volume_lookback_days:
        raise ValueError("minimum history days must be between one and the lookback")

    zone = ZoneInfo(timezone)
    target_minute = _normalize_target_minute(minute_start, trade_date=trade_date, zone=zone)
    mappings = industry_benchmarks or {}
    selected_symbols = set(symbols) if symbols else None
    snapshot_filter: set[str] | None = None
    if selected_symbols is not None:
        snapshot_filter = set(selected_symbols)
        if market_benchmark_symbol is not None:
            snapshot_filter.add(market_benchmark_symbol)
        snapshot_filter.update(
            benchmark for symbol, benchmark in mappings.items() if symbol in selected_symbols
        )

    snapshots = _load_snapshots(
        session,
        trade_date=trade_date,
        zone=zone,
        provider=provider,
        symbols=snapshot_filter,
        from_at=target_minute - timedelta(minutes=1) if target_minute is not None else None,
        until_at=target_minute + timedelta(minutes=1) if target_minute is not None else None,
    )
    expected_samples = max(1, math.ceil(60 / expected_interval_seconds))
    values = _aggregate_snapshots(
        snapshots,
        trade_date=trade_date,
        zone=zone,
        expected_sample_count=expected_samples,
    )
    if target_minute is not None:
        target_utc = target_minute.astimezone(UTC)
        values = [value for value in values if value.minute_start == target_utc]
    bars = _upsert_bars(session, values, trade_date=trade_date, provider=provider)
    session.flush()

    history = _load_relative_volume_history(
        session,
        trade_date=trade_date,
        zone=zone,
        symbols={bar.symbol for bar in bars},
        provider=provider,
        lookback_days=relative_volume_lookback_days,
    )
    feature_count = _upsert_features(
        session,
        bars,
        zone=zone,
        history=history,
        minimum_history_days=relative_volume_minimum_history_days,
        market_benchmark_symbol=market_benchmark_symbol,
        industry_benchmarks=mappings,
    )
    append_audit_event(
        session,
        event_type="market.minute_features.built",
        entity_type="trade_date",
        entity_id=trade_date.isoformat(),
        payload={
            "provider": provider.value,
            "snapshot_count": len(snapshots),
            "minute_bar_count": len(bars),
            "feature_count": feature_count,
            "market_benchmark_symbol": market_benchmark_symbol,
            "industry_mapping_count": len(mappings),
            "relative_volume_lookback_days": relative_volume_lookback_days,
            "relative_volume_minimum_history_days": relative_volume_minimum_history_days,
        },
    )
    return MinuteFeatureBuildReport(
        trade_date=trade_date,
        provider=provider.value,
        snapshot_count=len(snapshots),
        symbol_count=len({bar.symbol for bar in bars}),
        minute_bar_count=len(bars),
        feature_count=feature_count,
        market_benchmark_symbol=market_benchmark_symbol,
        industry_mapping_count=len(mappings),
        relative_volume_lookback_days=relative_volume_lookback_days,
        relative_volume_minimum_history_days=relative_volume_minimum_history_days,
    )


def list_minute_features(
    session: Session,
    *,
    trade_date: date,
    timezone: str,
    symbol: str | None = None,
    provider: QuoteProvider = QuoteProvider.TENCENT,
) -> list[dict[str, Any]]:
    """Return persisted bars and features as JSON-compatible dictionaries."""
    statement = (
        select(MinuteBar, MinuteFeature)
        .join(MinuteFeature, MinuteFeature.minute_bar_id == MinuteBar.id)
        .where(MinuteBar.trade_date == trade_date, MinuteBar.provider == provider)
        .order_by(MinuteBar.minute_start, MinuteBar.symbol)
    )
    if symbol is not None:
        statement = statement.where(MinuteBar.symbol == symbol)
    zone = ZoneInfo(timezone)
    return [_feature_payload(bar, feature, zone) for bar, feature in session.execute(statement)]


def _load_snapshots(
    session: Session,
    *,
    trade_date: date,
    zone: ZoneInfo,
    provider: QuoteProvider,
    symbols: set[str] | None,
    from_at: datetime | None = None,
    until_at: datetime | None = None,
) -> list[ProviderQuoteSnapshot]:
    local_start = datetime.combine(trade_date, time.min, tzinfo=zone)
    local_end = local_start + timedelta(days=1)
    statement = (
        select(ProviderQuoteSnapshot)
        .join(
            ReconciledQuoteSnapshot,
            and_(
                ReconciledQuoteSnapshot.collection_id == ProviderQuoteSnapshot.collection_id,
                ReconciledQuoteSnapshot.symbol == ProviderQuoteSnapshot.symbol,
            ),
        )
        .where(
            ProviderQuoteSnapshot.provider == provider,
            ProviderQuoteSnapshot.fetched_at >= local_start.astimezone(UTC),
            ProviderQuoteSnapshot.fetched_at < local_end.astimezone(UTC),
            ReconciledQuoteSnapshot.quality_state.in_(_USABLE_QUALITY_STATES),
        )
        .order_by(ProviderQuoteSnapshot.symbol, ProviderQuoteSnapshot.fetched_at)
    )
    if from_at is not None:
        statement = statement.where(ProviderQuoteSnapshot.fetched_at >= from_at.astimezone(UTC))
    if until_at is not None:
        statement = statement.where(ProviderQuoteSnapshot.fetched_at < until_at.astimezone(UTC))
    if symbols:
        statement = statement.where(ProviderQuoteSnapshot.symbol.in_(symbols))
    snapshots = list(session.scalars(statement))
    return [
        snapshot
        for snapshot in snapshots
        if not any(issue.get("severity") == "error" for issue in snapshot.validation_issues)
    ]


def _normalize_target_minute(
    value: datetime | None,
    *,
    trade_date: date,
    zone: ZoneInfo,
) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("minute_start must be timezone-aware")
    local = value.astimezone(zone)
    if local.second != 0 or local.microsecond != 0:
        raise ValueError("minute_start must be aligned to a minute boundary")
    if local.date() != trade_date:
        raise ValueError("minute_start must belong to trade_date in the configured timezone")
    return local


def _aggregate_snapshots(
    snapshots: list[ProviderQuoteSnapshot],
    *,
    trade_date: date,
    zone: ZoneInfo,
    expected_sample_count: int,
) -> list[_BarValue]:
    by_symbol: dict[str, list[ProviderQuoteSnapshot]] = defaultdict(list)
    for snapshot in snapshots:
        by_symbol[snapshot.symbol].append(snapshot)

    result: list[_BarValue] = []
    for symbol_snapshots in by_symbol.values():
        groups: dict[datetime, list[ProviderQuoteSnapshot]] = defaultdict(list)
        for snapshot in symbol_snapshots:
            local_fetched = snapshot.fetched_at.astimezone(zone)
            minute = local_fetched.replace(second=0, microsecond=0)
            groups[minute].append(snapshot)

        previous: ProviderQuoteSnapshot | None = None
        for local_minute, points in sorted(groups.items()):
            points.sort(key=lambda item: item.fetched_at)
            first = points[0]
            last = points[-1]
            flags: list[str] = []
            if len(points) == 1:
                flags.append("single_sample")
            if len(points) < expected_sample_count:
                flags.append("sample_coverage_low")

            volume_delta: int | None = None
            amount_delta: Decimal | None = None
            vwap: Decimal | None = None
            if previous is None:
                flags.append("cumulative_baseline_missing")
            elif previous.fetched_at.astimezone(zone).replace(
                second=0, microsecond=0
            ) != local_minute - timedelta(minutes=1):
                flags.append("cumulative_baseline_gap")
            else:
                raw_volume_delta = last.volume_shares - previous.volume_shares
                raw_amount_delta = last.amount_cny - previous.amount_cny
                if raw_volume_delta < 0:
                    flags.append("cumulative_volume_reset")
                else:
                    volume_delta = raw_volume_delta
                if raw_amount_delta < 0:
                    flags.append("cumulative_amount_reset")
                else:
                    amount_delta = raw_amount_delta
                if volume_delta is not None and volume_delta > 0 and amount_delta is not None:
                    vwap = amount_delta / Decimal(volume_delta)
                else:
                    flags.append("vwap_unavailable")

            prices = [point.latest for point in points]
            minute_start = local_minute.astimezone(UTC)
            result.append(
                _BarValue(
                    provider=first.provider,
                    symbol=first.symbol,
                    exchange=first.exchange,
                    trade_date=trade_date,
                    minute_start=minute_start,
                    minute_end=minute_start + timedelta(minutes=1),
                    open=first.latest,
                    high=max(prices),
                    low=min(prices),
                    close=last.latest,
                    cumulative_volume_start=(
                        previous.volume_shares if previous is not None else None
                    ),
                    cumulative_volume_end=last.volume_shares,
                    volume_shares=volume_delta,
                    cumulative_amount_start=(previous.amount_cny if previous is not None else None),
                    cumulative_amount_end=last.amount_cny,
                    amount_cny=amount_delta,
                    vwap=vwap,
                    sample_count=len(points),
                    expected_sample_count=expected_sample_count,
                    coverage_ratio=min(
                        _ONE,
                        Decimal(len(points)) / Decimal(expected_sample_count),
                    ),
                    first_quote_at=first.quote_at,
                    last_quote_at=last.quote_at,
                    quality_flags=tuple(flags),
                )
            )
            previous = last
    return sorted(result, key=lambda item: (item.minute_start, item.symbol))


def _upsert_bars(
    session: Session,
    values: list[_BarValue],
    *,
    trade_date: date,
    provider: QuoteProvider,
) -> list[MinuteBar]:
    symbols = {value.symbol for value in values}
    statement = select(MinuteBar).where(
        MinuteBar.trade_date == trade_date,
        MinuteBar.provider == provider,
    )
    if symbols:
        statement = statement.where(MinuteBar.symbol.in_(symbols))
    existing = {(bar.symbol, bar.minute_start): bar for bar in session.scalars(statement)}
    bars: list[MinuteBar] = []
    for value in values:
        key = (value.symbol, value.minute_start)
        bar = existing.get(key)
        if bar is None:
            bar = MinuteBar()
            session.add(bar)
        for field_name in (
            "provider",
            "symbol",
            "exchange",
            "trade_date",
            "minute_start",
            "minute_end",
            "open",
            "high",
            "low",
            "close",
            "cumulative_volume_start",
            "cumulative_volume_end",
            "volume_shares",
            "cumulative_amount_start",
            "cumulative_amount_end",
            "amount_cny",
            "vwap",
            "sample_count",
            "expected_sample_count",
            "coverage_ratio",
            "first_quote_at",
            "last_quote_at",
        ):
            setattr(bar, field_name, getattr(value, field_name))
        bar.quality_flags = list(value.quality_flags)
        bars.append(bar)
    return bars


def _load_relative_volume_history(
    session: Session,
    *,
    trade_date: date,
    zone: ZoneInfo,
    symbols: set[str],
    provider: QuoteProvider,
    lookback_days: int,
) -> dict[tuple[str, int, int], list[int]]:
    if not symbols:
        return {}
    calendar_window = max(30, lookback_days * 4)
    history_rows = list(
        session.scalars(
            select(MinuteBar)
            .where(
                MinuteBar.provider == provider,
                MinuteBar.symbol.in_(symbols),
                MinuteBar.trade_date < trade_date,
                MinuteBar.trade_date >= trade_date - timedelta(days=calendar_window),
                MinuteBar.volume_shares.is_not(None),
            )
            .order_by(MinuteBar.trade_date.desc())
        )
    )
    history: dict[tuple[str, int, int], list[int]] = defaultdict(list)
    for bar in history_rows:
        local = bar.minute_start.astimezone(zone)
        key = (bar.symbol, local.hour, local.minute)
        if len(history[key]) < lookback_days and bar.volume_shares is not None:
            history[key].append(bar.volume_shares)
    return dict(history)


def _upsert_features(
    session: Session,
    bars: list[MinuteBar],
    *,
    zone: ZoneInfo,
    history: dict[tuple[str, int, int], list[int]],
    minimum_history_days: int,
    market_benchmark_symbol: str | None,
    industry_benchmarks: dict[str, str],
) -> int:
    if not bars:
        return 0
    by_symbol_minute = {(bar.symbol, bar.minute_start): bar for bar in bars}
    existing = {
        feature.minute_bar_id: feature
        for feature in session.scalars(
            select(MinuteFeature).where(MinuteFeature.minute_bar_id.in_([bar.id for bar in bars]))
        )
    }
    for bar in bars:
        flags = list(bar.quality_flags)
        price_trend = _return_bps(bar.open, bar.close)
        vwap_deviation = (
            _return_bps(bar.vwap, bar.close) if bar.vwap is not None and bar.vwap > 0 else None
        )
        if vwap_deviation is None:
            flags.append("vwap_deviation_unavailable")

        local = bar.minute_start.astimezone(zone)
        history_values = history.get((bar.symbol, local.hour, local.minute), [])
        relative_volume: Decimal | None = None
        if bar.volume_shares is None:
            flags.append("relative_volume_current_unavailable")
        elif len(history_values) < minimum_history_days:
            flags.append("relative_volume_history_insufficient")
        else:
            average_volume = Decimal(sum(history_values)) / Decimal(len(history_values))
            if average_volume > 0:
                relative_volume = Decimal(bar.volume_shares) / average_volume
            else:
                flags.append("relative_volume_history_zero")

        market_strength: Decimal | None = None
        if market_benchmark_symbol is None:
            flags.append("market_benchmark_not_configured")
        else:
            benchmark = by_symbol_minute.get((market_benchmark_symbol, bar.minute_start))
            if benchmark is None:
                flags.append("market_benchmark_bar_missing")
            else:
                market_strength = price_trend - _return_bps(benchmark.open, benchmark.close)

        industry_symbol = industry_benchmarks.get(bar.symbol)
        industry_strength: Decimal | None = None
        if industry_symbol is None:
            flags.append("industry_benchmark_not_configured")
        else:
            benchmark = by_symbol_minute.get((industry_symbol, bar.minute_start))
            if benchmark is None:
                flags.append("industry_benchmark_bar_missing")
            else:
                industry_strength = price_trend - _return_bps(benchmark.open, benchmark.close)

        feature = existing.get(bar.id)
        if feature is None:
            feature = MinuteFeature(minute_bar_id=bar.id)
            session.add(feature)
        feature.price_trend_bps = price_trend
        feature.vwap_deviation_bps = vwap_deviation
        feature.relative_volume_ratio = relative_volume
        feature.relative_volume_history_days = len(history_values)
        feature.market_benchmark_symbol = market_benchmark_symbol
        feature.market_relative_strength_bps = market_strength
        feature.industry_benchmark_symbol = industry_symbol
        feature.industry_relative_strength_bps = industry_strength
        feature.quality_flags = list(dict.fromkeys(flags))
    session.flush()
    return len(bars)


def _return_bps(start: Decimal, end: Decimal) -> Decimal:
    if start <= 0:
        return Decimal(0)
    return (end / start - _ONE) * _BPS


def _feature_payload(
    bar: MinuteBar,
    feature: MinuteFeature,
    zone: ZoneInfo,
) -> dict[str, Any]:
    return {
        "provider": bar.provider.value,
        "symbol": bar.symbol,
        "exchange": bar.exchange.value,
        "trade_date": bar.trade_date.isoformat(),
        "minute_start": bar.minute_start.astimezone(zone).isoformat(),
        "minute_end": bar.minute_end.astimezone(zone).isoformat(),
        "open": str(bar.open),
        "high": str(bar.high),
        "low": str(bar.low),
        "close": str(bar.close),
        "incremental_volume_shares": bar.volume_shares,
        "incremental_amount_cny": str(bar.amount_cny) if bar.amount_cny is not None else None,
        "vwap": str(bar.vwap) if bar.vwap is not None else None,
        "sample_count": bar.sample_count,
        "expected_sample_count": bar.expected_sample_count,
        "coverage_ratio": str(bar.coverage_ratio),
        "price_trend_bps": str(feature.price_trend_bps),
        "vwap_deviation_bps": (
            str(feature.vwap_deviation_bps) if feature.vwap_deviation_bps is not None else None
        ),
        "relative_volume_ratio": (
            str(feature.relative_volume_ratio)
            if feature.relative_volume_ratio is not None
            else None
        ),
        "relative_volume_history_days": feature.relative_volume_history_days,
        "market_benchmark_symbol": feature.market_benchmark_symbol,
        "market_relative_strength_bps": (
            str(feature.market_relative_strength_bps)
            if feature.market_relative_strength_bps is not None
            else None
        ),
        "industry_benchmark_symbol": feature.industry_benchmark_symbol,
        "industry_relative_strength_bps": (
            str(feature.industry_relative_strength_bps)
            if feature.industry_relative_strength_bps is not None
            else None
        ),
        "quality_flags": feature.quality_flags,
    }
