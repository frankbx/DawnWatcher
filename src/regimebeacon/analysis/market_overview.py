"""Build an explicitly sample-based 15-minute A-share market overview."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from statistics import fmean, median
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from regimebeacon.domain import QuoteProvider
from regimebeacon.storage.models import MinuteBar, MinuteFeature, ProviderQuoteSnapshot

_MAX_POINT_DISTANCE = timedelta(seconds=45)


@dataclass(frozen=True, slots=True)
class PoolMember:
    """Pool metadata needed by the overview calculation."""

    symbol: str
    name: str
    instrument_type: str
    role: str
    industry: str | None
    proxy_quality: str | None


@dataclass(frozen=True, slots=True)
class Breadth:
    """Up/flat/down counts for one explicitly named sample."""

    up: int
    flat: int
    down: int
    available: int
    expected: int
    average_return_pct: float | None


@dataclass(frozen=True, slots=True)
class MarketTemperature:
    """Auditable intraday risk posture derived from price, breadth, and volume."""

    label: str
    score: float | None
    confidence_pct: float
    posture: str
    relative_volume_ratio: float | None
    opening_pattern: str | None
    evidence: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BenchmarkMove:
    """Daily and rolling-window movement for one benchmark ETF."""

    symbol: str
    name: str
    target: str
    daily_return_pct: float | None
    window_return_pct: float | None


@dataclass(frozen=True, slots=True)
class SectorView:
    """Industry sample breadth confirmed, where possible, by an ETF proxy."""

    industry: str
    fixed_sample_size: int
    available_sample_size: int
    sample_return_pct: float | None
    sample_median_return_pct: float | None
    sample_up_ratio_pct: float | None
    etf_symbol: str | None
    etf_return_pct: float | None
    etf_excess_vs_market_pct: float | None
    proxy_quality: str | None
    previous_sample_return_pct: float | None
    acceleration_pct: float | None
    rotation: str
    confirmation: str
    score: float | None


@dataclass(frozen=True, slots=True)
class MarketOverview:
    """One auditable market-temperature and sector-rotation snapshot."""

    trade_date: date
    observed_at: datetime
    window_minutes: int
    comparison_at: datetime
    previous_comparison_at: datetime
    market_direction: str
    temperature: MarketTemperature
    market_benchmark_symbol: str
    market_window_return_pct: float | None
    fixed_sample_daily_breadth: Breadth
    fixed_sample_window_breadth: Breadth
    benchmarks: tuple[BenchmarkMove, ...]
    sectors: tuple[SectorView, ...]
    top_sectors: tuple[SectorView, ...]
    bottom_sectors: tuple[SectorView, ...]
    warming_sectors: tuple[SectorView, ...]
    cooling_sectors: tuple[SectorView, ...]
    latest_quote_at: datetime | None
    fixed_sample_coverage_pct: float
    notes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["trade_date"] = self.trade_date.isoformat()
        for key in ("observed_at", "comparison_at", "previous_comparison_at", "latest_quote_at"):
            value = payload[key]
            payload[key] = value.isoformat() if isinstance(value, datetime) else None
        return payload


@dataclass(frozen=True, slots=True)
class _Point:
    fetched_at: datetime
    open: Decimal
    latest: Decimal
    previous_close: Decimal


@dataclass(frozen=True, slots=True)
class _Move:
    daily: float | None
    current: float | None
    previous: float | None
    gap: float | None
    from_open: float | None


def load_pool_members(path: Path) -> tuple[PoolMember, ...]:
    """Load and validate the generated pool's analysis metadata."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw_members = payload.get("members")
    if not isinstance(raw_members, list):
        raise ValueError("pool document must contain a members list")
    members: list[PoolMember] = []
    for raw in raw_members:
        if not isinstance(raw, dict):
            raise ValueError("pool members must be objects")
        symbol = raw.get("ts_code")
        name = raw.get("name")
        instrument_type = raw.get("instrument_type")
        role = raw.get("role")
        if (
            not isinstance(symbol, str)
            or not symbol
            or not isinstance(name, str)
            or not name
            or not isinstance(instrument_type, str)
            or not instrument_type
            or not isinstance(role, str)
            or not role
        ):
            raise ValueError("pool members require ts_code, name, instrument_type, and role")
        industry = raw.get("industry_l1")
        proxy_quality = raw.get("proxy_quality")
        members.append(
            PoolMember(
                symbol=symbol,
                name=name,
                instrument_type=instrument_type,
                role=role,
                industry=industry if isinstance(industry, str) else None,
                proxy_quality=proxy_quality if isinstance(proxy_quality, str) else None,
            )
        )
    if len({member.symbol for member in members}) != len(members):
        raise ValueError("pool members must have unique symbols")
    return tuple(members)


def build_market_overview(
    session: Session,
    *,
    members: tuple[PoolMember, ...],
    observed_at: datetime,
    timezone: str,
    window_minutes: int = 15,
    market_benchmark_symbol: str = "510300.SH",
    provider: QuoteProvider = QuoteProvider.TENCENT,
) -> MarketOverview:
    """Calculate broad moves, fixed-sample breadth, and sector rotation."""
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    if window_minutes < 1:
        raise ValueError("window_minutes must be positive")
    zone = ZoneInfo(timezone)
    local_observed = observed_at.astimezone(zone)
    comparison_at = local_observed - timedelta(minutes=window_minutes)
    previous_at = comparison_at - timedelta(minutes=window_minutes)
    symbols = {member.symbol for member in members}
    query_start = previous_at - _MAX_POINT_DISTANCE
    rows = session.execute(
        select(
            ProviderQuoteSnapshot.symbol,
            ProviderQuoteSnapshot.fetched_at,
            ProviderQuoteSnapshot.open,
            ProviderQuoteSnapshot.latest,
            ProviderQuoteSnapshot.previous_close,
        )
        .where(
            ProviderQuoteSnapshot.provider == provider,
            ProviderQuoteSnapshot.symbol.in_(symbols),
            ProviderQuoteSnapshot.fetched_at >= query_start.astimezone(UTC),
            ProviderQuoteSnapshot.fetched_at <= observed_at.astimezone(UTC),
        )
        .order_by(ProviderQuoteSnapshot.fetched_at)
    )
    points: dict[str, list[_Point]] = defaultdict(list)
    for symbol, fetched_at, open_price, latest, previous_close in rows:
        points[symbol].append(
            _Point(
                fetched_at=fetched_at.astimezone(zone),
                open=open_price,
                latest=latest,
                previous_close=previous_close,
            )
        )
    moves = {
        symbol: _calculate_move(values, comparison_at=comparison_at, previous_at=previous_at)
        for symbol, values in points.items()
    }

    fixed = tuple(
        member
        for member in members
        if member.instrument_type == "stock" and member.role == "fixed_representative"
    )
    fixed_daily = _breadth(fixed, moves, field="daily")
    fixed_window = _breadth(fixed, moves, field="current")
    available_fixed = sum(
        member.symbol in moves and moves[member.symbol].current is not None for member in fixed
    )
    benchmarks = _benchmark_moves(members, moves)
    market_move = moves.get(market_benchmark_symbol)
    market_return = market_move.current if market_move is not None else None
    relative_volume = _relative_volume_ratio(
        session,
        symbols={member.symbol for member in fixed},
        provider=provider,
        start_at=comparison_at,
        end_at=local_observed,
    )
    temperature = _market_temperature(
        daily=fixed_daily,
        window=fixed_window,
        market_move=market_move,
        fixed_sample_coverage_pct=(available_fixed / len(fixed) * 100 if fixed else 0),
        relative_volume_ratio=relative_volume,
        observed_at=local_observed,
        window_minutes=window_minutes,
    )
    sectors = _sector_views(
        fixed,
        members,
        moves,
        market_return=market_return,
    )
    ranked = sorted(
        (sector for sector in sectors if sector.score is not None),
        key=_sector_rank_key,
    )
    warming = sorted(
        (sector for sector in sectors if sector.rotation == "升温"),
        key=lambda sector: (-float(sector.acceleration_pct or 0), sector.industry),
    )
    cooling = sorted(
        (sector for sector in sectors if sector.rotation == "降温"),
        key=lambda sector: (float(sector.acceleration_pct or 0), sector.industry),
    )
    latest_quote_at = max(
        (values[-1].fetched_at for values in points.values() if values),
        default=None,
    )
    notes = [
        "涨跌家数仅指固定代表样本，不代表沪深市场全部股票。",
        "行业ETF可能包含创业板或科创板成分，ETF信号代表整个行业。",
    ]
    if not any(move.previous is not None for move in moves.values()):
        notes.append("前一15分钟窗口数据不足，板块升温/降温暂不可用。")
    return MarketOverview(
        trade_date=local_observed.date(),
        observed_at=local_observed,
        window_minutes=window_minutes,
        comparison_at=comparison_at,
        previous_comparison_at=previous_at,
        market_direction=_market_direction(market_return, fixed_window),
        temperature=temperature,
        market_benchmark_symbol=market_benchmark_symbol,
        market_window_return_pct=market_return,
        fixed_sample_daily_breadth=fixed_daily,
        fixed_sample_window_breadth=fixed_window,
        benchmarks=benchmarks,
        sectors=tuple(sectors),
        top_sectors=tuple(ranked[:5]),
        bottom_sectors=tuple(reversed(ranked[-5:])),
        warming_sectors=tuple(warming[:3]),
        cooling_sectors=tuple(cooling[:3]),
        latest_quote_at=latest_quote_at,
        fixed_sample_coverage_pct=round(available_fixed / len(fixed) * 100, 2) if fixed else 0,
        notes=tuple(notes),
    )


def _calculate_move(
    points: list[_Point], *, comparison_at: datetime, previous_at: datetime
) -> _Move:
    if not points:
        return _Move(None, None, None, None, None)
    latest = points[-1]
    current_base = _nearest(points, comparison_at)
    previous_base = _nearest(points, previous_at)
    daily = _return_pct(latest.previous_close, latest.latest)
    current = _return_pct(current_base.latest, latest.latest) if current_base is not None else None
    previous = (
        _return_pct(previous_base.latest, current_base.latest)
        if previous_base is not None and current_base is not None
        else None
    )
    return _Move(
        daily=daily,
        current=current,
        previous=previous,
        gap=_return_pct(latest.previous_close, latest.open),
        from_open=_return_pct(latest.open, latest.latest),
    )


def _nearest(points: list[_Point], target: datetime) -> _Point | None:
    point = min(points, key=lambda item: abs(item.fetched_at - target))
    return point if abs(point.fetched_at - target) <= _MAX_POINT_DISTANCE else None


def _return_pct(start: Decimal, end: Decimal) -> float | None:
    if start <= 0:
        return None
    return float((end / start - Decimal(1)) * Decimal(100))


def _breadth(
    members: tuple[PoolMember, ...],
    moves: dict[str, _Move],
    *,
    field: str,
) -> Breadth:
    values = [
        value
        for member in members
        if (move := moves.get(member.symbol)) is not None
        and (value := getattr(move, field)) is not None
    ]
    return Breadth(
        up=sum(value > 0 for value in values),
        flat=sum(value == 0 for value in values),
        down=sum(value < 0 for value in values),
        available=len(values),
        expected=len(members),
        average_return_pct=round(fmean(values), 4) if values else None,
    )


def _benchmark_moves(
    members: tuple[PoolMember, ...], moves: dict[str, _Move]
) -> tuple[BenchmarkMove, ...]:
    result = []
    for member in members:
        if member.role != "broad_benchmark":
            continue
        move = moves.get(member.symbol, _Move(None, None, None, None, None))
        result.append(
            BenchmarkMove(
                symbol=member.symbol,
                name=member.name,
                target=member.industry or member.name,
                daily_return_pct=_round(move.daily),
                window_return_pct=_round(move.current),
            )
        )
    return tuple(
        sorted(
            result,
            key=lambda item: (
                -(item.window_return_pct if item.window_return_pct is not None else -999),
                item.symbol,
            ),
        )
    )


def _sector_views(
    fixed: tuple[PoolMember, ...],
    members: tuple[PoolMember, ...],
    moves: dict[str, _Move],
    *,
    market_return: float | None,
) -> list[SectorView]:
    by_industry: dict[str, list[PoolMember]] = defaultdict(list)
    for member in fixed:
        if member.industry:
            by_industry[member.industry].append(member)
    proxies = {
        member.industry: member
        for member in members
        if member.role == "industry_benchmark" and member.industry is not None
    }
    result = []
    for industry, industry_members in sorted(by_industry.items()):
        current_values = [
            move.current
            for member in industry_members
            if (move := moves.get(member.symbol)) is not None and move.current is not None
        ]
        previous_values = [
            move.previous
            for member in industry_members
            if (move := moves.get(member.symbol)) is not None and move.previous is not None
        ]
        sample_mean = fmean(current_values) if current_values else None
        sample_median = median(current_values) if current_values else None
        up_ratio = (
            sum(value > 0 for value in current_values) / len(current_values) * 100
            if current_values
            else None
        )
        previous_mean = fmean(previous_values) if previous_values else None
        acceleration = (
            sample_mean - previous_mean
            if sample_mean is not None and previous_mean is not None
            else None
        )
        proxy = proxies.get(industry)
        proxy_move = moves.get(proxy.symbol) if proxy is not None else None
        etf_return = proxy_move.current if proxy_move is not None else None
        etf_excess = (
            etf_return - market_return
            if etf_return is not None and market_return is not None
            else None
        )
        score = _sector_score(sample_mean, sample_median, up_ratio, etf_excess)
        result.append(
            SectorView(
                industry=industry,
                fixed_sample_size=len(industry_members),
                available_sample_size=len(current_values),
                sample_return_pct=_round(sample_mean),
                sample_median_return_pct=_round(sample_median),
                sample_up_ratio_pct=_round(up_ratio),
                etf_symbol=proxy.symbol if proxy is not None else None,
                etf_return_pct=_round(etf_return),
                etf_excess_vs_market_pct=_round(etf_excess),
                proxy_quality=proxy.proxy_quality if proxy is not None else None,
                previous_sample_return_pct=_round(previous_mean),
                acceleration_pct=_round(acceleration),
                rotation=_rotation(acceleration),
                confirmation=_confirmation(sample_mean, up_ratio, etf_excess),
                score=_round(score),
            )
        )
    return result


def _sector_score(
    sample_mean: float | None,
    sample_median: float | None,
    up_ratio: float | None,
    etf_excess: float | None,
) -> float | None:
    if sample_mean is None or sample_median is None or up_ratio is None:
        return None
    proxy_component = etf_excess if etf_excess is not None else 0.0
    breadth_component = (up_ratio - 50) / 100
    return sample_mean * 0.4 + sample_median * 0.2 + proxy_component * 0.3 + breadth_component * 0.1


def _sector_rank_key(sector: SectorView) -> tuple[float, str]:
    score = sector.score if sector.score is not None else float("-inf")
    return -score, sector.industry


def _rotation(acceleration: float | None) -> str:
    if acceleration is None:
        return "历史不足"
    if acceleration >= 0.15:
        return "升温"
    if acceleration <= -0.15:
        return "降温"
    return "稳定"


def _confirmation(
    sample_mean: float | None, up_ratio: float | None, etf_excess: float | None
) -> str:
    if sample_mean is None or up_ratio is None:
        return "数据不足"
    if sample_mean >= 0.1 and up_ratio >= 60 and (etf_excess is None or etf_excess >= 0):
        return "一致走强"
    if sample_mean <= -0.1 and up_ratio <= 40 and (etf_excess is None or etf_excess <= 0):
        return "一致走弱"
    if etf_excess is not None and etf_excess >= 0.1 and up_ratio < 50:
        return "ETF权重驱动"
    if sample_mean >= 0.1 and up_ratio >= 60 and etf_excess is not None and etf_excess < 0:
        return "样本扩散"
    return "信号分化"


def _relative_volume_ratio(
    session: Session,
    *,
    symbols: set[str],
    provider: QuoteProvider,
    start_at: datetime,
    end_at: datetime,
) -> float | None:
    """Return the median same-minute relative volume for the current window."""
    if not symbols:
        return None
    window_start = start_at.replace(second=0, microsecond=0).astimezone(UTC)
    window_end = end_at.replace(second=0, microsecond=0).astimezone(UTC)
    if window_end <= window_start:
        return None
    values = [
        float(value)
        for value in session.scalars(
            select(MinuteFeature.relative_volume_ratio)
            .join(MinuteBar, MinuteFeature.minute_bar_id == MinuteBar.id)
            .where(
                MinuteBar.provider == provider,
                MinuteBar.symbol.in_(symbols),
                MinuteBar.minute_start >= window_start,
                MinuteBar.minute_start < window_end,
                MinuteFeature.relative_volume_ratio.is_not(None),
            )
        )
        if value is not None
    ]
    return round(median(values), 4) if values else None


def _market_temperature(
    *,
    daily: Breadth,
    window: Breadth,
    market_move: _Move | None,
    fixed_sample_coverage_pct: float,
    relative_volume_ratio: float | None,
    observed_at: datetime,
    window_minutes: int,
) -> MarketTemperature:
    """Classify an intraday temperature without treating volume as direction."""
    if (
        daily.available == 0
        or window.available == 0
        or market_move is None
        or market_move.daily is None
        or market_move.current is None
    ):
        return MarketTemperature(
            label="数据不足",
            score=None,
            confidence_pct=round(min(50.0, fixed_sample_coverage_pct * 0.5), 1),
            posture="仅监控，等待数据",
            relative_volume_ratio=relative_volume_ratio,
            opening_pattern=None,
            evidence=("价格、广度或基准数据不足，暂不形成温度判断。",),
        )

    daily_up_ratio = daily.up / daily.available * 100
    window_up_ratio = window.up / window.available * 100
    components: list[tuple[float, float | None]] = [
        (15.0, _breadth_balance(daily)),
        (10.0, _scaled(daily.average_return_pct, scale=1.0)),
        (10.0, _scaled(market_move.daily, scale=1.0)),
        (7.0, _breadth_balance(window)),
        (4.0, _scaled(window.average_return_pct, scale=0.35)),
        (4.0, _scaled(market_move.current, scale=0.35)),
    ]
    available_components: list[tuple[float, float]] = []
    for weight, value in components:
        if value is not None:
            available_components.append((weight, value))
    total_weight = sum(weight for weight, _ in available_components)
    weighted_direction = sum(weight * value for weight, value in available_components)
    score = round(_clamp(50 + weighted_direction / total_weight * 50, 0, 100), 1)

    elevated_volume = relative_volume_ratio is not None and relative_volume_ratio >= 1.25
    if score >= 85 and elevated_volume:
        label = "过热"
    elif score >= 65:
        label = "偏暖"
    elif score <= 15:
        label = "风险收缩"
    elif score <= 35:
        label = "偏冷"
    else:
        label = "中性/分化"

    posture = {
        "过热": "维持计划，禁止追高",
        "偏暖": "维持既定风险预算",
        "中性/分化": "等待确认，减少主动加仓",
        "偏冷": "收紧开仓条件",
        "风险收缩": "暂停新增风险",
    }[label]
    direction = 1 if score > 55 else (-1 if score < 45 else 0)
    signed_values = [value for _, value in available_components if abs(value) >= 0.1]
    if direction == 0:
        agreement = 1 - abs(fmean(value for _, value in available_components))
    elif signed_values:
        agreement = sum(value * direction > 0 for value in signed_values) / len(signed_values)
    else:
        agreement = 0.5
    availability = len(available_components) / len(components)
    confidence = round(
        _clamp(
            fixed_sample_coverage_pct * 0.45
            + agreement * 100 * 0.35
            + availability * 100 * 0.15
            + (5 if relative_volume_ratio is not None else 0),
            0,
            100,
        ),
        1,
    )
    opening_pattern = _opening_pattern(market_move, observed_at)
    evidence = [
        (f"当日样本上涨占比 {daily_up_ratio:.1f}%，平均收益 {_pct(daily.average_return_pct)}"),
        (
            f"近{window_minutes}分钟上涨占比 {window_up_ratio:.1f}%，"
            f"平均收益 {_pct(window.average_return_pct)}"
        ),
        (
            f"沪深300ETF当日 {_pct(market_move.daily)}，"
            f"近{window_minutes}分钟 {_pct(market_move.current)}"
        ),
    ]
    if relative_volume_ratio is not None:
        evidence.append(f"同分钟相对量能 {relative_volume_ratio:.2f} 倍")
    if opening_pattern is not None:
        evidence.append(f"开盘确认：{opening_pattern}")
    return MarketTemperature(
        label=label,
        score=score,
        confidence_pct=confidence,
        posture=posture,
        relative_volume_ratio=relative_volume_ratio,
        opening_pattern=opening_pattern,
        evidence=tuple(evidence),
    )


def _breadth_balance(breadth: Breadth) -> float | None:
    if breadth.available == 0:
        return None
    return (breadth.up - breadth.down) / breadth.available


def _scaled(value: float | None, *, scale: float) -> float | None:
    if value is None:
        return None
    return _clamp(value / scale, -1, 1)


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _opening_pattern(move: _Move, observed_at: datetime) -> str | None:
    minute_of_day = observed_at.hour * 60 + observed_at.minute
    if not 9 * 60 + 44 <= minute_of_day <= 10 * 60:
        return None
    if move.gap is None or move.from_open is None:
        return None
    threshold = 0.05
    if move.gap >= threshold:
        if move.from_open >= threshold:
            return "高开延续"
        if move.from_open <= -threshold:
            return "高开回吐"
        return "高开后暂稳"
    if move.gap <= -threshold:
        if move.from_open <= -threshold:
            return "低开走弱"
        if move.from_open >= threshold:
            return "低开修复"
        return "低开后暂稳"
    if move.from_open >= threshold:
        return "平开走强"
    if move.from_open <= -threshold:
        return "平开走弱"
    return "开盘方向未明"


def _market_direction(market_return: float | None, breadth: Breadth) -> str:
    if market_return is None or breadth.available == 0:
        return "数据不足"
    up_ratio = breadth.up / breadth.available * 100
    if market_return >= 0.05 and up_ratio >= 55:
        return "偏强"
    if market_return <= -0.05 and up_ratio <= 45:
        return "偏弱"
    return "震荡/分化"


def _round(value: float | None) -> float | None:
    return round(value, 4) if value is not None else None


def format_market_overview_markdown(report: MarketOverview) -> str:
    """Render a bounded mobile-friendly Feishu markdown summary."""
    window = report.fixed_sample_window_breadth
    daily = report.fixed_sample_daily_breadth
    temperature = report.temperature
    temperature_score = (
        f"{temperature.score:.2f}/100" if temperature.score is not None else "无数据"
    )
    lines = [
        (
            f"**市场温度**：{temperature.label}（{temperature_score}，"
            f"信号一致性 {temperature.confidence_pct:.2f}%）"
        ),
        f"**风险动作**：{temperature.posture}",
        "**温度依据**：" + "；".join(temperature.evidence),
        "",
        f"**市场状态**：{report.market_direction}",
        f"**时间**：{report.observed_at.strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"**{report.window_minutes}分钟沪深300**：{_pct(report.market_window_return_pct)}",
        "",
        f"**固定样本当日广度**：涨 {daily.up} / 平 {daily.flat} / 跌 {daily.down}",
        f"**固定样本{report.window_minutes}分钟广度**：涨 {window.up} / 平 {window.flat} / 跌 {window.down}，平均 {_pct(window.average_return_pct)}",
        f"**样本覆盖**：{window.available}/{window.expected}（{report.fixed_sample_coverage_pct:.2f}%）",
        "",
        "**强势行业**",
    ]
    lines.extend(_sector_line(item) for item in report.top_sectors[:3])
    lines.extend(["", "**弱势行业**"])
    lines.extend(_sector_line(item) for item in report.bottom_sectors[:3])
    if report.warming_sectors or report.cooling_sectors:
        lines.extend(["", "**板块轮动**"])
        if report.warming_sectors:
            lines.append("升温：" + "、".join(item.industry for item in report.warming_sectors))
        if report.cooling_sectors:
            lines.append("降温：" + "、".join(item.industry for item in report.cooling_sectors))
    lines.extend(
        [
            "",
            (
                "_温度与涨跌家数来自固定代表样本，不是全市场精确统计；"
                "分数和置信度仅表示样本强弱与信号一致性，不是涨跌概率；"
                "开盘15分钟只作风险修正，不作为独立买入信号。_"
            ),
        ]
    )
    return "\n".join(lines)[:4_000]


def _sector_line(item: SectorView) -> str:
    proxy = f"ETF {_pct(item.etf_return_pct)}" if item.etf_return_pct is not None else "无ETF代理"
    return (
        f"- {item.industry}：样本 {_pct(item.sample_return_pct)}，上涨占比 "
        f"{_pct(item.sample_up_ratio_pct)}，{proxy}，{item.confirmation}"
    )


def _pct(value: float | None) -> str:
    return f"{value:+.2f}%" if value is not None else "无数据"
