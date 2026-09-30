"""Validated, manually maintained holdings and quote-based risk review."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from regimebeacon.domain import QuoteProvider, QuoteSymbol
from regimebeacon.portfolio.minute_history import HistoryReference
from regimebeacon.storage.models import ProviderQuoteSnapshot

_HUNDRED = Decimal("100")
_MAX_QUOTE_AGE = timedelta(seconds=90)
_WINDOW_ALIGNMENT_TOLERANCE = timedelta(seconds=45)


@dataclass(frozen=True, slots=True)
class Holding:
    symbol: str
    name: str
    cost_cny: Decimal
    shares: int


@dataclass(frozen=True, slots=True)
class HoldingView:
    symbol: str
    name: str
    shares: int
    cost_cny: Decimal
    latest_cny: Decimal | None
    quote_at: datetime | None
    pnl_cny: Decimal | None
    pnl_pct: Decimal | None
    day_pct: Decimal | None
    window_pct: Decimal | None
    benchmark_window_pct: Decimal | None
    relative_window_pct: Decimal | None
    historical_days: int
    historical_latest_date: date | None
    historical_median_pct: Decimal | None
    historical_excess_pct: Decimal | None
    historical_volume_ratio: Decimal | None
    history_status: str
    state: str
    guidance: str

    def to_dict(self) -> dict[str, object]:
        value = asdict(self)
        for key, item in value.items():
            if isinstance(item, Decimal):
                value[key] = str(item)
            elif isinstance(item, datetime):
                value[key] = item.isoformat()
            elif isinstance(item, date):
                value[key] = item.isoformat()
        return value


@dataclass(frozen=True, slots=True)
class GuidanceThresholds:
    """Heuristic price bands for the report window; not backtested trade signals."""

    window_minutes: int
    large_move_pct: Decimal
    moderate_move_pct: Decimal
    relative_weak_pct: Decimal
    volume_ratio: Decimal = Decimal("1.5")
    cost_loss_pct: Decimal = Decimal("5")

    def to_dict(self) -> dict[str, object]:
        return {
            "window_minutes": self.window_minutes,
            "large_move_pct": str(self.large_move_pct),
            "moderate_move_pct": str(self.moderate_move_pct),
            "relative_weak_pct": str(self.relative_weak_pct),
            "volume_ratio": str(self.volume_ratio),
            "cost_loss_pct": str(self.cost_loss_pct),
        }


@dataclass(frozen=True, slots=True)
class HoldingsReport:
    observed_at: datetime
    window_minutes: int
    guidance_thresholds: GuidanceThresholds
    positions: tuple[HoldingView, ...]
    total_cost_cny: Decimal
    total_value_cny: Decimal | None
    total_pnl_cny: Decimal | None
    total_pnl_pct: Decimal | None
    data_complete: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "observed_at": self.observed_at.isoformat(),
            "window_minutes": self.window_minutes,
            "guidance_thresholds": self.guidance_thresholds.to_dict(),
            "positions": [position.to_dict() for position in self.positions],
            "total_cost_cny": str(self.total_cost_cny),
            "total_value_cny": str(self.total_value_cny)
            if self.total_value_cny is not None
            else None,
            "total_pnl_cny": str(self.total_pnl_cny) if self.total_pnl_cny is not None else None,
            "total_pnl_pct": str(self.total_pnl_pct) if self.total_pnl_pct is not None else None,
            "data_complete": self.data_complete,
        }


def load_holdings(path: Path) -> tuple[Holding, ...]:
    """Load Tushare-format symbols without silently accepting stale/invalid costs."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ValueError("holdings file requires schema_version 1")
    raw_positions = payload.get("positions")
    if not isinstance(raw_positions, list) or not raw_positions:
        raise ValueError("holdings file requires a nonempty positions list")
    values: list[Holding] = []
    for raw in raw_positions:
        if not isinstance(raw, dict):
            raise ValueError("holding must be an object")
        symbol = QuoteSymbol.parse(raw.get("symbol", "")).ts_code
        name = raw.get("name")
        shares = raw.get("shares")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"holding {symbol} requires a name")
        if isinstance(shares, bool) or not isinstance(shares, int) or shares <= 0:
            raise ValueError(f"holding {symbol} requires positive integer shares")
        try:
            cost = Decimal(str(raw.get("cost_cny")))
        except InvalidOperation as exc:
            raise ValueError(f"holding {symbol} has invalid cost") from exc
        if not cost.is_finite() or cost <= 0:
            raise ValueError(f"holding {symbol} requires positive finite cost")
        values.append(Holding(symbol=symbol, name=name.strip(), cost_cny=cost, shares=shares))
    if len({item.symbol for item in values}) != len(values):
        raise ValueError("holdings file contains duplicate symbols")
    return tuple(values)


def build_holdings_report(
    session: Session,
    *,
    holdings: tuple[Holding, ...],
    observed_at: datetime,
    timezone: str,
    window_minutes: int = 15,
    benchmark_symbol: str = "510300.SH",
    history: HistoryReference | None = None,
    window_end: datetime | None = None,
) -> HoldingsReport:
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    if window_minutes < 1:
        raise ValueError("window_minutes must be positive")
    thresholds = guidance_thresholds(window_minutes)
    if window_end is not None and (window_end.tzinfo is None or window_end.utcoffset() is None):
        raise ValueError("window_end must be timezone-aware")
    zone = ZoneInfo(timezone)
    local_now = observed_at.astimezone(zone)
    session_open = datetime.combine(local_now.date(), time(9, 15), tzinfo=zone)
    rows = session.scalars(
        select(ProviderQuoteSnapshot)
        .where(
            ProviderQuoteSnapshot.provider == QuoteProvider.TENCENT,
            ProviderQuoteSnapshot.symbol.in_(
                {holding.symbol for holding in holdings} | {benchmark_symbol}
            ),
            ProviderQuoteSnapshot.fetched_at >= session_open.astimezone(UTC),
            ProviderQuoteSnapshot.fetched_at <= observed_at.astimezone(UTC),
        )
        .order_by(ProviderQuoteSnapshot.symbol, ProviderQuoteSnapshot.fetched_at)
    )
    by_symbol: dict[str, list[ProviderQuoteSnapshot]] = defaultdict(list)
    for row in rows:
        if row.validation_issues or row.quote_at.astimezone(zone).date() != local_now.date():
            continue
        by_symbol[row.symbol].append(row)

    window_end = (
        window_end.astimezone(zone)
        if window_end is not None
        else local_now.replace(second=0, microsecond=0)
    )
    if window_end > local_now or local_now - window_end > timedelta(seconds=90):
        raise ValueError("window_end must be no more than 90 seconds before observed_at")
    boundary = window_end - timedelta(minutes=window_minutes)
    benchmark_pair = _window_pair(by_symbol.get(benchmark_symbol, []), boundary, window_end)
    benchmark_pct = _window_return(benchmark_pair)
    benchmark_points = by_symbol.get(benchmark_symbol, [])
    if not benchmark_points or not _fresh(benchmark_points[-1], local_now, zone, window_end):
        benchmark_pct = None
    positions: list[HoldingView] = []
    total_cost = sum((holding.cost_cny * holding.shares for holding in holdings), Decimal(0))
    total_value = Decimal(0)
    data_complete = True
    for holding in holdings:
        points = by_symbol.get(holding.symbol, [])
        latest = points[-1] if points else None
        fresh = bool(latest and _fresh(latest, local_now, zone, window_end))
        if not fresh:
            data_complete = False
            unconfirmed_close = bool(
                latest
                and window_end.time() == time(15, 0)
                and latest.quote_at.astimezone(zone) < window_end
            )
            positions.append(
                HoldingView(
                    symbol=holding.symbol,
                    name=holding.name,
                    shares=holding.shares,
                    cost_cny=holding.cost_cny,
                    latest_cny=None,
                    quote_at=latest.quote_at.astimezone(zone) if latest else None,
                    pnl_cny=None,
                    pnl_pct=None,
                    day_pct=None,
                    window_pct=None,
                    benchmark_window_pct=benchmark_pct,
                    relative_window_pct=None,
                    historical_days=0,
                    historical_latest_date=None,
                    historical_median_pct=None,
                    historical_excess_pct=None,
                    historical_volume_ratio=None,
                    history_status="收盘价未确认" if unconfirmed_close else "行情缺失或过期",
                    state="收盘价未确认" if unconfirmed_close else "行情缺失或过期",
                    guidance=(
                        "等待正式收盘价；不作最终走势判断。"
                        if unconfirmed_close
                        else "暂停走势判断；核实行情与实际持仓。"
                    ),
                )
            )
            continue
        assert latest is not None
        price = latest.latest
        total_value += price * holding.shares
        cost_return = (price / holding.cost_cny - 1) * _HUNDRED
        window_pair = _window_pair(points, boundary, window_end)
        window_pct = _window_return(window_pair)
        relative = (
            window_pct - benchmark_pct
            if window_pct is not None and benchmark_pct is not None
            else None
        )
        day_pct = (
            (price / latest.previous_close - 1) * _HUNDRED if latest.previous_close > 0 else None
        )
        reference = history.windows.get(holding.symbol) if history is not None else None
        history_status = history.status if history is not None else "未接入历史分钟线"
        volume_ratio = None
        if reference is not None and window_pct is not None:
            live_volume = _window_volume(window_pair)
            if (
                live_volume is not None
                and boundary.time() != time(9, 30)
                and reference.median_volume_shares > 0
            ):
                volume_ratio = Decimal(live_volume) / reference.median_volume_shares
            history_status = "可比" if volume_ratio is not None else "价格可比，成交量不可比"
        elif reference is not None:
            history_status = "当前窗口数据不足"
        state, guidance = _guidance(
            cost_return, window_pct, relative, volume_ratio, thresholds=thresholds
        )
        positions.append(
            HoldingView(
                symbol=holding.symbol,
                name=holding.name,
                shares=holding.shares,
                cost_cny=holding.cost_cny,
                latest_cny=price,
                quote_at=latest.quote_at.astimezone(zone),
                pnl_cny=(price - holding.cost_cny) * holding.shares,
                pnl_pct=cost_return,
                day_pct=day_pct,
                window_pct=window_pct,
                benchmark_window_pct=benchmark_pct,
                relative_window_pct=relative,
                historical_days=reference.days if reference is not None else 0,
                historical_latest_date=reference.latest_date if reference is not None else None,
                historical_median_pct=reference.median_return_pct
                if reference is not None and window_pct is not None
                else None,
                historical_excess_pct=window_pct - reference.median_return_pct
                if reference is not None and window_pct is not None
                else None,
                historical_volume_ratio=volume_ratio,
                history_status=history_status,
                state=state,
                guidance=guidance,
            )
        )
    pnl = total_value - total_cost if data_complete else None
    return HoldingsReport(
        observed_at=local_now,
        window_minutes=window_minutes,
        guidance_thresholds=thresholds,
        positions=tuple(positions),
        total_cost_cny=total_cost,
        total_value_cny=total_value if data_complete else None,
        total_pnl_cny=pnl,
        total_pnl_pct=pnl / total_cost * _HUNDRED if pnl is not None else None,
        data_complete=data_complete,
    )


def _window_pair(
    points: list[ProviderQuoteSnapshot], boundary: datetime, window_end: datetime
) -> tuple[ProviderQuoteSnapshot, ProviderQuoteSnapshot] | None:
    clock = boundary.time()
    if not (time(9, 30) <= clock <= time(11, 30) or time(13, 0) <= clock <= time(15, 0)):
        return None
    if not points:
        return None
    target = boundary.astimezone(UTC)
    end_target = window_end.astimezone(UTC)
    prior = min(points, key=lambda item: (abs(item.quote_at - target), item.fetched_at))
    last = min(
        points,
        key=lambda item: (abs(item.quote_at - end_target), -item.fetched_at.timestamp()),
    )
    if (
        abs(prior.quote_at - target) > _WINDOW_ALIGNMENT_TOLERANCE
        or abs(last.quote_at - end_target) > _WINDOW_ALIGNMENT_TOLERANCE
        or (boundary.time() == time(9, 30) and prior.quote_at < target)
        or (window_end.time() == time(15, 0) and last.quote_at < end_target)
        or last.fetched_at <= prior.fetched_at
        or last.quote_at <= prior.quote_at
        or prior.latest <= 0
    ):
        return None
    return prior, last


def _window_return(
    pair: tuple[ProviderQuoteSnapshot, ProviderQuoteSnapshot] | None,
) -> Decimal | None:
    if pair is None:
        return None
    prior, last = pair
    return (last.latest / prior.latest - 1) * _HUNDRED


def _window_volume(pair: tuple[ProviderQuoteSnapshot, ProviderQuoteSnapshot] | None) -> int | None:
    if pair is None:
        return None
    prior, last = pair
    if last.volume_shares < prior.volume_shares:
        return None
    return last.volume_shares - prior.volume_shares


def _fresh(
    point: ProviderQuoteSnapshot, now: datetime, zone: ZoneInfo, window_end: datetime
) -> bool:
    return (
        now - point.fetched_at.astimezone(zone) <= _MAX_QUOTE_AGE
        and now - point.quote_at.astimezone(zone) <= _MAX_QUOTE_AGE
        and (window_end.time() != time(15, 0) or point.quote_at.astimezone(zone) >= window_end)
    )


def guidance_thresholds(window_minutes: int) -> GuidanceThresholds:
    """Scale 15-minute price bands by sqrt(time), rounding to 0.1 percentage point."""
    if window_minutes < 1:
        raise ValueError("window_minutes must be positive")
    scale = (Decimal(window_minutes) / Decimal(15)).sqrt()
    large_move = (Decimal("1.0") * scale).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    moderate_move = (Decimal("0.5") * scale).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    return GuidanceThresholds(
        window_minutes=window_minutes,
        large_move_pct=large_move,
        moderate_move_pct=moderate_move,
        relative_weak_pct=moderate_move,
    )


def _guidance(
    cost_return: Decimal,
    window_pct: Decimal | None,
    relative_pct: Decimal | None,
    historical_volume_ratio: Decimal | None,
    *,
    thresholds: GuidanceThresholds,
) -> tuple[str, str]:
    if window_pct is None:
        return "窗口数据不足", "暂不作本窗口趋势判断；观察下一时段。"
    if cost_return <= -thresholds.cost_loss_pct and window_pct <= -thresholds.large_move_pct:
        return "风险升高", "核对预设止损与仓位；避免仅因成本价而补仓。"
    if historical_volume_ratio is not None and historical_volume_ratio >= thresholds.volume_ratio:
        if window_pct <= -thresholds.moderate_move_pct:
            return "放量走弱", "本时段量高于历史同窗且价格下行；复核预设风险位，暂缓加仓。"
        if window_pct >= thresholds.moderate_move_pct:
            return "放量走强", "本时段量高于历史同窗；观察延续性，按既定计划管理仓位。"
    if (
        window_pct <= -thresholds.large_move_pct
        and relative_pct is not None
        and relative_pct <= -thresholds.relative_weak_pct
    ):
        return "短线偏弱", "检查风险位及是否弱于大盘；暂缓加仓。"
    if window_pct >= thresholds.large_move_pct and cost_return > 0:
        return "短线走强", "按既定计划评估分批锁定利润；避免追高。"
    if window_pct >= thresholds.moderate_move_pct:
        return "温和走强", "观察量价延续性，不因单个窗口追涨。"
    return "震荡观察", "维持既定计划；等待更明确的量价信号。"


def format_holdings_report(report: HoldingsReport) -> str:
    """Put actionable status and fresh prices first on the phone card."""
    valid_count = sum(value.latest_cny is not None for value in report.positions)
    attention = [
        value for value in report.positions if value.state in {"风险升高", "放量走弱", "短线偏弱"}
    ]
    lines = [
        f"**{report.observed_at.strftime('%m-%d %H:%M')} 持仓速览** · 近{report.window_minutes}分钟 · 有效行情 {valid_count}/{len(report.positions)}",
    ]
    if attention:
        names = "、".join(f"{value.name}（{value.state}）" for value in attention)
        lines.append(f"**优先复核 {len(attention)} 只**：{names}")
    else:
        lines.append("**优先复核**：暂无突出风险信号")
    if report.data_complete:
        assert report.total_value_cny is not None and report.total_pnl_cny is not None
        assert report.total_pnl_pct is not None
        lines.append(
            f"**组合浮盈亏**：{_signed(report.total_pnl_cny, 2)} 元"
            f"（{_pct(report.total_pnl_pct)}） · 市值 ¥{report.total_value_cny:,.2f}"
        )
    else:
        lines.append("**组合估值**：部分行情缺失，暂停汇总")
    for value in report.positions:
        lines.append("\n——")
        lines.append(f"**{value.name} {value.symbol}** · **{value.state}**")
        if value.latest_cny is None:
            lines.append("**现价**：无有效报价，暂停价格与盈亏判断")
            lines.append(f"**操作建议**：{value.guidance}")
            if value.quote_at is not None:
                lines.append(f"最近报价：{value.quote_at.strftime('%H:%M:%S')}（未用于判断）")
            continue
        lines.append(
            f"**现价**：{_highlight_price(value.latest_cny, value.cost_cny)}"
            f" · **较成本**：{_pct(value.pnl_pct)}"
        )
        lines.append(
            f"**近{report.window_minutes}分** {_pct(value.window_pct)}"
            f" · 相对沪深300 {_points(value.relative_window_pct)}"
            f" · 日内 {_pct(value.day_pct)}"
        )
        lines.append(f"**操作建议**：{value.guidance}")
        lines.append(
            f"持仓 {value.shares} 股 · 成本 ¥{_money(value.cost_cny)}"
            f" · 浮盈亏 {_signed(value.pnl_cny, 2)} 元"
        )
        if value.historical_median_pct is not None:
            lines.append(
                f"历史同窗{value.historical_days}日中位 {_pct(value.historical_median_pct)} · "
                f"本窗较中位 {_points(value.historical_excess_pct)} · "
                f"成交量比 {_ratio(value.historical_volume_ratio)}"
            )
        else:
            lines.append(f"历史同窗：{value.history_status}")
        source_line = (
            f"报价 {value.quote_at.strftime('%H:%M:%S')}" if value.quote_at else "报价时间缺失"
        )
        if value.historical_latest_date is not None:
            source_line += f" · 历史截至 {value.historical_latest_date.isoformat()}"
        lines.append(source_line)
    lines.append(
        "\n**现价颜色**：高于成本为红色，低于成本为绿色；与日涨跌无关。"
        "\n*腾讯实时 / 新浪历史；跨源量比仅供粗略参考，09:30起始窗口不比较量。"
        "数值展示保留两位，计算沿用原始精度。阈值未经回测；"
        "人工维护持仓，不自动下单。成本不含费用税费，成交后请更新持仓。*"
    )
    return "\n".join(lines)


def _signed(value: Decimal | None, places: int) -> str:
    if value is None:
        return "无数据"
    rounded = value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)
    return f"{rounded:+,.{places}f}"


def _money(value: Decimal) -> str:
    rounded = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{rounded:,.2f}"


def _highlight_price(price: Decimal, cost: Decimal) -> str:
    rendered = f"¥{_money(price)}"
    if price < cost:
        return f"**<font color='green'>{rendered}</font>**"
    if price > cost:
        return f"**<font color='red'>{rendered}</font>**"
    return f"**{rendered}**"


def _pct(value: Decimal | None) -> str:
    return f"{_signed(value, 2)}%" if value is not None else "无数据"


def _points(value: Decimal | None) -> str:
    return f"{_signed(value, 2)} 个百分点" if value is not None else "无数据"


def _ratio(value: Decimal | None) -> str:
    return f"{value:.2f} 倍" if value is not None else "不可比"
