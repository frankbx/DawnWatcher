"""Same-clock reference windows from complete, normalized Sina minute days."""

from __future__ import annotations

import importlib
import json
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from statistics import median
from typing import Any

_MIN_DAYS = 5
_MAX_DAYS = 8
_MAX_CALENDAR_AGE = 21
_HUNDRED = Decimal(100)


@dataclass(frozen=True, slots=True)
class HistoricalWindow:
    days: int
    latest_date: date
    median_return_pct: Decimal
    median_volume_shares: Decimal


@dataclass(frozen=True, slots=True)
class HistoryReference:
    status: str
    windows: dict[str, HistoricalWindow] = field(default_factory=dict)
    latest_date: date | None = None


def load_history_reference(
    root: Path,
    *,
    symbols: tuple[str, ...],
    window_end: datetime,
    window_minutes: int = 15,
) -> HistoryReference:
    """Read at most eight past complete days; never read today's/future bars."""
    if window_end.tzinfo is None or window_end.utcoffset() is None:
        raise ValueError("window_end must be timezone-aware")
    if window_minutes < 1:
        raise ValueError("window_minutes must be positive")
    boundary = window_end - timedelta(minutes=window_minutes)
    if boundary.date() != window_end.date() or not _same_session(
        boundary.time(), window_end.time()
    ):
        return HistoryReference(status="非连续交易窗口")
    manifests = sorted(root.glob("holdings_as_of=*/manifest.json"), reverse=True)
    if not manifests:
        return HistoryReference(status="未找到持仓历史分钟线")
    for manifest_path in manifests:
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            if payload.get("timezone") != str(window_end.tzinfo):
                continue
            requested = payload.get("requested_symbols")
            if not isinstance(requested, list) or not set(symbols).intersection(requested):
                continue
            dates = [
                date.fromisoformat(value)
                for value in payload["complete_dates_for_all_requested_symbols"]
                if date.fromisoformat(value) < window_end.date()
            ]
            dates = sorted(set(dates))[-_MAX_DAYS:]
            if not dates:
                return HistoryReference(status="没有此前交易日的历史样本")
            last_date = dates[-1]
            if (window_end.date() - last_date).days > _MAX_CALENDAR_AGE:
                return HistoryReference(status="历史样本已过期", latest_date=last_date)
            return _read_windows(
                manifest_path.parent,
                dates,
                symbols,
                boundary,
                window_end,
            )
        except (OSError, ValueError, KeyError, TypeError, ImportError):
            continue
    return HistoryReference(status="历史文件不匹配或读取失败")


def _same_session(start: time, end: time) -> bool:
    return time(9, 30) <= start < end <= time(11, 30) or time(13, 0) <= start < end <= time(15, 0)


def _read_windows(
    directory: Path,
    dates: list[date],
    symbols: tuple[str, ...],
    boundary: datetime,
    window_end: datetime,
) -> HistoryReference:
    parquet = importlib.import_module("pyarrow.parquet")

    samples: dict[str, list[tuple[date, Decimal, int]]] = {symbol: [] for symbol in symbols}
    for trade_date in dates:
        path = directory / f"trade_date={trade_date.isoformat()}" / "part-000.parquet"
        try:
            bars = parquet.read_table(
                path,
                columns=[
                    "symbol",
                    "minute_start",
                    "minute_end",
                    "open",
                    "close",
                    "volume_shares",
                ],
            ).to_pylist()
        except (OSError, ValueError):
            continue
        window_bars: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in symbols}
        for bar in bars:
            symbol = bar["symbol"]
            if symbol not in window_bars:
                continue
            start = bar["minute_start"]
            end = bar["minute_end"]
            if boundary.time() <= start.time() and end.time() <= window_end.time():
                window_bars[symbol].append(bar)
        for symbol, items in window_bars.items():
            items.sort(key=lambda item: item["minute_start"])
            if not _complete_window(items, boundary.time(), window_end.time()):
                continue
            opening = Decimal(str(items[0]["open"]))
            closing = Decimal(str(items[-1]["close"]))
            volumes = [item["volume_shares"] for item in items]
            if (
                not opening.is_finite()
                or not closing.is_finite()
                or opening <= 0
                or closing <= 0
                or any(not isinstance(v, int) or v < 0 for v in volumes)
            ):
                continue
            samples[symbol].append((trade_date, (closing / opening - 1) * _HUNDRED, sum(volumes)))
    windows: dict[str, HistoricalWindow] = {}
    for symbol, values in samples.items():
        if len(values) < _MIN_DAYS:
            continue
        windows[symbol] = HistoricalWindow(
            days=len(values),
            latest_date=values[-1][0],
            median_return_pct=median(value[1] for value in values),
            median_volume_shares=median(Decimal(value[2]) for value in values),
        )
    return HistoryReference(
        status="可比" if len(windows) == len(symbols) else "部分标的历史样本不足",
        windows=windows,
        latest_date=dates[-1],
    )


def _complete_window(items: list[dict[str, Any]], start: time, end: time) -> bool:
    if not items or items[0]["minute_start"].time() != start:
        return False
    if items[-1]["minute_end"].time() != end:
        return False
    for item in items:
        duration = item["minute_end"] - item["minute_start"]
        if duration == timedelta(minutes=1):
            continue
        if not (
            duration == timedelta(minutes=3)
            and item["minute_start"].time() == time(14, 57)
            and item["minute_end"].time() == time(15, 0)
        ):
            return False
    return all(
        first["minute_end"] == second["minute_start"]
        for first, second in zip(items, items[1:], strict=False)
    )
