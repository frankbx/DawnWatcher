#!/usr/bin/env python3
"""Download and label complete Sina one-minute trading days via AkShare.

Requires an environment with akshare, pandas, and pyarrow. Output stays separate
from the production SQLite database and locally sampled minute bars.
"""

from __future__ import annotations

import argparse
import json
import re
import signal
import time
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import akshare as ak
import pandas as pd

ZONE = ZoneInfo("Asia/Shanghai")
SYMBOL_PATTERN = re.compile(r"^\d{6}\.(?:SH|SZ)$")
EXPECTED_LABELS = (
    tuple(pd.date_range("2000-01-01 09:31", "2000-01-01 11:30", freq="min").strftime("%H:%M"))
    + tuple(pd.date_range("2000-01-01 13:01", "2000-01-01 14:57", freq="min").strftime("%H:%M"))
    + ("15:00",)
)
NUMERIC_COLUMNS = ("open", "high", "low", "close", "volume", "amount")


def _request_timeout(_signum: int, _frame: object) -> None:
    raise TimeoutError("AkShare request exceeded 20 seconds")


def _symbols_from(path: Path) -> list[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if "requested_symbols" in payload:
        values = payload["requested_symbols"]
    elif "positions" in payload:
        values = [position["symbol"] for position in payload["positions"]]
    elif "members" in payload:
        values = [member["ts_code"] for member in payload["members"]]
    else:
        raise ValueError("symbol file must be a comparison report, holdings, or stock-pool JSON")
    if not isinstance(values, list) or not values:
        raise ValueError("symbol list is empty")
    symbols = list(dict.fromkeys(values))
    if not all(isinstance(value, str) and SYMBOL_PATTERN.fullmatch(value) for value in symbols):
        raise ValueError("symbols must use Tushare format, e.g. 600519.SH")
    return symbols


def _provider_symbol(symbol: str) -> str:
    code, market = symbol.split(".")
    return ("sh" if market == "SH" else "sz") + code


def _validate_day(group: pd.DataFrame) -> dict[str, object]:
    labels = group["day"].dt.strftime("%H:%M")
    observed = set(labels)
    expected = set(EXPECTED_LABELS)
    nulls = group[list(NUMERIC_COLUMNS)].isna().sum().sum()
    invalid_prices = (
        (group["high"] < group[["open", "close", "low"]].max(axis=1))
        | (group["low"] > group[["open", "close", "high"]].min(axis=1))
    ).sum()
    negative_activity = ((group["volume"] < 0) | (group["amount"] < 0)).sum()
    missing = sorted(expected - observed)
    unexpected = sorted(observed - expected)
    duplicates = int(labels.duplicated().sum())
    complete = not (
        missing or unexpected or duplicates or nulls or invalid_prices or negative_activity
    )
    return {
        "date": str(group["day"].dt.date.iloc[0]),
        "symbol": str(group["symbol"].iloc[0]),
        "rows": len(group),
        "complete": complete,
        "missing_labels": missing,
        "unexpected_labels": unexpected,
        "duplicate_labels": duplicates,
        "numeric_nulls": int(nulls),
        "ohlc_invalid": int(invalid_prices),
        "negative_volume_or_amount": int(negative_activity),
    }


def _label_starts(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.rename(
        columns={
            "day": "source_label",
            "volume": "volume_shares",
            "amount": "amount_cny",
        }
    ).copy()
    result["source_label"] = result["source_label"].dt.tz_localize(ZONE)
    closing = result["source_label"].dt.strftime("%H:%M").eq("15:00")
    result["minute_start"] = result["source_label"] - pd.Timedelta(minutes=1)
    result.loc[closing, "minute_start"] = result.loc[closing, "source_label"] - pd.Timedelta(
        minutes=3
    )
    result["minute_end"] = result["source_label"]
    result["minute_start_utc"] = result["minute_start"].dt.tz_convert(UTC)
    result["bar_type"] = "continuous_1m"
    result.loc[closing, "bar_type"] = "closing_call_auction_3m"
    result["interval_seconds"] = 60
    result.loc[closing, "interval_seconds"] = 180
    result["trade_date"] = result["source_label"].dt.strftime("%Y-%m-%d")
    result["source"] = "akshare_stock_zh_a_minute_sina"
    return result[
        [
            "symbol",
            "trade_date",
            "minute_start",
            "minute_end",
            "minute_start_utc",
            "source_label",
            "bar_type",
            "interval_seconds",
            "open",
            "high",
            "low",
            "close",
            "volume_shares",
            "amount_cny",
            "source",
        ]
    ].sort_values(["trade_date", "symbol", "minute_start"])


def _price_points(bars: pd.DataFrame) -> pd.DataFrame:
    """Expose 09:30 open and every bar close, including the 15:00 final price."""
    opening = bars[bars["minute_start"].dt.strftime("%H:%M").eq("09:30")].copy()
    if len(opening) != bars["symbol"].nunique():
        raise ValueError("expected one 09:30 opening bar per symbol and date")
    starts = opening[["symbol", "trade_date", "minute_start", "open", "source_label"]].rename(
        columns={"minute_start": "timestamp", "open": "price"}
    )
    starts["point_type"] = "session_open"
    starts["source_field"] = "open"
    ends = bars[["symbol", "trade_date", "minute_end", "close", "source_label", "bar_type"]].rename(
        columns={"minute_end": "timestamp", "close": "price"}
    )
    ends["point_type"] = "minute_close"
    ends.loc[ends["bar_type"].eq("closing_call_auction_3m"), "point_type"] = "session_close"
    ends["source_field"] = "close"
    points = pd.concat([starts, ends], ignore_index=True)
    points["timestamp_utc"] = points["timestamp"].dt.tz_convert(UTC)
    points = points[
        [
            "symbol",
            "trade_date",
            "timestamp",
            "timestamp_utc",
            "price",
            "point_type",
            "source_label",
            "source_field",
        ]
    ].sort_values(["symbol", "timestamp"])
    if points.duplicated(["symbol", "timestamp"]).any():
        raise ValueError("price series contains a duplicate symbol/timestamp")
    if len(points) != len(bars) + bars["symbol"].nunique():
        raise ValueError("price series does not contain exactly one added open point per symbol")
    if points["point_type"].eq("session_close").sum() != bars["symbol"].nunique():
        raise ValueError("expected one 15:00 closing point per symbol and date")
    if not (
        points[points["point_type"].eq("session_close")]["timestamp"].dt.strftime("%H:%M")
        == "15:00"
    ).all():
        raise ValueError("session close must be timestamped at 15:00")
    return points


def _write_price_points(output: Path, manifest: dict[str, object]) -> None:
    files = manifest["day_files"]
    if not isinstance(files, dict):
        raise ValueError("manifest day_files is invalid")
    point_files: dict[str, str] = {}
    total_rows = 0
    for trade_date, source_path in files.items():
        bars = pd.read_parquet(source_path)
        points = _price_points(bars)
        path = output / f"trade_date={trade_date}" / "price_points.parquet"
        if path.exists():
            raise FileExistsError(f"price-point file already exists: {path}")
        points.to_parquet(path, index=False)
        point_files[str(trade_date)] = str(path)
        total_rows += len(points)
    manifest["price_point_files"] = point_files
    manifest["price_point_rows"] = total_rows
    manifest["price_point_convention"] = (
        "One 09:30 session_open point from the first bar's open, each bar's close at its "
        "source end label, and a 15:00 session_close point from the auction bar's close. "
        "Price points contain no extra volume or amount and are not extra minute bars."
    )
    (output / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def main() -> None:
    as_of = datetime.now(ZONE).date()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--symbol-file",
        type=Path,
        default=Path("data/reports/akshare-minute-compare-2026-09-29/report.json"),
        help="Prior comparison report or stock-pool JSON.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(f"data/lake/akshare_sina_full_day/as_of={as_of.isoformat()}"),
    )
    parser.add_argument("--request-gap", type=float, default=0.8)
    parser.add_argument(
        "--derive-only",
        action="store_true",
        help="Add price-point files to an existing full-day download without new requests.",
    )
    args = parser.parse_args()
    if args.request_gap < 0:
        parser.error("--request-gap cannot be negative")
    if args.derive_only:
        manifest = json.loads((args.output / "manifest.json").read_text(encoding="utf-8"))
        _write_price_points(args.output, manifest)
        print(f"Wrote {manifest['price_point_rows']} price points without network requests")
        return
    symbols = _symbols_from(args.symbol_file)
    if args.output.exists():
        parser.error(f"output already exists: {args.output}; choose a new --output")
    args.output.mkdir(parents=True)
    signal.signal(signal.SIGALRM, _request_timeout)

    frames: list[pd.DataFrame] = []
    failed: list[dict[str, str]] = []
    timings: dict[str, float] = {}
    for number, symbol in enumerate(symbols, start=1):
        started = time.monotonic()
        try:
            signal.alarm(20)
            response = ak.stock_zh_a_minute(symbol=_provider_symbol(symbol), period="1", adjust="")
            signal.alarm(0)
            if response.empty:
                raise ValueError("empty response")
            if not set(("day", *NUMERIC_COLUMNS)).issubset(response.columns):
                raise ValueError(f"unexpected columns: {list(response.columns)}")
            response = response[["day", *NUMERIC_COLUMNS]].copy()
            response["symbol"] = symbol
            frames.append(response)
            timings[symbol] = round(time.monotonic() - started, 3)
            print(f"[{number}/{len(symbols)}] {symbol}: {len(response)} rows", flush=True)
        except Exception as exc:
            signal.alarm(0)
            failed.append({"symbol": symbol, "error": f"{type(exc).__name__}: {exc}"})
            print(f"[{number}/{len(symbols)}] {symbol}: FAILED {exc}", flush=True)
        time.sleep(args.request_gap)

    if not frames:
        raise RuntimeError("all AkShare requests failed")
    raw = pd.concat(frames, ignore_index=True)
    raw.to_parquet(args.output / "raw_response.parquet", index=False)
    source = raw.copy()
    source["day"] = pd.to_datetime(source["day"], errors="coerce")
    if source["day"].isna().any():
        raise ValueError("response contains unparseable timestamps; raw response retained")
    for column in NUMERIC_COLUMNS:
        source[column] = pd.to_numeric(source[column], errors="coerce")

    checks = [
        _validate_day(group)
        for _, group in source.groupby(["symbol", source["day"].dt.date], sort=True)
    ]
    valid = {(row["symbol"], row["date"]) for row in checks if row["complete"]}
    source["trade_date"] = source["day"].dt.strftime("%Y-%m-%d")
    selected = source[pd.MultiIndex.from_frame(source[["symbol", "trade_date"]]).isin(valid)].drop(
        columns="trade_date"
    )
    if selected.empty:
        raise RuntimeError("no complete symbol-days found; raw response retained")
    bars = _label_starts(selected)
    day_files: dict[str, str] = {}
    for trade_date, day_bars in bars.groupby("trade_date", sort=True):
        path = args.output / f"trade_date={trade_date}" / "part-000.parquet"
        path.parent.mkdir()
        day_bars.to_parquet(path, index=False)
        day_files[str(trade_date)] = str(path)

    all_symbols_complete = sorted(
        trade_date
        for trade_date, group in bars.groupby("trade_date")
        if group["symbol"].nunique() == len(symbols)
    )
    manifest = {
        "source": "AkShare stock_zh_a_minute / Sina / 1 minute / unadjusted",
        "akshare_version": ak.__version__,
        "downloaded_at": datetime.now(UTC).isoformat(),
        "timezone": "Asia/Shanghai",
        "symbol_file": str(args.symbol_file),
        "requested_symbols": symbols,
        "successful_symbols": len(frames),
        "failed_symbols": failed,
        "request_seconds": timings,
        "raw_rows": len(raw),
        "complete_symbol_days": len(valid),
        "full_day_rows": len(bars),
        "expected_source_labels_per_day": len(EXPECTED_LABELS),
        "source_label_convention": (
            "Ordinary Sina labels are interval ends: 09:31 -> [09:30,09:31). "
            "15:00 is the 14:57-15:00 closing auction, not a 14:59 one-minute bar."
        ),
        "opening_bar_caveat": (
            "09:31 is labeled [09:30,09:31), but Sina does not separately expose "
            "the 09:25 opening auction in this response; its volume allocation is unverified."
        ),
        "complete_dates_for_all_requested_symbols": all_symbols_complete,
        "day_files": day_files,
        "symbol_day_checks": checks,
    }
    _write_price_points(args.output, manifest)
    print(
        json.dumps(
            {
                key: manifest[key]
                for key in (
                    "successful_symbols",
                    "failed_symbols",
                    "raw_rows",
                    "complete_symbol_days",
                    "full_day_rows",
                    "complete_dates_for_all_requested_symbols",
                )
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
