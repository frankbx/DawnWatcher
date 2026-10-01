#!/usr/bin/env python3
"""Cache one unadjusted Sina daily bar for each pool and holding symbol.

AKShare wraps Sina's stock and ETF history endpoints. A JSONL checkpoint makes
this deliberately paced, one-symbol-at-a-time download resumable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import time
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

_SYMBOL = re.compile(r"^\d{6}\.(?:SH|SZ)$")
_ZONE = ZoneInfo("Asia/Shanghai")


def load_members(pool_file: Path, holdings_file: Path) -> list[dict[str, str]]:
    pool = json.loads(pool_file.read_text(encoding="utf-8"))["members"]
    holdings = json.loads(holdings_file.read_text(encoding="utf-8"))["positions"]
    members: dict[str, dict[str, str]] = {}
    for item in pool:
        symbol = item["ts_code"]
        if not _SYMBOL.fullmatch(symbol) or symbol in members:
            raise ValueError(f"invalid or duplicate pool symbol: {symbol}")
        kind = item["instrument_type"]
        if kind not in {"stock", "etf"}:
            raise ValueError(f"unsupported pool instrument type: {kind}")
        members[symbol] = {
            "symbol": symbol,
            "name": item["name"],
            "instrument_type": kind,
            "in_pool": True,
            "in_holdings": False,
        }
    for item in holdings:
        symbol = item["symbol"]
        if not _SYMBOL.fullmatch(symbol):
            raise ValueError(f"invalid holding symbol: {symbol}")
        if symbol in members:
            members[symbol]["in_holdings"] = True
        else:
            members[symbol] = {
                "symbol": symbol,
                "name": item["name"],
                "instrument_type": "stock",
                "in_pool": False,
                "in_holdings": True,
            }
    return [members[symbol] for symbol in sorted(members)]


def _timeout(_signum: int, _frame: object) -> None:
    raise TimeoutError("Sina daily request timed out")


def _provider_symbol(symbol: str) -> str:
    code, exchange = symbol.split(".")
    return ("sh" if exchange == "SH" else "sz") + code


def _finite_nonnegative(value: object, label: str) -> float:
    number = float(value)
    if not pd.notna(number) or number < 0 or number == float("inf"):
        raise ValueError(f"invalid {label}: {value}")
    return number


def download_one(ak: object, member: dict[str, str], trade_date: date) -> dict[str, object]:
    symbol = member["symbol"]
    provider_symbol = _provider_symbol(symbol)
    if member["instrument_type"] == "etf":
        frame = ak.fund_etf_hist_sina(symbol=provider_symbol)
        endpoint = "fund_etf_hist_sina"
    else:
        frame = ak.stock_zh_a_daily(
            symbol=provider_symbol,
            start_date=trade_date.strftime("%Y%m%d"),
            end_date=trade_date.strftime("%Y%m%d"),
            adjust="",
        )
        endpoint = "stock_zh_a_daily"
    if frame.empty or "date" not in frame.columns:
        raise ValueError("Sina returned no dated bars")
    target = frame.loc[pd.to_datetime(frame["date"]).dt.date == trade_date]
    if len(target) != 1:
        raise ValueError(f"expected one bar for {trade_date}, found {len(target)}")
    source = target.iloc[0]
    prices = {
        key: _finite_nonnegative(source[key], key) for key in ("open", "high", "low", "close")
    }
    if (
        min(prices.values()) <= 0
        or prices["high"] < max(prices.values())
        or prices["low"] > min(prices.values())
    ):
        raise ValueError(f"invalid OHLC for {symbol}")
    volume = _finite_nonnegative(source["volume"], "volume")
    amount = _finite_nonnegative(source["amount"], "amount")
    return {
        "trade_date": trade_date.isoformat(),
        **member,
        "source": "sina_via_akshare",
        "endpoint": endpoint,
        "adjust": "none",
        **prices,
        "volume_shares": volume,
        "amount_cny": amount,
    }


def _checkpoint(path: Path) -> dict[str, dict[str, object]]:
    rows: dict[str, dict[str, object]] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                row = json.loads(line)
                rows[str(row["symbol"])] = row
    return rows


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def run(args: argparse.Namespace) -> dict[str, object]:
    import akshare as ak

    members = load_members(args.pool_file, args.holdings_file)
    output = args.output / f"trade_date={args.trade_date.isoformat()}"
    output.mkdir(parents=True, exist_ok=True)
    checkpoint = output / "download-checkpoint.jsonl"
    cached = _checkpoint(checkpoint)
    signal.signal(signal.SIGALRM, _timeout)
    failures: dict[str, str] = {}
    for index, member in enumerate(members, start=1):
        symbol = member["symbol"]
        if symbol in cached:
            continue
        for attempt in range(1, args.retries + 1):
            try:
                signal.alarm(args.timeout_seconds)
                row = download_one(ak, member, args.trade_date)
                with checkpoint.open("a", encoding="utf-8") as stream:
                    stream.write(json.dumps(row, ensure_ascii=False) + "\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                cached[symbol] = row
                print(f"[{index}/{len(members)}] {symbol}: ok", flush=True)
                break
            except Exception as exc:
                failures[symbol] = f"{type(exc).__name__}: {exc}"
                print(
                    f"[{index}/{len(members)}] {symbol}: attempt {attempt} failed: {failures[symbol]}",
                    flush=True,
                )
                if attempt < args.retries:
                    time.sleep(min(10.0, args.delay_seconds * (2**attempt)))
            finally:
                signal.alarm(0)
        time.sleep(args.delay_seconds)
    requested = {member["symbol"] for member in members}
    rows = [cached[symbol] for symbol in sorted(requested & cached.keys())]
    if len(rows) != len({str(row["symbol"]) for row in rows}):
        raise ValueError("duplicate symbols in daily output")
    frame = pd.DataFrame(rows)
    parquet = output / "day.parquet"
    if not frame.empty:
        temporary = output / f".day.{os.getpid()}.parquet"
        frame.to_parquet(temporary, index=False, compression="zstd")
        os.replace(temporary, parquet)
        restored = pd.read_parquet(parquet)
        if len(restored) != len(rows) or set(restored["symbol"]) != set(cached) & requested:
            raise RuntimeError("daily Parquet verification failed")
    missing = sorted(requested - cached.keys())
    manifest: dict[str, object] = {
        "schema_version": 1,
        "trade_date": args.trade_date.isoformat(),
        "source": "Sina via AKShare",
        "adjust": "none",
        "generated_at": datetime.now(_ZONE).isoformat(),
        "requested_symbol_count": len(requested),
        "pool_symbol_count": sum(bool(member["in_pool"]) for member in members),
        "holding_symbol_count": sum(bool(member["in_holdings"]) for member in members),
        "row_count": len(rows),
        "complete": not missing,
        "missing_symbols": missing,
        "failures": {symbol: failures.get(symbol, "not downloaded") for symbol in missing},
        "parquet_path": str(parquet.resolve()) if parquet.exists() else None,
        "parquet_sha256": hashlib.sha256(parquet.read_bytes()).hexdigest()
        if parquet.exists()
        else None,
    }
    _atomic_json(output / "manifest.json", manifest)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--pool-file", type=Path, default=Path("config/stock_pools/initial-v1/pool.json")
    )
    parser.add_argument("--holdings-file", type=Path, default=Path("data/private/holdings.json"))
    parser.add_argument("--output", type=Path, default=Path("data/lake/sina_daily"))
    parser.add_argument("--delay-seconds", type=float, default=1.0)
    parser.add_argument("--timeout-seconds", type=int, default=20)
    parser.add_argument("--retries", type=int, default=2)
    args = parser.parse_args()
    if args.delay_seconds < 0 or args.timeout_seconds < 1 or args.retries < 1:
        parser.error("delay must be nonnegative; timeout and retries must be positive")
    manifest = run(args)
    print(json.dumps(manifest, ensure_ascii=False, indent=2), flush=True)
    if not manifest["complete"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
