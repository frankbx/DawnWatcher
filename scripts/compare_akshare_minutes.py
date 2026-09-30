#!/usr/bin/env python3
"""Download a small AkShare minute sample and compare it with local bars.

Run with a Python environment containing akshare, pandas, and pyarrow.
The downloaded bars are kept separate from the production minute-bar database.
"""

from __future__ import annotations

import argparse
import json
import signal
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import akshare as ak
import pandas as pd

SAMPLE_SYMBOLS = (
    "600519.SH",
    "601318.SH",
    "600036.SH",
    "601899.SH",
    "603986.SH",
    "603259.SH",
    "600900.SH",
    "000333.SZ",
    "600150.SH",
    "600176.SH",
    "600938.SH",
    "600487.SH",
    "000977.SZ",
    "000338.SZ",
    "000001.SZ",
    "600000.SH",
    "510300.SH",
    "510500.SH",
    "159915.SZ",
    "588000.SH",
)
DATES = ("2026-09-23", "2026-09-24", "2026-09-28", "2026-09-29")
SHANGHAI = ZoneInfo("Asia/Shanghai")


def _timeout(_signum: int, _frame: object) -> None:
    raise TimeoutError("AkShare request exceeded 20 seconds")


def _sina_symbol(symbol: str) -> str:
    code, exchange = symbol.split(".")
    return ("sh" if exchange == "SH" else "sz") + code


def _is_continuous(timestamp: pd.Timestamp) -> bool:
    minute = timestamp.hour * 60 + timestamp.minute
    return 9 * 60 + 30 <= minute < 11 * 60 + 30 or 13 * 60 <= minute < 14 * 60 + 57


def _stats(values: pd.Series) -> dict[str, float | int | None]:
    values = pd.to_numeric(values, errors="coerce").dropna()
    if values.empty:
        return {"n": 0, "median": None, "p95": None, "max": None}
    return {
        "n": len(values),
        "median": round(float(values.median()), 6),
        "p95": round(float(values.quantile(0.95)), 6),
        "max": round(float(values.max()), 6),
    }


def _load_local(database: Path) -> pd.DataFrame:
    query = """
        SELECT symbol, trade_date, minute_start, open, high, low, close,
               volume_shares, amount_cny, sample_count, expected_sample_count,
               quality_flags
        FROM minute_bar
        WHERE trade_date IN ({}) AND symbol IN ({})
    """.format(",".join("?" for _ in DATES), ",".join("?" for _ in SAMPLE_SYMBOLS))
    with sqlite3.connect(f"file:{database.resolve()}?mode=ro", uri=True) as connection:
        frame = pd.read_sql_query(query, connection, params=(*DATES, *SAMPLE_SYMBOLS))
    if frame.empty:
        return frame
    frame["local_minute"] = (
        pd.to_datetime(frame["minute_start"], utc=True).dt.tz_convert(SHANGHAI).dt.tz_localize(None)
    )
    for field in ("open", "high", "low", "close", "volume_shares", "amount_cny"):
        frame[field] = pd.to_numeric(frame[field], errors="coerce")
    return frame


def _compare(reference: pd.DataFrame, local: pd.DataFrame, shift_minutes: int) -> pd.DataFrame:
    selected = reference.copy()
    selected["local_minute"] = selected["source_time"] + pd.Timedelta(minutes=shift_minutes)
    selected = selected[selected["local_minute"].map(_is_continuous)]
    local = local.rename(
        columns={name: f"{name}_local" for name in ("open", "high", "low", "close")}
    )
    return local.merge(selected, on=["symbol", "local_minute"], how="inner")


def _summarize(merged: pd.DataFrame) -> dict[str, object]:
    if merged.empty:
        return {"matched_minutes": 0}
    delta = (merged["close_local"] - merged["close_ak"]).abs()
    metrics: dict[str, object] = {
        "matched_minutes": len(merged),
        "close_abs_cny": _stats(delta),
        "close_within_0_01_pct": round(float((delta <= 0.010001).mean() * 100), 2),
        "close_within_0_05_pct": round(float((delta <= 0.050001).mean() * 100), 2),
        "local_high_not_above_reference_pct": round(
            float((merged["high_local"] <= merged["high_ak"] + 0.010001).mean() * 100), 2
        ),
        "local_low_not_below_reference_pct": round(
            float((merged["low_local"] >= merged["low_ak"] - 0.010001).mean() * 100), 2
        ),
    }
    usable = merged[merged["volume_shares"].notna() & merged["amount_cny"].notna()].copy()
    metrics["volume_amount_matched_minutes"] = len(usable)
    if not usable.empty:
        for local_name, reference_name, label in (
            ("volume_shares", "volume", "volume_shares"),
            ("amount_cny", "amount", "amount_cny"),
        ):
            local_sum = float(usable[local_name].sum())
            reference_sum = float(usable[reference_name].sum())
            metrics[f"{label}_local_sum"] = round(local_sum, 4)
            metrics[f"{label}_reference_sum"] = round(reference_sum, 4)
            metrics[f"{label}_ratio"] = (
                round(local_sum / reference_sum, 6) if reference_sum else None
            )
            metrics[f"{label}_minute_abs_pct"] = _stats(
                (
                    (usable[local_name] - usable[reference_name]).abs()
                    / usable[reference_name].replace(0, float("nan"))
                )
                * 100
            )
    return metrics


def _compare_closing_auction(reference: pd.DataFrame, local: pd.DataFrame) -> pd.DataFrame:
    final = reference[reference["source_time"] == pd.Timestamp("2026-09-29 15:00:00")]
    auction = local[
        (local["trade_date"] == "2026-09-29")
        & (local["local_minute"].dt.strftime("%H:%M").between("14:57", "14:59"))
    ].sort_values("local_minute")
    grouped = auction.groupby("symbol").agg(
        local_last_close=("close", "last"),
        local_volume=("volume_shares", "sum"),
        local_amount=("amount_cny", "sum"),
        local_minutes=("close", "size"),
    )
    result = final[["symbol", "close_ak", "volume", "amount"]].merge(
        grouped, on="symbol", how="inner"
    )
    result["close_gap_cny"] = result["close_ak"] - result["local_last_close"]
    result["volume_capture_ratio"] = result["local_volume"] / result["volume"]
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path("data/db/regimebeacon.sqlite3"))
    parser.add_argument(
        "--output", type=Path, default=Path("data/reports/akshare-minute-compare-2026-09-29")
    )
    parser.add_argument("--reuse-download", action="store_true", help="Reuse saved Sina Parquet")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    signal.signal(signal.SIGALRM, _timeout)

    failures: list[dict[str, str]] = []
    timings: dict[str, float] = {}
    if args.reuse_download:
        download_file = args.output / "akshare_sina_1m.parquet"
        reference = pd.read_parquet(download_file)
        downloaded_at = datetime.fromtimestamp(download_file.stat().st_mtime, UTC).isoformat()
        old_report = args.output / "report.json"
        if old_report.exists():
            previous = json.loads(old_report.read_text(encoding="utf-8"))
            failures = previous.get("failed_symbols", [])
            timings = previous.get("request_seconds", {})
    else:
        downloaded: list[pd.DataFrame] = []
        for index, symbol in enumerate(SAMPLE_SYMBOLS, start=1):
            started = time.monotonic()
            try:
                signal.alarm(20)
                frame = ak.stock_zh_a_minute(symbol=_sina_symbol(symbol), period="1", adjust="")
                signal.alarm(0)
                if frame.empty:
                    raise ValueError("empty response")
                frame = frame.rename(
                    columns={
                        "day": "source_time",
                        "open": "open_ak",
                        "high": "high_ak",
                        "low": "low_ak",
                        "close": "close_ak",
                    }
                )
                frame["source_time"] = pd.to_datetime(frame["source_time"])
                frame = frame[frame["source_time"].dt.strftime("%Y-%m-%d").isin(DATES)].copy()
                for name in ("open_ak", "high_ak", "low_ak", "close_ak", "volume", "amount"):
                    frame[name] = pd.to_numeric(frame[name], errors="coerce")
                frame["symbol"] = symbol
                frame["source"] = "akshare_sina"
                downloaded.append(frame)
                timings[symbol] = round(time.monotonic() - started, 3)
                print(f"[{index}/{len(SAMPLE_SYMBOLS)}] {symbol}: {len(frame)} rows", flush=True)
            except Exception as exc:
                signal.alarm(0)
                failures.append({"symbol": symbol, "error": f"{type(exc).__name__}: {exc}"})
                print(f"[{index}/{len(SAMPLE_SYMBOLS)}] {symbol}: FAILED {exc}", flush=True)
            time.sleep(0.8)

        if not downloaded:
            raise RuntimeError("All AkShare requests failed; no comparison data available")
        reference = pd.concat(downloaded, ignore_index=True)
        reference.to_parquet(args.output / "akshare_sina_1m.parquet", index=False)
        downloaded_at = datetime.now(UTC).isoformat()
    local = _load_local(args.database)
    continuous_local = local[local["local_minute"].map(_is_continuous)].copy()

    aligned = _compare(reference, continuous_local, -1)
    unshifted = _compare(reference, continuous_local, 0)
    aligned["close_abs_cny"] = (aligned["close_local"] - aligned["close_ak"]).abs()
    aligned.to_csv(args.output / "matched_minutes.csv", index=False)
    auction = _compare_closing_auction(reference, local)
    auction.to_csv(args.output / "closing_auction_compare.csv", index=False)

    details: list[dict[str, object]] = []
    for (trade_date, symbol), group in aligned.groupby(["trade_date", "symbol"]):
        row = {"date": str(trade_date), "symbol": str(symbol)}
        row.update(_summarize(group))
        details.append(row)
    by_date = {trade_date: _summarize(group) for trade_date, group in aligned.groupby("trade_date")}
    counts = (
        reference.assign(date=reference["source_time"].dt.strftime("%Y-%m-%d"))
        .groupby(["date", "symbol"])
        .size()
        .rename("rows")
        .reset_index()
        .to_dict(orient="records")
    )
    report = {
        "retrieved_at": downloaded_at,
        "compared_at": datetime.now(UTC).isoformat(),
        "akshare_version": ak.__version__,
        "source": "AkShare stock_zh_a_minute (Sina, unadjusted, 1 minute)",
        "eastmoney_pilot": "600519, 000001, 510300: ConnectionError/RemoteDisconnected on 2026-09-29",
        "requested_symbols": list(SAMPLE_SYMBOLS),
        "requested_dates": list(DATES),
        "successful_symbols": int(reference["symbol"].nunique()),
        "failed_symbols": failures,
        "request_seconds": timings,
        "downloaded_rows": len(reference),
        "downloaded_rows_by_date_symbol": counts,
        "local_rows_in_scope": len(continuous_local),
        "comparison_scope": "continuous auction-free minutes only; Sina timestamp shifted -1 minute",
        "alignment_check": {
            "unshifted": _summarize(unshifted),
            "sina_minus_one_minute": _summarize(aligned),
        },
        "by_date": by_date,
        "by_date_symbol": details,
        "closing_auction": {
            "matched_symbols": len(auction),
            "changed_final_close_symbols": int((auction["close_gap_cny"].abs() > 0.0005).sum()),
            "final_close_abs_cny": _stats(auction["close_gap_cny"].abs()),
            "local_volume_shares": int(auction["local_volume"].sum()),
            "sina_1500_volume_shares": int(auction["volume"].sum()),
            "volume_capture_ratio": round(
                float(auction["local_volume"].sum() / auction["volume"].sum()), 6
            ),
        },
    }
    (args.output / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "successful_symbols",
                    "failed_symbols",
                    "downloaded_rows",
                    "local_rows_in_scope",
                    "alignment_check",
                    "by_date",
                )
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
