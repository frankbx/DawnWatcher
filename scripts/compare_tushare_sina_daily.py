#!/usr/bin/env python3
"""Download Tushare stock/ETF daily bars and compare with cached Sina bars."""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import tushare as ts

_FIELDS = "ts_code,trade_date,open,high,low,close,vol,amount"
_PRICES = ("open", "high", "low", "close")


def compare(
    sina: pd.DataFrame, stocks: pd.DataFrame, funds: pd.DataFrame, trade_date: date
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Return per-symbol differences and a summary after normalizing Tushare units."""
    expected = trade_date.isoformat()
    if len(sina) != len(sina[["symbol", "trade_date"]].drop_duplicates()):
        raise ValueError("Sina cache has duplicate symbol/date keys")
    if set(sina["trade_date"]) != {expected}:
        raise ValueError("Sina cache trade date does not match requested date")

    collected = []
    upstream_counts = {}
    for frame, kind, name in ((stocks, "stock", "daily"), (funds, "etf", "fund_daily")):
        upstream_counts[name] = len(frame)
        required = {"ts_code", "trade_date", *_PRICES, "vol", "amount"}
        if not required.issubset(frame.columns):
            raise ValueError(f"{name} missing columns: {sorted(required - set(frame.columns))}")
        part = frame.copy()
        if not part.empty and set(part["trade_date"].astype(str)) != {
            trade_date.strftime("%Y%m%d")
        }:
            raise ValueError(f"{name} returned the wrong trade date")
        part["instrument_type"] = kind
        part = part.rename(columns={"ts_code": "symbol"})
        part = part[part["symbol"].isin(sina["symbol"])]
        collected.append(part)
    tushare = pd.concat(collected, ignore_index=True)
    if tushare.duplicated(["symbol", "instrument_type"]).any():
        raise ValueError("Tushare returned duplicate symbol/type keys")
    if (pd.to_numeric(tushare["vol"], errors="coerce") < 0).any() or (
        pd.to_numeric(tushare["amount"], errors="coerce") < 0
    ).any():
        raise ValueError("Tushare returned negative volume or amount")

    joined = sina.merge(
        tushare,
        on=["symbol", "instrument_type"],
        how="left",
        suffixes=("_sina", "_tushare"),
        indicator=True,
        validate="one_to_one",
    )
    rows = []
    for item in joined.to_dict("records"):
        matched = item["_merge"] == "both"
        output: dict[str, object] = {
            "trade_date": expected,
            "symbol": item["symbol"],
            "instrument_type": item["instrument_type"],
            "name": item["name"],
            "in_pool": bool(item["in_pool"]),
            "in_holdings": bool(item["in_holdings"]),
            "matched": matched,
        }
        if matched:
            for field in _PRICES:
                left = Decimal(str(item[f"{field}_sina"]))
                right = Decimal(str(item[f"{field}_tushare"]))
                output[f"{field}_sina"] = float(left)
                output[f"{field}_tushare"] = float(right)
                output[f"{field}_difference"] = float(left - right)
            sina_volume = Decimal(str(item["volume_shares"]))
            tushare_volume = Decimal(str(item["vol"])) * 100
            sina_amount = Decimal(str(item["amount_cny"]))
            tushare_amount = Decimal(str(item["amount"])) * 1000
            output.update(
                sina_volume_shares_or_units=float(sina_volume),
                tushare_volume_shares_or_units=float(tushare_volume),
                volume_difference=float(sina_volume - tushare_volume),
                sina_amount_cny=float(sina_amount),
                tushare_amount_cny=float(tushare_amount),
                amount_difference_cny=float(sina_amount - tushare_amount),
                amount_matches_round_half_up_to_yuan=(
                    sina_amount == tushare_amount.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
                ),
            )
        rows.append(output)
    detail = pd.DataFrame(rows)
    matched = detail.loc[detail["matched"]] if not detail.empty else detail
    field_summary = {}
    for field in (*_PRICES, "volume", "amount"):
        name = f"{field}_difference" + ("_cny" if field == "amount" else "")
        differences = matched[name].abs() if not matched.empty else pd.Series(dtype=float)
        field_summary[field] = {
            "nonzero_count": int((differences > 1e-8).sum()),
            "max_absolute_difference": float(differences.max()) if not differences.empty else None,
        }
    report: dict[str, object] = {
        "trade_date": expected,
        "source": "Tushare daily + fund_daily versus Sina via AKShare",
        "adjust": "none",
        "tushare_upstream_rows": upstream_counts,
        "requested_symbol_count": len(sina),
        "matched_symbol_count": len(matched),
        "missing_symbols": detail.loc[~detail["matched"], "symbol"].tolist(),
        "matched_by_type": matched["instrument_type"].value_counts().to_dict(),
        "unit_normalization": {
            "vol": "Tushare hands × 100 = shares or ETF units",
            "amount": "Tushare thousand yuan × 1000 = yuan",
        },
        "differences": field_summary,
        "amount_matches_round_half_up_to_yuan_count": int(
            matched["amount_matches_round_half_up_to_yuan"].sum()
        )
        if not matched.empty
        else 0,
    }
    return detail, report


def _atomic_parquet(frame: pd.DataFrame, target: Path) -> None:
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    frame.to_parquet(temporary, index=False, compression="zstd")
    os.replace(temporary, target)


def _atomic_json(payload: dict[str, object], target: Path) -> None:
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, target)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True, type=date.fromisoformat)
    parser.add_argument("--token-file", type=Path, default=Path("token"))
    parser.add_argument("--sina-root", type=Path, default=Path("data/lake/sina_daily"))
    parser.add_argument(
        "--output-root", type=Path, default=Path("data/lake/tushare_sina_daily_compare")
    )
    args = parser.parse_args()
    day = args.trade_date.isoformat()
    sina_path = args.sina_root / f"trade_date={day}" / "day.parquet"
    sina = pd.read_parquet(sina_path)
    token = args.token_file.read_text(encoding="utf-8").strip()
    if not token:
        parser.error("Tushare token file is empty")
    pro = ts.pro_api(token)
    trade_date = args.trade_date.strftime("%Y%m%d")
    stocks = pro.daily(trade_date=trade_date, fields=_FIELDS)
    funds = pro.fund_daily(trade_date=trade_date, fields=_FIELDS)
    detail, report = compare(sina, stocks, funds, args.trade_date)
    output = args.output_root / f"trade_date={day}"
    output.mkdir(parents=True, exist_ok=True)
    _atomic_parquet(detail, output / "comparison.parquet")
    report["generated_at"] = datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()
    report["comparison_path"] = str((output / "comparison.parquet").resolve())
    _atomic_json(report, output / "report.json")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if report["missing_symbols"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
