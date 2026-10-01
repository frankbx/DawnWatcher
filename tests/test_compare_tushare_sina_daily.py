"""Unit and matching checks for the Tushare/Sina daily comparison."""

from __future__ import annotations

import importlib.util
from datetime import date
from pathlib import Path

import pandas as pd
import pytest


def _script():
    path = Path(__file__).resolve().parents[1] / "scripts" / "compare_tushare_sina_daily.py"
    spec = importlib.util.spec_from_file_location("compare_tushare_sina_daily", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_unit_normalization_and_rounding() -> None:
    script = _script()
    day = date(2026, 9, 30)
    sina = pd.DataFrame(
        [
            {
                "trade_date": day.isoformat(),
                "symbol": "002409.SZ",
                "instrument_type": "stock",
                "name": "雅克科技",
                "in_pool": False,
                "in_holdings": True,
                "open": 125.47,
                "high": 125.8,
                "low": 120.51,
                "close": 121.0,
                "volume_shares": 9151351,
                "amount_cny": 1117917647,
            }
        ]
    )
    stocks = pd.DataFrame(
        [
            {
                "ts_code": "002409.SZ",
                "trade_date": "20260930",
                "open": 125.47,
                "high": 125.8,
                "low": 120.51,
                "close": 121.0,
                "vol": 91513.51,
                "amount": 1117917.64709,
            }
        ]
    )
    funds = pd.DataFrame(columns=stocks.columns)
    detail, report = script.compare(sina, stocks, funds, day)
    assert report["matched_symbol_count"] == 1
    assert report["differences"]["volume"]["nonzero_count"] == 0
    assert report["differences"]["amount"]["max_absolute_difference"] == 0.09
    assert report["amount_matches_round_half_up_to_yuan_count"] == 1
    assert detail.loc[0, "close_difference"] == 0


def test_wrong_upstream_day_fails_closed() -> None:
    script = _script()
    sina = pd.DataFrame(
        [{"trade_date": "2026-09-30", "symbol": "002409.SZ", "instrument_type": "stock"}]
    )
    upstream = pd.DataFrame(
        [
            {
                "ts_code": "002409.SZ",
                "trade_date": "20260929",
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "vol": 1,
                "amount": 1,
            }
        ]
    )
    with pytest.raises(ValueError, match="wrong trade date"):
        script.compare(sina, upstream, upstream.iloc[0:0], date(2026, 9, 30))
