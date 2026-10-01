"""Offline contract checks for the paced Sina daily cache downloader."""

from __future__ import annotations

import importlib.util
import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest


def _script():
    path = Path(__file__).resolve().parents[1] / "scripts" / "download_sina_daily.py"
    spec = importlib.util.spec_from_file_location("download_sina_daily", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_load_members_deduplicates_holdings_overlap(tmp_path: Path) -> None:
    script = _script()
    pool = tmp_path / "pool.json"
    holdings = tmp_path / "holdings.json"
    pool.write_text(
        json.dumps(
            {
                "members": [
                    {"ts_code": "002409.SZ", "name": "雅克科技", "instrument_type": "stock"},
                    {"ts_code": "510300.SH", "name": "沪深300ETF", "instrument_type": "etf"},
                ]
            }
        ),
        encoding="utf-8",
    )
    holdings.write_text(
        json.dumps(
            {
                "positions": [
                    {"symbol": "002409.SZ", "name": "雅克科技"},
                    {"symbol": "603993.SH", "name": "洛阳钼业"},
                ]
            }
        ),
        encoding="utf-8",
    )
    members = script.load_members(pool, holdings)
    assert len(members) == 3
    assert next(x for x in members if x["symbol"] == "002409.SZ")["in_holdings"]
    assert not next(x for x in members if x["symbol"] == "603993.SH")["in_pool"]


def test_download_one_rejects_missing_day_and_invalid_ohlc() -> None:
    script = _script()

    class FakeAk:
        def stock_zh_a_daily(self, **_kwargs):
            return pd.DataFrame(
                [
                    {
                        "date": date(2026, 9, 30),
                        "open": 10.0,
                        "high": 9.0,
                        "low": 8.0,
                        "close": 10.0,
                        "volume": 100,
                        "amount": 1000,
                    }
                ]
            )

    member = {
        "symbol": "002409.SZ",
        "name": "雅克科技",
        "instrument_type": "stock",
        "in_pool": True,
        "in_holdings": True,
    }
    with pytest.raises(ValueError, match="invalid OHLC"):
        script.download_one(FakeAk(), member, date(2026, 9, 30))
    with pytest.raises(ValueError, match="expected one bar"):
        script.download_one(FakeAk(), member, date(2026, 9, 29))
