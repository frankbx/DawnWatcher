"""Offline daily-lake contracts and DuckDB integration."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from regimebeacon.analysis.daily_query import query_daily_history, summarize_daily_pool
from regimebeacon.providers.tushare_daily import TushareDailyClient, TushareDailyError
from regimebeacon.storage.daily_lake import load_daily_members, sync_daily_date
from regimebeacon.storage.models import DailyLakePartition

TRADE_DATE = date(2026, 9, 30)
MEMBERS = {"002409.SZ": "stock", "512880.SH": "etf"}


def _price(symbol: str) -> dict[str, object]:
    return {
        "ts_code": symbol,
        "trade_date": "20260930",
        "open": 10.0,
        "high": 10.5,
        "low": 9.5,
        "close": 10.2,
        "pre_close": 10.0,
        "change": 0.2,
        "pct_chg": 2.0,
        "vol": 100.0,
        "amount": 102.0,
    }


class FakeSource:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.rows: dict[str, list[dict[str, object]]] = {
            "daily": [_price("002409.SZ")],
            "fund_daily": [_price("512880.SH")],
            "adj_factor": [{"ts_code": "002409.SZ", "trade_date": "20260930", "adj_factor": 4.0}],
            "fund_adj": [{"ts_code": "512880.SH", "trade_date": "20260930", "adj_factor": 1.0}],
        }

    def fetch(self, endpoint: str, trade_date: date) -> list[dict[str, Any]]:
        assert trade_date == TRADE_DATE
        self.calls.append(endpoint)
        return self.rows[endpoint]


def test_daily_lake_sync_query_and_idempotency(
    tmp_path: Path, session_factory_fixture: sessionmaker[Session]
) -> None:
    source = FakeSource()
    root = tmp_path / "lake" / "tushare_daily"
    first = sync_daily_date(
        source=source,
        session_factory=session_factory_fixture,
        lake_root=root,
        trade_date=TRADE_DATE,
        members=MEMBERS,
    )
    assert [item["status"] for item in first] == ["complete", "complete"]
    assert [item["row_count"] for item in first] == [2, 2]
    assert first[0]["path"] != first[1]["path"]
    assert source.calls == ["daily", "fund_daily", "adj_factor", "fund_adj"]

    again = sync_daily_date(
        source=source,
        session_factory=session_factory_fixture,
        lake_root=root,
        trade_date=TRADE_DATE,
        members=MEMBERS,
    )
    assert again == first
    assert len(source.calls) == 4
    with session_factory_fixture() as session:
        history = query_daily_history(
            session,
            symbol="002409.SZ",
            start_date=TRADE_DATE,
            end_date=TRADE_DATE,
            lake_root=root,
        )
        summary = summarize_daily_pool(session, trade_date=TRADE_DATE, lake_root=root)
    assert len(history) == 1
    assert history[0]["close"] == 10.2
    assert history[0]["adj_factor"] == 4.0
    assert history[0]["trade_date"] == "2026-09-30"
    assert {group["instrument_type"] for group in summary["groups"]} == {"stock", "etf"}
    assert all(group["turnover_cny"] == 102000.0 for group in summary["groups"])


def test_missing_factor_is_partial_and_not_joined(
    tmp_path: Path, session_factory_fixture: sessionmaker[Session]
) -> None:
    source = FakeSource()
    source.rows["fund_adj"] = []
    root = tmp_path / "lake" / "tushare_daily"
    report = sync_daily_date(
        source=source,
        session_factory=session_factory_fixture,
        lake_root=root,
        trade_date=TRADE_DATE,
        members=MEMBERS,
    )
    assert report[0]["status"] == "complete"
    assert report[1]["status"] == "partial"
    assert report[1]["missing_symbols"] == ["512880.SH"]
    with session_factory_fixture() as session:
        rows = query_daily_history(
            session,
            symbol="002409.SZ",
            start_date=TRADE_DATE,
            end_date=TRADE_DATE,
            lake_root=root,
        )
    assert rows[0]["adj_factor"] is None


def test_failed_refresh_keeps_previous_complete_snapshot(
    tmp_path: Path, session_factory_fixture: sessionmaker[Session]
) -> None:
    source = FakeSource()
    root = tmp_path / "lake" / "tushare_daily"
    original = sync_daily_date(
        source=source,
        session_factory=session_factory_fixture,
        lake_root=root,
        trade_date=TRADE_DATE,
        members=MEMBERS,
    )
    source.rows["fund_adj"] *= 2
    with pytest.raises(ValueError, match="duplicate"):
        sync_daily_date(
            source=source,
            session_factory=session_factory_fixture,
            lake_root=root,
            trade_date=TRADE_DATE,
            members=MEMBERS,
            refresh=True,
        )
    with session_factory_fixture() as session:
        active = session.scalars(select(DailyLakePartition)).all()
    assert {row.status for row in active} == {"complete"}
    assert {row.parquet_path for row in active} == {item["path"] for item in original}


def test_new_source_failure_is_recorded_without_exposing_partial_price(
    tmp_path: Path, session_factory_fixture: sessionmaker[Session]
) -> None:
    class BrokenFactorSource(FakeSource):
        def fetch(self, endpoint: str, trade_date: date) -> list[dict[str, Any]]:
            if endpoint == "fund_adj":
                raise TushareDailyError("fund_adj unavailable")
            return super().fetch(endpoint, trade_date)

    root = tmp_path / "lake" / "tushare_daily"
    with pytest.raises(TushareDailyError, match="fund_adj unavailable"):
        sync_daily_date(
            source=BrokenFactorSource(),
            session_factory=session_factory_fixture,
            lake_root=root,
            trade_date=TRADE_DATE,
            members=MEMBERS,
        )
    with session_factory_fixture() as session:
        rows = session.scalars(select(DailyLakePartition)).all()
        history = query_daily_history(
            session,
            symbol="002409.SZ",
            start_date=TRADE_DATE,
            end_date=TRADE_DATE,
            lake_root=root,
        )
    assert len(rows) == 1
    assert rows[0].dataset == "factor"
    assert rows[0].status == "failed"
    assert rows[0].parquet_path is None
    assert history == []


def test_corrupt_active_parquet_is_rejected(
    tmp_path: Path, session_factory_fixture: sessionmaker[Session]
) -> None:
    root = tmp_path / "lake" / "tushare_daily"
    report = sync_daily_date(
        source=FakeSource(),
        session_factory=session_factory_fixture,
        lake_root=root,
        trade_date=TRADE_DATE,
        members=MEMBERS,
    )
    path = Path(str(report[0]["path"]))
    path.write_bytes(b"corrupt")
    with session_factory_fixture() as session, pytest.raises(RuntimeError, match="checksum"):
        summarize_daily_pool(session, trade_date=TRADE_DATE, lake_root=root)


def test_member_fingerprint_invalidates_same_size_pool(
    tmp_path: Path, session_factory_fixture: sessionmaker[Session]
) -> None:
    source = FakeSource()
    root = tmp_path / "lake" / "tushare_daily"
    sync_daily_date(
        source=source,
        session_factory=session_factory_fixture,
        lake_root=root,
        trade_date=TRADE_DATE,
        members=MEMBERS,
    )
    different = {"002409.SZ": "stock", "510300.SH": "etf"}
    report = sync_daily_date(
        source=source,
        session_factory=session_factory_fixture,
        lake_root=root,
        trade_date=TRADE_DATE,
        members=different,
    )
    assert len(source.calls) == 8
    assert report[0]["status"] == "partial"
    assert report[0]["missing_symbols"] == ["510300.SH"]


def test_member_loader_includes_private_holdings_without_costs(tmp_path: Path) -> None:
    pool, holdings = tmp_path / "pool.json", tmp_path / "holdings.json"
    pool.write_text(
        json.dumps({"members": [{"ts_code": "512880.SH", "instrument_type": "etf"}]}),
        encoding="utf-8",
    )
    holdings.write_text(
        json.dumps({"positions": [{"symbol": "002409.SZ", "cost": 143.905}]}),
        encoding="utf-8",
    )
    assert load_daily_members(pool, holdings) == MEMBERS


def test_tushare_daily_client_decodes_named_rows() -> None:
    def responder(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        assert payload["api_name"] == "adj_factor"
        assert payload["params"] == {"trade_date": "20260930"}
        return httpx.Response(
            200,
            json={
                "code": 0,
                "data": {
                    "fields": ["adj_factor", "trade_date", "ts_code"],
                    "items": [[4.0, "20260930", "002409.SZ"]],
                },
            },
        )

    with httpx.Client(transport=httpx.MockTransport(responder)) as http:
        client = TushareDailyClient(
            token="private-token", api_url="https://example.invalid", client=http
        )
        assert client.fetch("adj_factor", TRADE_DATE)[0]["ts_code"] == "002409.SZ"


def test_tushare_daily_client_permission_failure_does_not_leak_token() -> None:
    with httpx.Client(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json={"code": -2001, "msg": "permission denied"})
        )
    ) as http:
        client = TushareDailyClient(
            token="private-token", api_url="https://example.invalid", client=http
        )
        with pytest.raises(TushareDailyError, match="permission denied") as error:
            client.fetch("fund_adj", TRADE_DATE)
    assert "private-token" not in str(error.value)
