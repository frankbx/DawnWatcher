"""Private holdings validation, risk review, and scheduled card coverage."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from scripts.watch_holdings import _next_due, _reportable_slot, enqueue_once
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from regimebeacon.config import Settings
from regimebeacon.domain import Exchange, QuoteProvider
from regimebeacon.notifications.feishu import build_alert_card
from regimebeacon.portfolio.holdings import (
    Holding,
    _guidance,
    build_holdings_report,
    format_holdings_report,
    guidance_thresholds,
    load_holdings,
)
from regimebeacon.portfolio.minute_history import load_history_reference
from regimebeacon.storage.models import (
    MarketCollectionRun,
    NotificationOutbox,
    ProviderQuoteSnapshot,
)

_ZONE = ZoneInfo("Asia/Shanghai")


def test_loads_and_rejects_invalid_holdings(tmp_path: Path) -> None:
    path = tmp_path / "holdings.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "positions": [
                    {
                        "symbol": "600001.SH",
                        "name": "测试股",
                        "cost_cny": "140.005",
                        "shares": 200,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    holding = load_holdings(path)[0]
    assert holding.symbol == "600001.SH"
    assert holding.cost_cny == Decimal("140.005")
    assert holding.shares == 200

    document = json.loads(path.read_text(encoding="utf-8"))
    document["positions"].append(dict(document["positions"][0]))
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate"):
        load_holdings(path)


def test_guidance_price_thresholds_follow_report_window() -> None:
    five = guidance_thresholds(5)
    fifteen = guidance_thresholds(15)
    assert (five.moderate_move_pct, five.large_move_pct) == (
        Decimal("0.3"),
        Decimal("0.6"),
    )
    assert (fifteen.moderate_move_pct, fifteen.large_move_pct) == (
        Decimal("0.5"),
        Decimal("1.0"),
    )
    assert five.cost_loss_pct == fifteen.cost_loss_pct == Decimal("5")
    assert five.volume_ratio == fifteen.volume_ratio == Decimal("1.5")
    assert _guidance(Decimal("-6"), Decimal("-0.7"), None, None, thresholds=five)[0] == "风险升高"
    assert (
        _guidance(Decimal("-6"), Decimal("-0.7"), None, None, thresholds=fifteen)[0] == "震荡观察"
    )
    assert (
        _guidance(Decimal("0"), Decimal("-0.35"), None, Decimal("1.6"), thresholds=five)[0]
        == "放量走弱"
    )
    assert (
        _guidance(Decimal("0"), Decimal("-0.35"), None, Decimal("1.6"), thresholds=fifteen)[0]
        == "震荡观察"
    )


def test_holdings_report_calculates_cost_return_and_15m_risk(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    holdings = (Holding("600001.SH", "测试股", Decimal("140.005"), 200),)
    with session_factory_fixture.begin() as session:
        _seed(session, "600001.SH", "140", datetime(2026, 9, 29, 9, 45, tzinfo=_ZONE))
        _seed(session, "600001.SH", "137", datetime(2026, 9, 29, 10, 0, tzinfo=_ZONE))
        _seed(session, "510300.SH", "4.00", datetime(2026, 9, 29, 9, 45, tzinfo=_ZONE))
        _seed(session, "510300.SH", "4.01", datetime(2026, 9, 29, 10, 0, tzinfo=_ZONE))
    with session_factory_fixture() as session:
        report = build_holdings_report(
            session,
            holdings=holdings,
            observed_at=datetime(2026, 9, 29, 10, 0, 40, tzinfo=_ZONE),
            timezone="Asia/Shanghai",
        )
    value = report.positions[0]
    assert value.latest_cny == Decimal("137")
    assert value.pnl_cny == (Decimal("137") - Decimal("140.005")) * 200
    assert value.window_pct is not None and value.window_pct < Decimal("-2")
    assert value.relative_window_pct is not None and value.relative_window_pct < Decimal("-2")
    assert value.state == "短线偏弱"
    assert report.data_complete
    assert "测试股" in format_holdings_report(report)
    assert "**操作建议**" in format_holdings_report(report)
    assert "不自动下单" in format_holdings_report(report)


def test_stale_quote_suppresses_valuation_and_guidance(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    holdings = (Holding("600001.SH", "测试股", Decimal("140.005"), 200),)
    with session_factory_fixture.begin() as session:
        _seed(session, "600001.SH", "137", datetime(2026, 9, 29, 9, 45, tzinfo=_ZONE))
    with session_factory_fixture() as session:
        report = build_holdings_report(
            session,
            holdings=holdings,
            observed_at=datetime(2026, 9, 29, 10, 0, 40, tzinfo=_ZONE),
            timezone="Asia/Shanghai",
        )
    assert not report.data_complete
    assert report.total_value_cny is None
    assert report.positions[0].state == "行情缺失或过期"
    assert report.positions[0].latest_cny is None
    assert "**操作建议**：暂停走势判断" in format_holdings_report(report)


def test_holdings_card_highlights_price_relative_to_cost(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    holdings = (
        Holding("600001.SH", "低于成本", Decimal("100"), 100),
        Holding("600002.SH", "高于成本", Decimal("100"), 100),
        Holding("600003.SH", "等于成本", Decimal("100"), 100),
        Holding("600004.SH", "行情缺失", Decimal("100"), 100),
    )
    quote_at = datetime(2026, 9, 29, 10, 0, tzinfo=_ZONE)
    with session_factory_fixture.begin() as session:
        _seed(session, "600001.SH", "99", quote_at)
        _seed(session, "600002.SH", "101", quote_at)
        _seed(session, "600003.SH", "100", quote_at)
    with session_factory_fixture() as session:
        report = build_holdings_report(
            session,
            holdings=holdings,
            observed_at=quote_at + timedelta(seconds=40),
            timezone="Asia/Shanghai",
            window_minutes=5,
        )

    markdown = format_holdings_report(report)
    assert "**现价颜色**：高于成本为红色，低于成本为绿色" in markdown
    assert "**现价**：**<font color='green'>¥99.00</font>**" in markdown
    assert "**现价**：**<font color='red'>¥101.00</font>**" in markdown
    assert "**现价**：**¥100.00**" in markdown
    assert "**行情缺失 600004.SH**" in markdown
    assert "**现价**：无有效报价" in markdown
    assert markdown.count("**现价**：") == 4
    assert "有效行情 3/4" in markdown
    assert "成本 ¥100.00" in markdown
    notification = NotificationOutbox(
        id="holdings-mixed",
        idempotency_key="holdings-mixed",
        event_type="portfolio.holdings_review.completed",
        channel="feishu",
        recipient="portfolio_holdings",
        payload={
            "data_complete": report.data_complete,
            "markdown": markdown,
            "report": report.to_dict(),
        },
    )
    card = build_alert_card(notification)
    assert card["header"]["template"] == "orange"
    snapshot = card["body"]["elements"][2]
    assert [column["display_name"] for column in snapshot["columns"]] == [
        "标的",
        "现价",
        "当日",
        "近5分",
        "较成本",
    ]
    rows = snapshot["rows"]
    assert rows[0]["daily"] == "**<font color='green'>-1.00%</font>**"
    assert rows[1]["daily"] == "**<font color='red'>+1.00%</font>**"
    assert rows[2]["daily"] == "**+0.00%**"
    assert rows[-1]["name"] == "行情缺失"
    assert rows[-1]["price"] == "—"
    assert rows[-1]["daily"] == "—"
    assert rows[-1]["pnl"] == "—"
    assert "暂停价格与盈亏判断" in markdown


def test_first_opening_report_does_not_compare_with_auction(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    holdings = (Holding("600001.SH", "测试股", Decimal("140.005"), 200),)
    with session_factory_fixture.begin() as session:
        _seed(session, "600001.SH", "140", datetime(2026, 9, 29, 9, 15, tzinfo=_ZONE))
        _seed(session, "600001.SH", "141", datetime(2026, 9, 29, 9, 30, tzinfo=_ZONE))
    with session_factory_fixture() as session:
        report = build_holdings_report(
            session,
            holdings=holdings,
            observed_at=datetime(2026, 9, 29, 9, 30, 40, tzinfo=_ZONE),
            timezone="Asia/Shanghai",
        )
    assert report.positions[0].window_pct is None
    assert report.positions[0].state == "窗口数据不足"


def test_historical_same_clock_comparison_changes_volume_risk_prompt(
    session_factory_fixture: sessionmaker[Session], tmp_path: Path
) -> None:
    _write_history(tmp_path)
    slot = datetime(2026, 9, 30, 10, 0, tzinfo=_ZONE)
    history = load_history_reference(
        tmp_path,
        symbols=("600001.SH",),
        window_end=slot,
    )
    assert history.status == "可比"
    assert history.windows["600001.SH"].days == 5
    holdings = (Holding("600001.SH", "测试股", Decimal("100"), 200),)
    with session_factory_fixture.begin() as session:
        _seed(session, "600001.SH", "100", slot - timedelta(minutes=15), volume=1000)
        _seed(session, "600001.SH", "99", slot, volume=4000)
    with session_factory_fixture() as session:
        report = build_holdings_report(
            session,
            holdings=holdings,
            observed_at=slot + timedelta(seconds=40),
            timezone="Asia/Shanghai",
            history=history,
            window_end=slot,
        )
    value = report.positions[0]
    assert value.historical_median_pct == Decimal("1")
    assert value.historical_excess_pct == Decimal("-2")
    assert value.historical_volume_ratio == Decimal("2")
    assert value.state == "放量走弱"
    assert "历史同窗5日中位" in format_holdings_report(report)
    assert "成交量比 2.00 倍" in format_holdings_report(report)


def test_history_never_uses_today_or_future_days(tmp_path: Path) -> None:
    _write_history(tmp_path)
    reference = load_history_reference(
        tmp_path,
        symbols=("600001.SH",),
        window_end=datetime(2026, 9, 29, 10, 0, tzinfo=_ZONE),
    )
    assert reference.status == "部分标的历史样本不足"
    assert not reference.windows


def test_new_holding_does_not_disable_history_for_existing_symbols(tmp_path: Path) -> None:
    _write_history(tmp_path)
    reference = load_history_reference(
        tmp_path,
        symbols=("600001.SH", "600002.SH"),
        window_end=datetime(2026, 9, 30, 10, 0, tzinfo=_ZONE),
    )
    assert reference.status == "部分标的历史样本不足"
    assert set(reference.windows) == {"600001.SH"}


def test_old_history_and_auction_window_fail_closed(tmp_path: Path) -> None:
    _write_history(tmp_path)
    stale = load_history_reference(
        tmp_path,
        symbols=("600001.SH",),
        window_end=datetime(2026, 10, 22, 10, 0, tzinfo=_ZONE),
    )
    assert stale.status == "历史样本已过期"
    assert not stale.windows
    opening = load_history_reference(
        tmp_path,
        symbols=("600001.SH",),
        window_end=datetime(2026, 9, 30, 9, 30, tzinfo=_ZONE),
    )
    assert opening.status == "非连续交易窗口"


def test_unconfirmed_auction_close_suppresses_final_advice(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    holdings = (Holding("600001.SH", "测试股", Decimal("100"), 200),)
    slot = datetime(2026, 9, 29, 15, 0, tzinfo=_ZONE)
    with session_factory_fixture.begin() as session:
        _seed(
            session,
            "600001.SH",
            "101",
            slot + timedelta(seconds=15),
            quote_at=slot - timedelta(seconds=18),
        )
    with session_factory_fixture() as session:
        report = build_holdings_report(
            session,
            holdings=holdings,
            observed_at=slot + timedelta(seconds=40),
            timezone="Asia/Shanghai",
            window_end=slot,
        )
    assert not report.data_complete
    assert report.positions[0].state == "收盘价未确认"
    assert report.positions[0].latest_cny is None
    assert "收盘价未确认" in format_holdings_report(report)


def test_holdings_card_and_schedule_are_separate() -> None:
    notification = NotificationOutbox(
        id="holdings-1",
        idempotency_key="holdings-1",
        event_type="portfolio.holdings_review.completed",
        channel="feishu",
        recipient="portfolio_holdings",
        payload={"data_complete": True, "markdown": "**测试股**：观察"},
    )
    card = build_alert_card(notification)
    assert card["schema"] == "2.0"
    assert card["header"]["title"]["content"] == "RegimeBeacon 持仓观察"
    assert "测试股" in card["body"]["elements"][0]["content"]

    notification.payload["report"] = {"window_minutes": 5}
    assert build_alert_card(notification)["header"]["title"]["content"] == (
        "RegimeBeacon 持仓5分钟观察"
    )

    assert _next_due(datetime(2026, 9, 29, 9, 30, 1, tzinfo=_ZONE), 5, 40) == datetime(
        2026, 9, 29, 9, 30, 40, tzinfo=_ZONE
    )
    assert _next_due(datetime(2026, 9, 29, 9, 30, 41, tzinfo=_ZONE), 5, 40) == datetime(
        2026, 9, 29, 9, 35, 40, tzinfo=_ZONE
    )
    assert not _reportable_slot(datetime(2026, 9, 29, 9, 30, tzinfo=_ZONE), 5)
    assert _reportable_slot(datetime(2026, 9, 29, 9, 35, tzinfo=_ZONE), 5)
    assert _reportable_slot(datetime(2026, 9, 29, 11, 30, tzinfo=_ZONE), 5)
    assert not _reportable_slot(datetime(2026, 9, 29, 11, 35, tzinfo=_ZONE), 5)
    assert not _reportable_slot(datetime(2026, 9, 29, 13, 0, tzinfo=_ZONE), 5)
    assert _reportable_slot(datetime(2026, 9, 29, 13, 5, tzinfo=_ZONE), 5)
    assert _reportable_slot(datetime(2026, 9, 29, 15, 0, tzinfo=_ZONE), 5)


def test_holdings_notification_is_idempotent_across_restarts(
    database_settings: Settings,
    session_factory_fixture: sessionmaker[Session],
    tmp_path: Path,
) -> None:
    path = tmp_path / "holdings.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "positions": [
                    {
                        "symbol": "600001.SH",
                        "name": "测试股",
                        "cost_cny": "140.005",
                        "shares": 200,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    with session_factory_fixture.begin() as session:
        _seed(session, "600001.SH", "140", datetime(2026, 9, 29, 9, 55, tzinfo=_ZONE))
        _seed(session, "600001.SH", "141", datetime(2026, 9, 29, 10, 0, tzinfo=_ZONE))
    slot = datetime(2026, 9, 29, 10, 0, tzinfo=_ZONE)
    ids = [
        enqueue_once(
            database_settings,
            holdings_file=path,
            observed_at=slot + timedelta(seconds=40),
            slot=slot,
            interval_minutes=5,
            benchmark="510300.SH",
        )
        for _ in range(2)
    ]
    assert ids[0] == ids[1]
    with session_factory_fixture() as session:
        assert session.scalar(select(func.count()).select_from(NotificationOutbox)) == 1
        notification = session.scalar(select(NotificationOutbox))
        assert notification is not None
        assert notification.event_type == "portfolio.holdings_review.completed"
        assert notification.payload["expires_at"] == (slot + timedelta(minutes=5)).isoformat()
        assert notification.payload["report"]["window_minutes"] == 5
        assert notification.payload["report"]["guidance_thresholds"]["large_move_pct"] == "0.6"
        card = build_alert_card(notification)
        assert card["schema"] == "2.0"
        assert card["header"]["title"]["content"] == "RegimeBeacon 持仓5分钟观察"
        elements = card["body"]["elements"]
        assert [element["tag"] for element in elements] == [
            "markdown",
            "markdown",
            "table",
            "markdown",
            "table",
            "markdown",
        ]
        assert "**操作提示**" in elements[-1]["content"]
        assert elements[2]["rows"][0]["price"] == "**<font color='red'>¥141.00</font>**"
        assert elements[2]["rows"][0]["pnl"] == "**<font color='red'>+0.71%</font>**"
        assert elements[4]["rows"][0]["historical"] == "—"


def _write_history(root: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    parquet = pytest.importorskip("pyarrow.parquet")
    directory = root / "holdings_as_of=2026-09-29"
    days = [date(2026, 9, day) for day in (23, 24, 25, 28, 29)]
    for trade_date in days:
        partition = directory / f"trade_date={trade_date.isoformat()}"
        partition.mkdir(parents=True)
        start = datetime.combine(trade_date, datetime.min.time(), _ZONE).replace(hour=9, minute=45)
        rows = [
            {
                "symbol": "600001.SH",
                "minute_start": start + timedelta(minutes=index),
                "minute_end": start + timedelta(minutes=index + 1),
                "open": 100.0,
                "close": 101.0 if index == 14 else 100.0,
                "volume_shares": 100,
            }
            for index in range(15)
        ]
        parquet.write_table(pa.Table.from_pylist(rows), partition / "part-000.parquet")
    (directory / "manifest.json").write_text(
        json.dumps(
            {
                "timezone": "Asia/Shanghai",
                "requested_symbols": ["600001.SH"],
                "complete_dates_for_all_requested_symbols": [day.isoformat() for day in days],
            }
        ),
        encoding="utf-8",
    )


def _seed(
    session: Session,
    symbol: str,
    price: str,
    local_at: datetime,
    *,
    volume: int = 1000,
    quote_at: datetime | None = None,
) -> None:
    collected = local_at.astimezone(UTC)
    collection_id = str(uuid4())
    value = Decimal(price)
    session.add(
        MarketCollectionRun(
            id=collection_id,
            idempotency_key=collection_id,
            expected_trade_date=date(2026, 9, 29),
            requested_symbols=[symbol],
            started_at=collected,
            finished_at=collected + timedelta(seconds=1),
            provider_summaries={"tencent": {}},
            quality_counts={},
        )
    )
    session.add(
        ProviderQuoteSnapshot(
            collection_id=collection_id,
            provider=QuoteProvider.TENCENT,
            symbol=symbol,
            exchange=Exchange.SSE if symbol.endswith(".SH") else Exchange.SZSE,
            name=symbol,
            quote_at=quote_at.astimezone(UTC) if quote_at is not None else collected,
            fetched_at=collected,
            open=value,
            previous_close=Decimal("100"),
            latest=value,
            high=value,
            low=value,
            volume_shares=volume,
            amount_cny=value * volume,
            bid1_price=value,
            bid1_volume_shares=100,
            ask1_price=value,
            ask1_volume_shares=100,
            volume_precision_shares=100,
            raw_field_count=88,
            validation_issues=[],
        )
    )
