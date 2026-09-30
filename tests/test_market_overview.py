"""Sample breadth and sector-rotation overview tests."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session, sessionmaker

from regimebeacon.analysis.market_overview import (
    PoolMember,
    build_market_overview,
    format_market_overview_markdown,
)
from regimebeacon.domain import Exchange, QuoteProvider
from regimebeacon.notifications.feishu import build_market_analysis_card
from regimebeacon.storage.models import (
    MarketCollectionRun,
    MinuteBar,
    MinuteFeature,
    ProviderQuoteSnapshot,
)

_ZONE = ZoneInfo("Asia/Shanghai")
_DATE = date(2026, 9, 29)


def test_builds_explicit_fixed_sample_breadth_and_sector_confirmation(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    members = (
        PoolMember("600001.SH", "行业甲一", "stock", "fixed_representative", "行业甲", None),
        PoolMember("600002.SH", "行业甲二", "stock", "fixed_representative", "行业甲", None),
        PoolMember("600003.SH", "动态股", "stock", "dynamic_observer", "行业甲", None),
        PoolMember("510300.SH", "沪深300ETF", "etf", "broad_benchmark", None, "direct"),
        PoolMember("512001.SH", "行业甲ETF", "etf", "industry_benchmark", "行业甲", "direct"),
    )
    symbols = tuple(member.symbol for member in members)
    prices = {
        "600001.SH": ("100", "101", "103"),
        "600002.SH": ("100", "99", "98"),
        "600003.SH": ("100", "110", "120"),
        "510300.SH": ("100", "100", "100.5"),
        "512001.SH": ("100", "101", "102"),
    }
    with session_factory_fixture.begin() as session:
        for index, local_at in enumerate(
            (
                datetime(2026, 9, 29, 9, 30, tzinfo=_ZONE),
                datetime(2026, 9, 29, 9, 45, tzinfo=_ZONE),
                datetime(2026, 9, 29, 10, 0, tzinfo=_ZONE),
            )
        ):
            _seed_points(session, symbols, prices, index=index, local_at=local_at)
        _seed_relative_volume_features(session)

    with session_factory_fixture() as session:
        overview = build_market_overview(
            session,
            members=members,
            observed_at=datetime(2026, 9, 29, 10, 0, 10, tzinfo=_ZONE),
            timezone="Asia/Shanghai",
        )

    assert overview.fixed_sample_window_breadth.up == 1
    assert overview.fixed_sample_window_breadth.down == 1
    assert overview.fixed_sample_window_breadth.available == 2
    assert overview.fixed_sample_window_breadth.expected == 2
    assert overview.fixed_sample_coverage_pct == 100
    assert len(overview.sectors) == 1
    sector = overview.sectors[0]
    assert sector.industry == "行业甲"
    assert sector.etf_symbol == "512001.SH"
    assert sector.previous_sample_return_pct is not None
    assert overview.temperature.label == "偏暖"
    assert overview.temperature.score == 68.0
    assert overview.temperature.relative_volume_ratio == 1.6
    assert overview.temperature.opening_pattern == "平开走强"
    markdown = format_market_overview_markdown(overview)
    assert "市场温度" in markdown
    assert "风险动作" in markdown
    assert "固定样本当日广度" in markdown
    assert "不是全市场精确统计" in markdown
    card = build_market_analysis_card(
        markdown, direction=overview.temperature.label, report=overview.to_dict()
    )
    elements = card["body"]["elements"]
    tables = [element for element in elements if element["tag"] == "table"]
    assert len(tables) == 3
    assert "68.00/100" in elements[0]["content"]
    assert tables[0]["rows"] == [
        {"period": "当日", "up": "1", "flat": "0", "down": "1"},
        {"period": "近15分", "up": "1", "flat": "0", "down": "1"},
    ]
    assert tables[1]["rows"][0]["window"] == "**<font color='red'>+0.50%</font>**"
    assert tables[2]["rows"][0]["sample"] == "**<font color='red'>+0.49%</font>**"


def test_market_temperature_marks_broad_selloff_as_risk_contraction(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    members = (
        PoolMember("600001.SH", "下跌股一", "stock", "fixed_representative", "行业甲", None),
        PoolMember("600002.SH", "下跌股二", "stock", "fixed_representative", "行业乙", None),
        PoolMember("510300.SH", "沪深300ETF", "etf", "broad_benchmark", None, "direct"),
    )
    symbols = tuple(member.symbol for member in members)
    prices = {
        "600001.SH": ("100", "98", "96"),
        "600002.SH": ("100", "97", "94"),
        "510300.SH": ("100", "99", "98"),
    }
    with session_factory_fixture.begin() as session:
        for index, local_at in enumerate(
            (
                datetime(2026, 9, 29, 9, 30, tzinfo=_ZONE),
                datetime(2026, 9, 29, 9, 45, tzinfo=_ZONE),
                datetime(2026, 9, 29, 10, 0, tzinfo=_ZONE),
            )
        ):
            _seed_points(session, symbols, prices, index=index, local_at=local_at)

    with session_factory_fixture() as session:
        overview = build_market_overview(
            session,
            members=members,
            observed_at=datetime(2026, 9, 29, 10, 0, 10, tzinfo=_ZONE),
            timezone="Asia/Shanghai",
        )

    assert overview.temperature.label == "风险收缩"
    assert overview.temperature.score == 0.0
    assert overview.temperature.posture == "暂停新增风险"
    assert overview.temperature.opening_pattern == "平开走弱"


def _seed_points(
    session: Session,
    symbols: tuple[str, ...],
    prices: dict[str, tuple[str, str, str]],
    *,
    index: int,
    local_at: datetime,
) -> None:
    collection_id = str(uuid4())
    fetched_at = local_at.astimezone(UTC)
    session.add(
        MarketCollectionRun(
            id=collection_id,
            idempotency_key=f"overview-{collection_id}",
            expected_trade_date=_DATE,
            market_phase=None,
            requested_symbols=list(symbols),
            started_at=fetched_at,
            finished_at=fetched_at,
            provider_summaries={"tencent": {}},
            quality_counts={},
        )
    )
    for symbol in symbols:
        exchange = Exchange.SSE if symbol.endswith(".SH") else Exchange.SZSE
        price = Decimal(prices[symbol][index])
        session.add(
            ProviderQuoteSnapshot(
                collection_id=collection_id,
                provider=QuoteProvider.TENCENT,
                symbol=symbol,
                exchange=exchange,
                name=symbol,
                quote_at=fetched_at,
                fetched_at=fetched_at,
                open=Decimal("100"),
                previous_close=Decimal("100"),
                latest=price,
                high=price,
                low=price,
                volume_shares=1000 + index * 100,
                amount_cny=Decimal(100_000 + index * 10_000),
                bid1_price=price,
                bid1_volume_shares=100,
                ask1_price=price,
                ask1_volume_shares=100,
                volume_precision_shares=100,
                raw_field_count=88,
                validation_issues=[],
            )
        )


def _seed_relative_volume_features(session: Session) -> None:
    local_minute = datetime(2026, 9, 29, 9, 45, tzinfo=_ZONE)
    start = local_minute.astimezone(UTC)
    bars: list[tuple[MinuteBar, Decimal]] = []
    for symbol, ratio in (("600001.SH", Decimal("1.5")), ("600002.SH", Decimal("1.7"))):
        bar = MinuteBar(
            provider=QuoteProvider.TENCENT,
            symbol=symbol,
            exchange=Exchange.SSE,
            trade_date=_DATE,
            minute_start=start,
            minute_end=start + timedelta(minutes=1),
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100"),
            cumulative_volume_start=1_000,
            cumulative_volume_end=1_200,
            volume_shares=200,
            cumulative_amount_start=Decimal("100000"),
            cumulative_amount_end=Decimal("120000"),
            amount_cny=Decimal("20000"),
            vwap=Decimal("100"),
            sample_count=4,
            expected_sample_count=4,
            coverage_ratio=Decimal("1"),
            first_quote_at=start,
            last_quote_at=start + timedelta(seconds=45),
            quality_flags=[],
        )
        session.add(bar)
        bars.append((bar, ratio))
    session.flush()
    for bar, ratio in bars:
        session.add(
            MinuteFeature(
                minute_bar_id=bar.id,
                price_trend_bps=Decimal("0"),
                vwap_deviation_bps=Decimal("0"),
                relative_volume_ratio=ratio,
                relative_volume_history_days=20,
                market_benchmark_symbol="510300.SH",
                market_relative_strength_bps=Decimal("0"),
                industry_benchmark_symbol=None,
                industry_relative_strength_bps=None,
                quality_flags=[],
            )
        )
