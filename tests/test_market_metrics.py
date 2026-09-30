"""Persisted Tencent reliability metric tests."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from scripts import send_market_status_report
from sqlalchemy.orm import Session, sessionmaker

from regimebeacon.config import Settings
from regimebeacon.domain import DataQualityState, Exchange, QuoteProvider
from regimebeacon.market import MarketPhase
from regimebeacon.storage.market_metrics import build_market_metrics_report
from regimebeacon.storage.models import MarketCollectionRun, ReconciledQuoteSnapshot


def test_market_metrics_aggregate_single_source_latency_and_gaps(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    started = datetime(2026, 9, 28, 2, 0, tzinfo=UTC)
    with session_factory_fixture.begin() as session:
        first = _collection("first", started, valid=2, latency=120)
        second = _collection("second", started + timedelta(seconds=15), valid=2, latency=130)
        third = _collection("third", started + timedelta(seconds=60), valid=1, latency=110)
        session.add_all((first, second, third))
        session.flush()
        session.add_all(
            (
                _reconciled(first.id, "600000.SH", DataQualityState.COMPLETE),
                _reconciled(first.id, "000001.SZ", DataQualityState.COMPLETE),
                _reconciled(second.id, "600000.SH", DataQualityState.COMPLETE),
                _reconciled(second.id, "000001.SZ", DataQualityState.COMPLETE),
                _reconciled(third.id, "600000.SH", DataQualityState.COMPLETE),
                _reconciled(third.id, "000001.SZ", DataQualityState.BLOCKED),
            )
        )

    with session_factory_fixture() as session:
        report = build_market_metrics_report(
            session,
            start_at=started,
            end_at=started + timedelta(days=1),
            expected_interval_seconds=15,
        )

    assert report["provider"] == "tencent"
    assert report["single_source_mode"] is True
    assert report["collection_count"] == 3
    assert report["metrics"]["successful_run_rate_pct"] == 66.667
    assert report["metrics"]["valid_quote_rate_pct"] == 83.333
    assert report["metrics"]["latency_ms"]["average"] == 120.0
    assert report["collection_gaps"]["count"] == 1
    assert report["collection_gaps"]["max_seconds"] == 45.0
    assert report["quality_counts"]["blocked"] == 1


def test_market_metrics_excludes_legacy_dual_rows(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    started = datetime(2026, 9, 28, 2, 0, tzinfo=UTC)
    with session_factory_fixture.begin() as session:
        session.add(_collection("single", started, valid=1, latency=100))
        session.add(
            MarketCollectionRun(
                id="legacy-collection",
                idempotency_key="legacy-collection",
                expected_trade_date=date(2026, 9, 28),
                market_phase=MarketPhase.MORNING_CONTINUOUS,
                requested_symbols=["600000.SH"],
                started_at=started + timedelta(seconds=15),
                finished_at=started + timedelta(seconds=15, milliseconds=100),
                provider_summaries={"sina": {"valid_quote_count": 1}, "tencent": {}},
                quality_counts={"conflicted": 1},
            )
        )

    with session_factory_fixture() as session:
        report = build_market_metrics_report(
            session,
            start_at=started,
            end_at=started + timedelta(days=1),
            expected_interval_seconds=15,
        )

    assert report["collection_count"] == 1
    assert report["total_collection_count"] == 2
    assert report["legacy_dual_collection_count"] == 1
    assert report["quality_counts"]["conflicted"] == 0


def test_market_metrics_excludes_both_call_auctions(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    started = datetime(2026, 9, 28, 1, 15, tzinfo=UTC)
    with session_factory_fixture.begin() as session:
        opening = _collection("opening", started, valid=0, latency=100)
        opening.market_phase = MarketPhase.OPENING_CALL_AUCTION
        regular = _collection("regular", started + timedelta(minutes=15), valid=2, latency=120)
        closing = _collection(
            "closing", started + timedelta(hours=5, minutes=42), valid=2, latency=130
        )
        closing.market_phase = MarketPhase.CLOSING_CALL_AUCTION
        session.add_all((opening, regular, closing))
        session.flush()
        session.add_all(
            (
                _reconciled(opening.id, "600000.SH", DataQualityState.BLOCKED),
                _reconciled(opening.id, "000001.SZ", DataQualityState.BLOCKED),
                _reconciled(regular.id, "600000.SH", DataQualityState.COMPLETE),
                _reconciled(regular.id, "000001.SZ", DataQualityState.COMPLETE),
                _reconciled(closing.id, "600000.SH", DataQualityState.COMPLETE),
                _reconciled(closing.id, "000001.SZ", DataQualityState.COMPLETE),
            )
        )

    with session_factory_fixture() as session:
        report = build_market_metrics_report(
            session,
            start_at=started,
            end_at=started + timedelta(days=1),
            expected_interval_seconds=15,
        )

    assert report["collection_count"] == 1
    assert report["excluded_non_continuous_collection_count"] == 2
    assert report["metrics"]["valid_quote_rate_pct"] == 100.0
    assert report["quality_counts"]["blocked"] == 0


def test_status_card_health_uses_continuous_metrics_only(
    database_settings: Settings,
    session_factory_fixture: sessionmaker[Session],
    monkeypatch: object,
) -> None:
    opening_at = datetime(2026, 9, 28, 1, 15, tzinfo=UTC)
    with session_factory_fixture.begin() as session:
        opening = _collection("status-opening", opening_at, valid=0, latency=100)
        opening.market_phase = MarketPhase.OPENING_CALL_AUCTION
        regular = _collection(
            "status-regular", opening_at + timedelta(minutes=15), valid=2, latency=120
        )
        session.add_all((opening, regular))
        session.flush()
        session.add_all(
            (
                _reconciled(opening.id, "600000.SH", DataQualityState.BLOCKED),
                _reconciled(opening.id, "000001.SZ", DataQualityState.BLOCKED),
                _reconciled(regular.id, "600000.SH", DataQualityState.COMPLETE),
                _reconciled(regular.id, "000001.SZ", DataQualityState.COMPLETE),
            )
        )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        send_market_status_report.shutil,
        "disk_usage",
        lambda path: SimpleNamespace(total=20 * 1024**3, free=10 * 1024**3),
    )
    zone = ZoneInfo("Asia/Shanghai")
    _, healthy, details = send_market_status_report.build_status(
        database_settings,
        start_at=opening_at.astimezone(zone),
        observed_at=datetime(2026, 9, 28, 15, 10, tzinfo=zone),
        counts={"total": 2, "stocks": 1, "etfs": 1},
    )

    assert healthy is True
    assert details["report"]["collection_count"] == 1
    assert details["report"]["excluded_non_continuous_collection_count"] == 1


def test_status_card_flags_gap_above_acceptance_threshold(
    database_settings: Settings,
    session_factory_fixture: sessionmaker[Session],
    monkeypatch: object,
) -> None:
    start = datetime(2026, 9, 28, 1, 30, tzinfo=UTC)
    with session_factory_fixture.begin() as session:
        session.add_all(
            (
                _collection("gap-first", start, valid=2, latency=100),
                _collection("gap-second", start + timedelta(seconds=75), valid=2, latency=100),
            )
        )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        send_market_status_report.shutil,
        "disk_usage",
        lambda path: SimpleNamespace(total=20 * 1024**3, free=10 * 1024**3),
    )
    zone = ZoneInfo("Asia/Shanghai")
    _, healthy, details = send_market_status_report.build_status(
        database_settings,
        start_at=start.astimezone(zone),
        observed_at=(start + timedelta(minutes=2)).astimezone(zone),
        counts={"total": 2, "stocks": 1, "etfs": 1},
    )

    assert healthy is False
    assert details["report"]["collection_gaps"]["max_seconds"] == 75.0


def test_status_card_does_not_repeat_resolved_historical_gap_as_current_warning(
    database_settings: Settings,
    session_factory_fixture: sessionmaker[Session],
    monkeypatch: object,
) -> None:
    start = datetime(2026, 9, 28, 1, 30, tzinfo=UTC)
    with session_factory_fixture.begin() as session:
        session.add_all(
            (
                _collection("historical-first", start, valid=2, latency=100),
                _collection(
                    "historical-second", start + timedelta(seconds=75), valid=2, latency=100
                ),
                _collection(
                    "current-first", start + timedelta(minutes=19, seconds=25), valid=2,
                    latency=100,
                ),
                _collection(
                    "current-second", start + timedelta(minutes=19, seconds=40), valid=2,
                    latency=100,
                ),
                _collection(
                    "current-third", start + timedelta(minutes=19, seconds=55), valid=2,
                    latency=100,
                ),
            )
        )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        send_market_status_report.shutil,
        "disk_usage",
        lambda path: SimpleNamespace(total=20 * 1024**3, free=10 * 1024**3),
    )
    zone = ZoneInfo("Asia/Shanghai")
    message, healthy, details = send_market_status_report.build_status(
        database_settings,
        start_at=start.astimezone(zone),
        observed_at=(start + timedelta(minutes=20)).astimezone(zone),
        counts={"total": 2, "stocks": 1, "etfs": 1},
    )

    assert healthy is True
    assert details["historical_quality_ok"] is False
    assert details["current_quality_ok"] is True
    assert details["report"]["collection_gaps"]["max_seconds"] > 60
    assert "**状态**：当前运行正常" in message
    assert "**当日数据质量**：存在历史未达标项（不代表当前故障）" in message


def test_status_card_allows_first_minute_of_continuous_trading_to_start(
    database_settings: Settings,
    session_factory_fixture: sessionmaker[Session],
    monkeypatch: object,
) -> None:
    del session_factory_fixture
    monkeypatch.setattr(  # type: ignore[attr-defined]
        send_market_status_report.shutil,
        "disk_usage",
        lambda path: SimpleNamespace(total=20 * 1024**3, free=10 * 1024**3),
    )
    zone = ZoneInfo("Asia/Shanghai")
    message, healthy, details = send_market_status_report.build_status(
        database_settings,
        start_at=datetime(2026, 9, 28, 9, 30, tzinfo=zone),
        observed_at=datetime(2026, 9, 28, 9, 30, 30, tzinfo=zone),
        counts={"total": 2, "stocks": 1, "etfs": 1},
    )

    assert healthy is True
    assert details["current_quality_ok"] is True
    assert "**运行心跳**：开盘启动宽限期" in message


def _collection(
    key: str, started_at: datetime, *, valid: int, latency: float
) -> MarketCollectionRun:
    return MarketCollectionRun(
        id=f"collection-{key}",
        idempotency_key=f"metrics-{key}",
        expected_trade_date=date(2026, 9, 28),
        market_phase=MarketPhase.MORNING_CONTINUOUS,
        requested_symbols=["600000.SH", "000001.SZ"],
        started_at=started_at,
        finished_at=started_at + timedelta(milliseconds=200),
        provider_summaries={
            "tencent": {
                "provider": "tencent",
                "quote_count": valid,
                "valid_quote_count": valid,
                "elapsed_ms": latency,
                "circuit_state": "closed",
                "request_dispatch_ready_at": [started_at.isoformat()],
                "batch_issues": [],
                "archives": [],
            }
        },
        quality_counts={"complete": valid, "blocked": 2 - valid},
    )


def _reconciled(
    collection_id: str,
    symbol: str,
    state: DataQualityState,
) -> ReconciledQuoteSnapshot:
    return ReconciledQuoteSnapshot(
        collection_id=collection_id,
        symbol=symbol,
        exchange=Exchange.SSE if symbol.endswith(".SH") else Exchange.SZSE,
        quality_state=state,
        selected_provider=QuoteProvider.TENCENT if state is not DataQualityState.BLOCKED else None,
        comparisons=[],
        reasons=[],
    )
