"""Persisted Tencent reliability metric tests."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from sqlalchemy.orm import Session, sessionmaker

from dawnwatcher.domain import DataQualityState, Exchange, QuoteProvider
from dawnwatcher.market import MarketPhase
from dawnwatcher.storage.market_metrics import build_market_metrics_report
from dawnwatcher.storage.models import MarketCollectionRun, ReconciledQuoteSnapshot


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
