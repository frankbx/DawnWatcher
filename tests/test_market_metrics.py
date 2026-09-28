"""Persisted dual-source reliability metric tests."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from sqlalchemy.orm import Session, sessionmaker

from dawnwatcher.domain import DataQualityState, Exchange, QuoteProvider
from dawnwatcher.market import MarketPhase
from dawnwatcher.storage.market_metrics import build_market_metrics_report
from dawnwatcher.storage.models import MarketCollectionRun, ReconciledQuoteSnapshot


def test_market_metrics_aggregate_success_latency_skew_conflicts_and_breakers(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    started = datetime(2026, 9, 28, 2, 0, tzinfo=UTC)
    with session_factory_fixture.begin() as session:
        first = _collection(
            "first",
            started,
            sina=_summary(valid=2, latency=100, ready_at=started),
            tencent=_summary(
                valid=2,
                latency=120,
                ready_at=started + timedelta(milliseconds=10),
            ),
            quality={"complete": 1, "conflicted": 1},
        )
        second_time = started + timedelta(seconds=15)
        second = _collection(
            "second",
            second_time,
            sina=_summary(
                valid=0,
                latency=10,
                ready_at=second_time,
                issue="circuit_opened",
                circuit_state="open",
            ),
            tencent=_summary(
                valid=2,
                latency=130,
                ready_at=second_time + timedelta(milliseconds=4),
            ),
            quality={"degraded": 2},
        )
        third_time = started + timedelta(seconds=60)
        third = _collection(
            "third",
            third_time,
            sina=_summary(
                valid=0,
                latency=0,
                ready_at=None,
                issue="circuit_open",
                circuit_state="open",
            ),
            tencent=_summary(valid=2, latency=110, ready_at=third_time),
            quality={"degraded": 2},
        )
        session.add_all((first, second, third))
        session.flush()
        session.add_all(
            (
                _reconciled(first.id, "600000.SH", DataQualityState.COMPLETE, "match"),
                _reconciled(first.id, "000001.SZ", DataQualityState.CONFLICTED, "conflict"),
                _reconciled(second.id, "600000.SH", DataQualityState.DEGRADED, None),
            )
        )

    with session_factory_fixture() as session:
        report = build_market_metrics_report(
            session,
            start_at=started,
            end_at=started + timedelta(days=1),
            expected_interval_seconds=15,
        )

    assert report["collection_count"] == 3
    assert report["providers"]["sina"]["successful_run_rate_pct"] == 33.333
    assert report["providers"]["sina"]["valid_quote_rate_pct"] == 33.333
    assert report["providers"]["sina"]["latency_ms"]["average"] == 55.0
    assert report["providers"]["sina"]["circuit_opened_count"] == 1
    assert report["providers"]["sina"]["circuit_suppressed_count"] == 1
    assert report["providers"]["tencent"]["successful_run_rate_pct"] == 100.0
    assert report["request_start_skew_ms"]["count"] == 2
    assert report["request_start_skew_ms"]["max"] == 10.0
    assert report["field_comparisons"]["conflict_count"] == 1
    assert report["field_comparisons"]["conflicts_by_field"] == {"latest": 1}
    assert report["collection_gaps"]["count"] == 1
    assert report["collection_gaps"]["max_seconds"] == 45.0


def _collection(
    key: str,
    started_at: datetime,
    *,
    sina: dict[str, object],
    tencent: dict[str, object],
    quality: dict[str, int],
) -> MarketCollectionRun:
    return MarketCollectionRun(
        id=f"collection-{key}",
        idempotency_key=f"metrics-{key}",
        expected_trade_date=date(2026, 9, 28),
        market_phase=MarketPhase.MORNING_CONTINUOUS,
        requested_symbols=["600000.SH", "000001.SZ"],
        started_at=started_at,
        finished_at=started_at + timedelta(milliseconds=200),
        provider_summaries={"sina": sina, "tencent": tencent},
        quality_counts=quality,
    )


def _summary(
    *,
    valid: int,
    latency: float,
    ready_at: datetime | None,
    issue: str | None = None,
    circuit_state: str = "closed",
) -> dict[str, object]:
    issues = (
        [{"code": issue, "message": issue, "severity": "error", "symbol": None}] if issue else []
    )
    return {
        "provider": "sina",
        "quote_count": valid,
        "valid_quote_count": valid,
        "elapsed_ms": latency,
        "circuit_state": circuit_state,
        "request_dispatch_ready_at": [ready_at.isoformat()] if ready_at else [],
        "batch_issues": issues,
        "archives": [],
    }


def _reconciled(
    collection_id: str,
    symbol: str,
    state: DataQualityState,
    comparison_state: str | None,
) -> ReconciledQuoteSnapshot:
    comparisons = (
        [
            {
                "field": "latest",
                "state": comparison_state,
                "sina_value": "10.00",
                "tencent_value": "10.10",
                "absolute_difference": "0.10",
            }
        ]
        if comparison_state
        else []
    )
    return ReconciledQuoteSnapshot(
        collection_id=collection_id,
        symbol=symbol,
        exchange=Exchange.SSE if symbol.endswith(".SH") else Exchange.SZSE,
        quality_state=state,
        selected_provider=QuoteProvider.SINA,
        comparisons=comparisons,
        reasons=[],
    )
