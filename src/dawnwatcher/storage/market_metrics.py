"""Aggregate persisted Tencent collection reliability metrics."""

from __future__ import annotations

import math
from datetime import datetime
from statistics import fmean
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dawnwatcher.domain import DataQualityState
from dawnwatcher.storage.models import MarketCollectionRun, ReconciledQuoteSnapshot

_PROVIDER_KEY = "tencent"


def build_market_metrics_report(
    session: Session,
    *,
    start_at: datetime,
    end_at: datetime,
    expected_interval_seconds: float,
) -> dict[str, Any]:
    """Summarize Tencent availability, latency, quality, and collection gaps."""
    _validate_range(start_at, end_at)
    all_collections = list(
        session.scalars(
            select(MarketCollectionRun)
            .where(
                MarketCollectionRun.started_at >= start_at,
                MarketCollectionRun.started_at < end_at,
            )
            .order_by(MarketCollectionRun.started_at)
        )
    )
    collections = [item for item in all_collections if _is_single_source(item)]
    collection_ids = [item.id for item in collections]
    reconciled = (
        list(
            session.scalars(
                select(ReconciledQuoteSnapshot).where(
                    ReconciledQuoteSnapshot.collection_id.in_(collection_ids)
                )
            )
        )
        if collection_ids
        else []
    )

    quality_counts = {
        state.value: sum(snapshot.quality_state is state for snapshot in reconciled)
        for state in DataQualityState
    }
    gap_durations = _collection_gap_durations(collections, expected_interval_seconds)
    return {
        "provider": _PROVIDER_KEY,
        "single_source_mode": True,
        "start_at": start_at.isoformat(),
        "end_at": end_at.isoformat(),
        "collection_count": len(collections),
        "total_collection_count": len(all_collections),
        "legacy_dual_collection_count": len(all_collections) - len(collections),
        "requested_quote_count": sum(len(item.requested_symbols) for item in collections),
        "metrics": _provider_metrics(collections),
        "quality_counts": quality_counts,
        "collection_gaps": {
            "expected_interval_seconds": expected_interval_seconds,
            "gap_threshold_seconds": expected_interval_seconds * 1.5,
            "count": len(gap_durations),
            "max_seconds": round(max(gap_durations), 3) if gap_durations else None,
            "durations_seconds": [round(value, 3) for value in gap_durations],
        },
    }


def _provider_metrics(collections: list[MarketCollectionRun]) -> dict[str, Any]:
    run_count = len(collections)
    successful_runs = 0
    valid_quotes = 0
    requested_quotes = 0
    latencies: list[float] = []
    circuit_opened_count = 0
    circuit_suppressed_count = 0
    circuit_open_state_runs = 0

    for collection in collections:
        requested_count = len(collection.requested_symbols)
        requested_quotes += requested_count
        raw_summary = collection.provider_summaries.get(_PROVIDER_KEY, {})
        summary = raw_summary if isinstance(raw_summary, dict) else {}
        valid_count = _integer(summary.get("valid_quote_count"))
        valid_quotes += valid_count
        elapsed = summary.get("elapsed_ms")
        raw_ready_times = summary.get("request_dispatch_ready_at")
        request_was_attempted = raw_ready_times is None or (
            isinstance(raw_ready_times, list) and bool(raw_ready_times)
        )
        if isinstance(elapsed, int | float) and request_was_attempted:
            latencies.append(float(elapsed))
        raw_issues = summary.get("batch_issues", [])
        issues = raw_issues if isinstance(raw_issues, list) else []
        issue_codes = {str(issue.get("code")) for issue in issues if isinstance(issue, dict)}
        has_error = any(
            isinstance(issue, dict) and issue.get("severity") == "error" for issue in issues
        )
        if valid_count == requested_count and not has_error:
            successful_runs += 1
        circuit_opened_count += int("circuit_opened" in issue_codes)
        circuit_suppressed_count += int("circuit_open" in issue_codes)
        circuit_open_state_runs += int(summary.get("circuit_state") == "open")

    return {
        "run_count": run_count,
        "successful_run_count": successful_runs,
        "successful_run_rate_pct": _percentage(successful_runs, run_count),
        "requested_quote_count": requested_quotes,
        "valid_quote_count": valid_quotes,
        "valid_quote_rate_pct": _percentage(valid_quotes, requested_quotes),
        "latency_ms": _distribution(latencies),
        "circuit_opened_count": circuit_opened_count,
        "circuit_suppressed_count": circuit_suppressed_count,
        "circuit_open_state_run_count": circuit_open_state_runs,
    }


def _is_single_source(collection: MarketCollectionRun) -> bool:
    return set(collection.provider_summaries) == {_PROVIDER_KEY}


def _collection_gap_durations(
    collections: list[MarketCollectionRun], expected_interval_seconds: float
) -> list[float]:
    threshold = expected_interval_seconds * 1.5
    gaps: list[float] = []
    for previous, current in zip(collections, collections[1:], strict=False):
        if (
            previous.expected_trade_date != current.expected_trade_date
            or previous.market_phase != current.market_phase
        ):
            continue
        duration = (current.started_at - previous.started_at).total_seconds()
        if duration > threshold:
            gaps.append(duration)
    return gaps


def _distribution(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "average": None, "p50": None, "p95": None, "max": None}
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "average": round(fmean(ordered), 3),
        "p50": round(_percentile(ordered, 0.50), 3),
        "p95": round(_percentile(ordered, 0.95), 3),
        "max": round(ordered[-1], 3),
    }


def _percentile(ordered: list[float], percentile: float) -> float:
    return ordered[max(0, math.ceil(percentile * len(ordered)) - 1)]


def _percentage(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator * 100, 3) if denominator else None


def _integer(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _validate_range(start_at: datetime, end_at: datetime) -> None:
    if start_at.tzinfo is None or start_at.utcoffset() is None:
        raise ValueError("start_at must be timezone-aware")
    if end_at.tzinfo is None or end_at.utcoffset() is None:
        raise ValueError("end_at must be timezone-aware")
    if end_at <= start_at:
        raise ValueError("end_at must be after start_at")
