"""Atomic persistence of single-provider collection results."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from dawnwatcher.domain.quotes import DataQualityState, MarketCollectionResult
from dawnwatcher.storage.audit import append_audit_event
from dawnwatcher.storage.models import (
    MarketCollectionRun,
    ProviderQuoteSnapshot,
    ReconciledQuoteSnapshot,
)


def persist_market_collection(
    session: Session,
    result: MarketCollectionResult,
) -> MarketCollectionRun:
    """Persist a collection and all child snapshots exactly once."""
    existing = session.scalar(
        select(MarketCollectionRun).where(
            MarketCollectionRun.idempotency_key == result.idempotency_key
        )
    )
    if existing is not None:
        return existing

    quality_counts = {
        state.value: sum(item.state is state for item in result.reconciled.values())
        for state in DataQualityState
    }
    collection = MarketCollectionRun(
        id=result.collection_id,
        idempotency_key=result.idempotency_key,
        expected_trade_date=result.expected_trade_date,
        market_phase=result.market_phase,
        requested_symbols=[symbol.ts_code for symbol in result.requested_symbols],
        started_at=result.started_at,
        finished_at=result.finished_at,
        provider_summaries={result.provider.value: result.provider_result.to_summary()},
        quality_counts=quality_counts,
    )
    session.add(collection)
    session.flush()

    provider_result = result.provider_result
    for symbol, quote in provider_result.quotes.items():
        session.add(
            ProviderQuoteSnapshot(
                collection_id=collection.id,
                provider=quote.provider,
                symbol=symbol,
                exchange=quote.symbol.exchange,
                name=quote.name,
                quote_at=quote.quote_at,
                fetched_at=quote.fetched_at,
                open=quote.open,
                previous_close=quote.previous_close,
                latest=quote.latest,
                high=quote.high,
                low=quote.low,
                volume_shares=quote.volume_shares,
                amount_cny=quote.amount_cny,
                bid1_price=quote.bid1_price,
                bid1_volume_shares=quote.bid1_volume_shares,
                ask1_price=quote.ask1_price,
                ask1_volume_shares=quote.ask1_volume_shares,
                volume_precision_shares=quote.volume_precision_shares,
                raw_field_count=quote.raw_field_count,
                validation_issues=[
                    issue.to_dict() for issue in provider_result.quote_issues.get(symbol, ())
                ],
            )
        )

    for symbol, reconciled in result.reconciled.items():
        session.add(
            ReconciledQuoteSnapshot(
                collection_id=collection.id,
                symbol=symbol,
                exchange=reconciled.symbol.exchange,
                quality_state=reconciled.state,
                selected_provider=reconciled.selected_provider,
                comparisons=list(reconciled.comparisons),
                reasons=list(reconciled.reasons),
            )
        )

    append_audit_event(
        session,
        event_type="market.collection.persisted",
        entity_type="market_collection_run",
        entity_id=collection.id,
        correlation_id=collection.id,
        idempotency_key=f"market-collection-persisted:{result.idempotency_key}",
        payload={
            "requested_symbol_count": len(result.requested_symbols),
            "market_phase": result.market_phase.value if result.market_phase is not None else None,
            "quality_counts": quality_counts,
        },
    )
    session.flush()
    return collection
