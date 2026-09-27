"""Deterministic Sina/Tencent quote reconciliation."""

from __future__ import annotations

from decimal import Decimal

from dawnwatcher.domain.quotes import (
    ComparisonState,
    DataQualityState,
    FieldComparison,
    MarketQuote,
    ProviderCollectionResult,
    QuoteProvider,
    QuoteSymbol,
    ReconciledQuote,
)

_PRICE_FIELDS = ("latest", "open", "previous_close", "high", "low", "bid1_price", "ask1_price")


def reconcile_quotes(
    symbols: tuple[QuoteSymbol, ...],
    provider_results: dict[QuoteProvider, ProviderCollectionResult],
) -> dict[str, ReconciledQuote]:
    """Reconcile validated snapshots without mixing fields between providers."""
    sina_result = provider_results.get(QuoteProvider.SINA)
    tencent_result = provider_results.get(QuoteProvider.TENCENT)
    sina_quotes = sina_result.valid_quotes if sina_result is not None else {}
    tencent_quotes = tencent_result.valid_quotes if tencent_result is not None else {}

    output: dict[str, ReconciledQuote] = {}
    for symbol in symbols:
        sina = sina_quotes.get(symbol.ts_code)
        tencent = tencent_quotes.get(symbol.ts_code)
        if sina is None and tencent is None:
            state = (
                DataQualityState.STALE
                if _has_stale_issue(symbol.ts_code, sina_result, tencent_result)
                else DataQualityState.BLOCKED
            )
            output[symbol.ts_code] = ReconciledQuote(
                symbol=symbol,
                state=state,
                selected_provider=None,
                selected_quote=None,
                reasons=("no validated provider quote is available",),
            )
            continue
        if sina is None or tencent is None:
            selected = sina or tencent
            if selected is None:  # pragma: no cover - guarded above for type narrowing
                raise AssertionError("one provider quote must be available")
            output[symbol.ts_code] = ReconciledQuote(
                symbol=symbol,
                state=DataQualityState.DEGRADED,
                selected_provider=selected.provider,
                selected_quote=selected,
                reasons=(f"only {selected.provider.value} supplied a validated quote",),
            )
            continue
        output[symbol.ts_code] = _reconcile_pair(symbol, sina, tencent)
    return output


def _reconcile_pair(
    symbol: QuoteSymbol,
    sina: MarketQuote,
    tencent: MarketQuote,
) -> ReconciledQuote:
    comparisons = [
        _compare_price(field_name, getattr(sina, field_name), getattr(tencent, field_name))
        for field_name in _PRICE_FIELDS
    ]
    comparisons.extend(
        (
            _informational_comparison("volume_shares", sina.volume_shares, tencent.volume_shares),
            _informational_comparison("amount_cny", sina.amount_cny, tencent.amount_cny),
            _informational_comparison(
                "quote_time_seconds",
                int(sina.quote_at.timestamp()),
                int(tencent.quote_at.timestamp()),
            ),
        )
    )

    reasons: list[str] = []
    if sina.quote_at.date() != tencent.quote_at.date():
        state = DataQualityState.CONFLICTED
        reasons.append("providers report different quote dates")
    elif any(item.state is ComparisonState.CONFLICT for item in comparisons):
        state = DataQualityState.CONFLICTED
        reasons.append("one or more critical price fields conflict")
    elif any(item.state is ComparisonState.NEAR for item in comparisons):
        state = DataQualityState.NEAR
        reasons.append("critical prices are close but outside exact-match tolerance")
    else:
        state = DataQualityState.COMPLETE

    if sina.name != tencent.name:
        reasons.append("provider security names differ")
    return ReconciledQuote(
        symbol=symbol,
        state=state,
        selected_provider=QuoteProvider.SINA,
        selected_quote=sina,
        comparisons=tuple(comparisons),
        reasons=tuple(reasons),
    )


def _compare_price(field_name: str, sina: Decimal, tencent: Decimal) -> FieldComparison:
    difference = abs(sina - tencent)
    reference = max(abs(sina), abs(tencent))
    match_tolerance = max(Decimal("0.01"), reference * Decimal("0.0002"))
    near_tolerance = max(Decimal("0.03"), reference * Decimal("0.0005"))
    if difference <= match_tolerance:
        state = ComparisonState.MATCH
    elif difference <= near_tolerance:
        state = ComparisonState.NEAR
    else:
        state = ComparisonState.CONFLICT
    return FieldComparison(
        field=field_name,
        state=state,
        sina_value=str(sina),
        tencent_value=str(tencent),
        absolute_difference=str(difference),
    )


def _informational_comparison(
    field_name: str, sina: int | Decimal, tencent: int | Decimal
) -> FieldComparison:
    return FieldComparison(
        field=field_name,
        state=ComparisonState.INFORMATIONAL,
        sina_value=str(sina),
        tencent_value=str(tencent),
        absolute_difference=str(abs(sina - tencent)),
    )


def _has_stale_issue(
    symbol: str,
    sina_result: ProviderCollectionResult | None,
    tencent_result: ProviderCollectionResult | None,
) -> bool:
    results = (result for result in (sina_result, tencent_result) if result is not None)
    return any(
        issue.code == "stale_trade_date"
        for result in results
        for issue in result.quote_issues.get(symbol, ())
    )
