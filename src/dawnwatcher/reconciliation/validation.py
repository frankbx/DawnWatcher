"""Provider-independent structural and freshness validation."""

from __future__ import annotations

from datetime import UTC, date, timedelta
from decimal import Decimal

from dawnwatcher.domain.quotes import IssueSeverity, MarketQuote, QuoteIssue


def validate_quote(
    quote: MarketQuote,
    *,
    expected_trade_date: date | None = None,
) -> tuple[QuoteIssue, ...]:
    """Validate invariants required before a quote can enter reconciliation."""
    issues: list[QuoteIssue] = []
    symbol = quote.symbol.ts_code

    if not quote.name:
        issues.append(_error("empty_name", "security name is empty", symbol))
    if expected_trade_date is not None and quote.quote_at.date() != expected_trade_date:
        issues.append(
            _error(
                "stale_trade_date",
                f"quote date {quote.quote_at.date()} != expected {expected_trade_date}",
                symbol,
            )
        )
    if quote.quote_at.astimezone(UTC) > quote.fetched_at.astimezone(UTC) + timedelta(minutes=5):
        issues.append(_error("future_quote_time", "quote timestamp is in the future", symbol))

    for field_name, value in (
        ("open", quote.open),
        ("previous_close", quote.previous_close),
        ("latest", quote.latest),
        ("high", quote.high),
        ("low", quote.low),
    ):
        if value <= 0:
            issues.append(_error("non_positive_price", f"{field_name} must be positive", symbol))

    if quote.low > quote.high:
        issues.append(_error("invalid_price_range", "low is greater than high", symbol))
    if quote.latest < quote.low or quote.latest > quote.high:
        issues.append(
            _error("latest_outside_range", "latest is outside the daily high/low", symbol)
        )
    if quote.latest > quote.previous_close * Decimal("10"):
        issues.append(_error("implausible_price", "latest exceeds 10x previous close", symbol))

    for field_name, market_value in (
        ("volume_shares", quote.volume_shares),
        ("amount_cny", quote.amount_cny),
        ("bid1_volume_shares", quote.bid1_volume_shares),
        ("ask1_volume_shares", quote.ask1_volume_shares),
    ):
        if market_value < 0:
            issues.append(_error("negative_market_value", f"{field_name} is negative", symbol))

    if quote.bid1_price < 0 or quote.ask1_price < 0:
        issues.append(_error("negative_book_price", "top-of-book price is negative", symbol))
    if quote.bid1_price > 0 and quote.ask1_price > 0 and quote.bid1_price > quote.ask1_price:
        issues.append(_error("crossed_book", "bid1 is greater than ask1", symbol))
    return tuple(issues)


def _error(code: str, message: str, symbol: str) -> QuoteIssue:
    return QuoteIssue(
        code=code,
        message=message,
        severity=IssueSeverity.ERROR,
        symbol=symbol,
    )
