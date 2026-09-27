"""Provider adapter contracts and parsing helpers."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Protocol

from dawnwatcher.domain.quotes import (
    MarketQuote,
    QuoteIssue,
    QuoteProvider,
    QuoteSymbol,
    RawQuoteBatch,
)


class QuoteParseError(ValueError):
    """Raised when a provider field cannot be normalized safely."""


class QuoteProviderAdapter(Protocol):
    """Provider-specific HTTP request and response parser."""

    provider: QuoteProvider
    encoding: str

    def build_url(self, symbols: tuple[QuoteSymbol, ...]) -> str: ...

    def request_headers(self) -> dict[str, str]: ...

    def parse(
        self, batch: RawQuoteBatch
    ) -> tuple[dict[str, MarketQuote], tuple[QuoteIssue, ...]]: ...


def decimal_field(value: str, *, field_name: str) -> Decimal:
    """Parse a finite decimal value with a useful field-specific error."""
    try:
        parsed = Decimal(value.strip())
    except (InvalidOperation, ValueError) as exc:
        raise QuoteParseError(f"invalid {field_name}: {value!r}") from exc
    if not parsed.is_finite():
        raise QuoteParseError(f"non-finite {field_name}: {value!r}")
    return parsed


def integer_field(value: str, *, field_name: str) -> int:
    """Parse an integer that may be formatted by a provider as a decimal."""
    parsed = decimal_field(value, field_name=field_name)
    integral = parsed.to_integral_value()
    if parsed != integral:
        raise QuoteParseError(f"non-integral {field_name}: {value!r}")
    return int(integral)
