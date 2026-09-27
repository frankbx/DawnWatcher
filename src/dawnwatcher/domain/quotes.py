"""Provider-independent quote and data-quality contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from dawnwatcher.market import MarketPhase


class QuoteProvider(StrEnum):
    """Supported free real-time quote providers."""

    SINA = "sina"
    TENCENT = "tencent"


class Exchange(StrEnum):
    """Supported mainland exchanges."""

    SSE = "sse"
    SZSE = "szse"
    BSE = "bse"


class DataQualityState(StrEnum):
    """Decision-facing state after validation and reconciliation."""

    COMPLETE = "complete"
    NEAR = "near"
    DEGRADED = "degraded"
    CONFLICTED = "conflicted"
    STALE = "stale"
    BLOCKED = "blocked"


class IssueSeverity(StrEnum):
    """Severity of a parse or validation issue."""

    WARNING = "warning"
    ERROR = "error"


class ComparisonState(StrEnum):
    """Agreement level for one normalized field."""

    MATCH = "match"
    NEAR = "near"
    CONFLICT = "conflict"
    INFORMATIONAL = "informational"


@dataclass(frozen=True, slots=True)
class QuoteSymbol:
    """Canonical Tushare symbol represented by a numeric code and exchange."""

    code: str
    exchange: Exchange

    @classmethod
    def parse(cls, value: str) -> QuoteSymbol:
        """Parse Tushare codes plus legacy numeric and provider-prefixed aliases."""
        normalized = value.strip().upper()
        explicit_exchange: Exchange | None = None
        if "." in normalized:
            parts = normalized.rsplit(".", maxsplit=1)
            if len(parts) != 2:
                raise ValueError(f"invalid Tushare security code: {value}")
            normalized, suffix = parts
            try:
                explicit_exchange = {
                    "SH": Exchange.SSE,
                    "SZ": Exchange.SZSE,
                    "BJ": Exchange.BSE,
                }[suffix]
            except KeyError as exc:
                raise ValueError(f"unsupported Tushare exchange suffix: {value}") from exc
        elif normalized.startswith("SH"):
            explicit_exchange = Exchange.SSE
            normalized = normalized[2:]
        elif normalized.startswith("SZ"):
            explicit_exchange = Exchange.SZSE
            normalized = normalized[2:]
        elif normalized.startswith("BJ"):
            explicit_exchange = Exchange.BSE
            normalized = normalized[2:]

        if len(normalized) != 6 or not normalized.isdigit():
            raise ValueError(f"invalid mainland security code: {value}")

        inferred = _infer_exchange(normalized)
        if explicit_exchange is not None and explicit_exchange is not inferred:
            raise ValueError(f"symbol prefix and exchange disagree: {value}")
        return cls(code=normalized, exchange=inferred)

    @property
    def ts_code(self) -> str:
        """Return the canonical uppercase Tushare security code."""
        suffix = {
            Exchange.SSE: "SH",
            Exchange.SZSE: "SZ",
            Exchange.BSE: "BJ",
        }[self.exchange]
        return f"{self.code}.{suffix}"

    @property
    def provider_code(self) -> str:
        """Return the common provider prefix used by Sina and Tencent."""
        prefix = {
            Exchange.SSE: "sh",
            Exchange.SZSE: "sz",
            Exchange.BSE: "bj",
        }[self.exchange]
        return f"{prefix}{self.code}"


def _infer_exchange(code: str) -> Exchange:
    if code.startswith(("600", "601", "603", "605", "688", "689")):
        return Exchange.SSE
    if code.startswith(("000", "001", "002", "003", "300", "301")):
        return Exchange.SZSE
    if code.startswith(("4", "8", "920")):
        return Exchange.BSE
    raise ValueError(f"unsupported A-share code prefix: {code}")


@dataclass(frozen=True, slots=True)
class QuoteIssue:
    """Machine-readable provider, parser, or validator issue."""

    code: str
    message: str
    severity: IssueSeverity
    symbol: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "code": self.code,
            "message": self.message,
            "severity": self.severity.value,
            "symbol": self.symbol,
        }


@dataclass(frozen=True, slots=True)
class MarketQuote:
    """Normalized top-of-book quote from exactly one provider."""

    provider: QuoteProvider
    symbol: QuoteSymbol
    name: str
    quote_at: datetime
    fetched_at: datetime
    open: Decimal
    previous_close: Decimal
    latest: Decimal
    high: Decimal
    low: Decimal
    volume_shares: int
    amount_cny: Decimal
    bid1_price: Decimal
    bid1_volume_shares: int
    ask1_price: Decimal
    ask1_volume_shares: int
    volume_precision_shares: int
    raw_field_count: int

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation without float conversion."""
        return {
            "provider": self.provider.value,
            "symbol": self.symbol.ts_code,
            "exchange": self.symbol.exchange.value,
            "name": self.name,
            "quote_at": self.quote_at.isoformat(),
            "fetched_at": self.fetched_at.isoformat(),
            "open": str(self.open),
            "previous_close": str(self.previous_close),
            "latest": str(self.latest),
            "high": str(self.high),
            "low": str(self.low),
            "volume_shares": self.volume_shares,
            "amount_cny": str(self.amount_cny),
            "bid1_price": str(self.bid1_price),
            "bid1_volume_shares": self.bid1_volume_shares,
            "ask1_price": str(self.ask1_price),
            "ask1_volume_shares": self.ask1_volume_shares,
            "volume_precision_shares": self.volume_precision_shares,
            "raw_field_count": self.raw_field_count,
        }


@dataclass(frozen=True, slots=True)
class RawQuoteBatch:
    """Exact bytes and request metadata received from one provider."""

    provider: QuoteProvider
    requested_symbols: tuple[QuoteSymbol, ...]
    requested_at: datetime
    fetched_at: datetime
    elapsed_ms: float
    status_code: int
    encoding: str
    request_url: str
    body: bytes


@dataclass(frozen=True, slots=True)
class RawArchiveRecord:
    """Immutable reference to an archived raw provider response."""

    provider: QuoteProvider
    path: str
    sha256: str
    size_bytes: int

    def to_dict(self) -> dict[str, str | int]:
        return {
            "provider": self.provider.value,
            "path": self.path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class ProviderCollectionResult:
    """Quotes, issues, and archives produced by one provider in one cycle."""

    provider: QuoteProvider
    quotes: dict[str, MarketQuote] = field(default_factory=dict)
    quote_issues: dict[str, tuple[QuoteIssue, ...]] = field(default_factory=dict)
    batch_issues: tuple[QuoteIssue, ...] = ()
    archives: tuple[RawArchiveRecord, ...] = ()
    request_dispatch_ready_at: tuple[datetime, ...] = ()
    elapsed_ms: float = 0.0
    circuit_state: str = "closed"

    @property
    def valid_quotes(self) -> dict[str, MarketQuote]:
        """Return quotes with no symbol-level error issue."""
        return {
            symbol: quote
            for symbol, quote in self.quotes.items()
            if not any(
                issue.severity is IssueSeverity.ERROR for issue in self.quote_issues.get(symbol, ())
            )
        }

    def to_summary(self) -> dict[str, Any]:
        return {
            "provider": self.provider.value,
            "quote_count": len(self.quotes),
            "valid_quote_count": len(self.valid_quotes),
            "elapsed_ms": round(self.elapsed_ms, 3),
            "circuit_state": self.circuit_state,
            "request_dispatch_ready_at": [
                value.isoformat() for value in self.request_dispatch_ready_at
            ],
            "batch_issues": [issue.to_dict() for issue in self.batch_issues],
            "archives": [archive.to_dict() for archive in self.archives],
        }


@dataclass(frozen=True, slots=True)
class FieldComparison:
    """Comparison of one normalized value across two providers."""

    field: str
    state: ComparisonState
    sina_value: str
    tencent_value: str
    absolute_difference: str

    def to_dict(self) -> dict[str, str]:
        return {
            "field": self.field,
            "state": self.state.value,
            "sina_value": self.sina_value,
            "tencent_value": self.tencent_value,
            "absolute_difference": self.absolute_difference,
        }


@dataclass(frozen=True, slots=True)
class ReconciledQuote:
    """Decision-facing quote quality result without cross-provider field mixing."""

    symbol: QuoteSymbol
    state: DataQualityState
    selected_provider: QuoteProvider | None
    selected_quote: MarketQuote | None
    comparisons: tuple[FieldComparison, ...] = ()
    reasons: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol.ts_code,
            "exchange": self.symbol.exchange.value,
            "state": self.state.value,
            "selected_provider": (
                self.selected_provider.value if self.selected_provider is not None else None
            ),
            "selected_quote": (
                self.selected_quote.to_dict() if self.selected_quote is not None else None
            ),
            "comparisons": [comparison.to_dict() for comparison in self.comparisons],
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True, slots=True)
class MarketCollectionResult:
    """Complete output of one dual-provider collection cycle."""

    collection_id: str
    idempotency_key: str
    requested_symbols: tuple[QuoteSymbol, ...]
    expected_trade_date: date | None
    started_at: datetime
    finished_at: datetime
    providers: dict[QuoteProvider, ProviderCollectionResult]
    reconciled: dict[str, ReconciledQuote]
    market_phase: MarketPhase | None = None

    @property
    def request_start_skew_ms(self) -> tuple[float, ...]:
        """Return the local launch-time skew for every dual-provider batch."""
        provider_starts = [result.request_dispatch_ready_at for result in self.providers.values()]
        if len(provider_starts) < 2:
            return ()
        comparable_batches = min(len(values) for values in provider_starts)
        return tuple(
            (
                max(values[index] for values in provider_starts)
                - min(values[index] for values in provider_starts)
            ).total_seconds()
            * 1_000
            for index in range(comparable_batches)
        )

    def to_dict(self, *, include_quotes: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "collection_id": self.collection_id,
            "idempotency_key": self.idempotency_key,
            "requested_symbols": [symbol.ts_code for symbol in self.requested_symbols],
            "expected_trade_date": (
                self.expected_trade_date.isoformat()
                if self.expected_trade_date is not None
                else None
            ),
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "market_phase": self.market_phase.value if self.market_phase is not None else None,
            "auction_mode": (
                self.market_phase.auction_mode.value if self.market_phase is not None else None
            ),
            "providers": {
                provider.value: result.to_summary() for provider, result in self.providers.items()
            },
            "request_start_skew_ms": [round(value, 3) for value in self.request_start_skew_ms],
            "max_request_start_skew_ms": (
                round(max(self.request_start_skew_ms), 3) if self.request_start_skew_ms else None
            ),
            "quality_counts": {
                state.value: sum(item.state is state for item in self.reconciled.values())
                for state in DataQualityState
            },
        }
        if include_quotes:
            payload["reconciled"] = {
                symbol: quote.to_dict() for symbol, quote in self.reconciled.items()
            }
        return payload
