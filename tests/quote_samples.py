"""Deterministic provider payload builders used by Phase 2 tests."""

from __future__ import annotations

from datetime import UTC, datetime

from dawnwatcher.domain import QuoteProvider, QuoteSymbol
from dawnwatcher.domain.quotes import RawQuoteBatch


def tencent_line(
    provider_code: str = "sh600000",
    *,
    name: str = "浦发银行",
    timestamp: str = "20260924100000",
    latest: str = "9.00",
) -> str:
    fields = [""] * 88
    fields[0] = "1"
    fields[1] = name
    fields[2] = provider_code[2:]
    fields[3] = latest
    fields[4] = "8.98"
    fields[5] = "8.99"
    fields[6] = "528364"
    fields[9] = "9.00"
    fields[10] = "1887"
    fields[19] = "9.01"
    fields[20] = "1053"
    fields[30] = timestamp
    fields[33] = "9.05"
    fields[34] = "8.97"
    fields[35] = f"{latest}/528364/475964884"
    fields[36] = "528364"
    fields[37] = "47596"
    return f'v_{provider_code}="{"~".join(fields)}";'


def raw_batch(provider: QuoteProvider, symbol: QuoteSymbol, body: bytes) -> RawQuoteBatch:
    fetched_at = datetime(2026, 9, 24, 2, 0, 1, tzinfo=UTC)
    return RawQuoteBatch(
        provider=provider,
        requested_symbols=(symbol,),
        requested_at=fetched_at,
        fetched_at=fetched_at,
        elapsed_ms=10.0,
        status_code=200,
        encoding="gb18030",
        request_url="https://example.test/quotes",
        body=body,
    )
