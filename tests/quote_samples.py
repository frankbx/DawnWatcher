"""Deterministic provider payload builders used by Phase 2 tests."""

from __future__ import annotations

from datetime import UTC, datetime

from dawnwatcher.domain import QuoteProvider, QuoteSymbol
from dawnwatcher.domain.quotes import RawQuoteBatch


def sina_line(
    provider_code: str = "sh600000",
    *,
    name: str = "浦发银行",
    quote_date: str = "2026-09-24",
    quote_time: str = "10:00:00",
    latest: str = "9.000",
) -> str:
    fields = [
        name,
        "8.990",
        "8.980",
        latest,
        "9.050",
        "8.970",
        "9.000",
        "9.010",
        "52836397",
        "475964884.000",
        "188700",
        "9.000",
        "411700",
        "8.990",
        "291600",
        "8.980",
        "500100",
        "8.970",
        "590400",
        "8.960",
        "105300",
        "9.010",
        "816161",
        "9.020",
        "1653700",
        "9.030",
        "934515",
        "9.040",
        "1796900",
        "9.050",
        quote_date,
        quote_time,
        "00",
        "",
    ]
    return f'var hq_str_{provider_code}="{",".join(fields)}";'


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
