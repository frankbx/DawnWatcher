"""Tencent market-data collection with durable raw-response replay."""

from __future__ import annotations

import time
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import httpx

from dawnwatcher.config import Settings
from dawnwatcher.domain.quotes import (
    DataQualityState,
    IssueSeverity,
    MarketCollectionResult,
    ProviderCollectionResult,
    QuoteIssue,
    QuoteProvider,
    QuoteSymbol,
    RawArchiveRecord,
    RawQuoteBatch,
    ReconciledQuote,
)
from dawnwatcher.market import MarketPhase
from dawnwatcher.providers.archive import RawQuoteArchive
from dawnwatcher.providers.base import QuoteProviderAdapter
from dawnwatcher.providers.circuit_breaker import CircuitBreaker
from dawnwatcher.providers.tencent import TencentQuoteAdapter
from dawnwatcher.reconciliation.validation import validate_quote


class MarketDataCollector:
    """Fetch, archive, parse, and validate one Tencent market snapshot."""

    def __init__(
        self,
        settings: Settings,
        *,
        client: httpx.AsyncClient | None = None,
        adapter: QuoteProviderAdapter | None = None,
    ) -> None:
        self.settings = settings
        self._owns_client = client is None
        self.client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(settings.market_request_timeout_seconds),
            follow_redirects=False,
        )
        self.adapter = adapter or TencentQuoteAdapter()
        if self.adapter.provider is not QuoteProvider.TENCENT:
            raise ValueError("only the Tencent quote adapter is supported")
        self.breaker = CircuitBreaker(
            failure_threshold=settings.circuit_failure_threshold,
            cooldown_seconds=settings.circuit_cooldown_seconds,
        )
        self.archive = RawQuoteArchive(settings.data_dir / "raw" / "quotes")

    async def __aenter__(self) -> MarketDataCollector:
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc_type, exc, traceback
        await self.aclose()

    async def aclose(self) -> None:
        """Close the internally-created HTTP connection pool."""
        if self._owns_client:
            await self.client.aclose()

    async def collect(
        self,
        symbols: tuple[QuoteSymbol, ...],
        *,
        expected_trade_date: date | None = None,
        idempotency_key: str | None = None,
        archive_raw: bool | None = None,
        market_phase: MarketPhase | None = None,
    ) -> MarketCollectionResult:
        """Collect one Tencent snapshot without retries."""
        normalized_symbols = _deduplicate_symbols(symbols)
        if not normalized_symbols:
            raise ValueError("at least one symbol is required")
        started_at = datetime.now(UTC)
        collection_id = str(uuid4())
        key = idempotency_key or f"market:{started_at.isoformat()}:{collection_id}"
        should_archive = self.settings.archive_raw_quotes if archive_raw is None else archive_raw

        provider_result = await self._collect_provider(
            normalized_symbols,
            expected_trade_date=expected_trade_date,
            archive_raw=should_archive,
        )
        reconciled = _assess_quotes(normalized_symbols, provider_result)
        return MarketCollectionResult(
            collection_id=collection_id,
            idempotency_key=key,
            requested_symbols=normalized_symbols,
            expected_trade_date=expected_trade_date,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            provider_result=provider_result,
            reconciled=reconciled,
            market_phase=market_phase,
        )

    async def _collect_provider(
        self,
        symbols: tuple[QuoteSymbol, ...],
        *,
        expected_trade_date: date | None,
        archive_raw: bool,
    ) -> ProviderCollectionResult:
        adapter = self.adapter
        breaker = self.breaker
        if not breaker.allow_request():
            return ProviderCollectionResult(
                provider=adapter.provider,
                batch_issues=(
                    QuoteIssue(
                        code="circuit_open",
                        message="provider request suppressed until the recovery probe",
                        severity=IssueSeverity.ERROR,
                    ),
                ),
                circuit_state=breaker.state.value,
            )

        started = time.perf_counter()
        quotes = {}
        quote_issues: dict[str, list[QuoteIssue]] = {}
        batch_issues: list[QuoteIssue] = []
        archives: list[RawArchiveRecord] = []
        request_dispatch_ready_at: list[datetime] = []
        try:
            for chunk in _chunks(symbols, self.settings.market_batch_size):
                batch = await self._fetch_batch(chunk)
                request_dispatch_ready_at.append(batch.requested_at)
                if archive_raw:
                    archives.append(self.archive.write(batch))
                if batch.status_code < 200 or batch.status_code >= 300:
                    raise httpx.HTTPStatusError(
                        f"provider returned HTTP {batch.status_code}",
                        request=httpx.Request("GET", batch.request_url),
                        response=httpx.Response(batch.status_code),
                    )
                parsed, parse_issues = adapter.parse(batch)
                quotes.update(parsed)
                for issue in parse_issues:
                    if issue.symbol is None:
                        batch_issues.append(issue)
                    else:
                        quote_issues.setdefault(issue.symbol, []).append(issue)

            for symbol, quote in quotes.items():
                quote_issues.setdefault(symbol, []).extend(
                    validate_quote(quote, expected_trade_date=expected_trade_date)
                )
            valid_count = sum(
                not any(
                    item.severity is IssueSeverity.ERROR for item in quote_issues.get(symbol, ())
                )
                for symbol in quotes
            )
            if not quotes:
                _record_breaker_failure(breaker, batch_issues)
            else:
                breaker.record_success()
            if valid_count == 0:
                batch_issues.append(
                    QuoteIssue(
                        code="no_valid_quotes",
                        message="Tencent returned no quote eligible for monitoring",
                        severity=IssueSeverity.ERROR,
                    )
                )
        except (httpx.HTTPError, UnicodeError, ValueError, OSError) as exc:
            _record_breaker_failure(breaker, batch_issues)
            batch_issues.append(
                QuoteIssue(
                    code="provider_failure",
                    message=f"{type(exc).__name__}: {exc}",
                    severity=IssueSeverity.ERROR,
                )
            )
        return ProviderCollectionResult(
            provider=adapter.provider,
            quotes=quotes,
            quote_issues={key: tuple(value) for key, value in quote_issues.items()},
            batch_issues=tuple(batch_issues),
            archives=tuple(archives),
            request_dispatch_ready_at=tuple(request_dispatch_ready_at),
            elapsed_ms=(time.perf_counter() - started) * 1_000,
            circuit_state=breaker.state.value,
        )

    async def _fetch_batch(
        self,
        symbols: tuple[QuoteSymbol, ...],
    ) -> RawQuoteBatch:
        adapter = self.adapter
        url = adapter.build_url(symbols)
        headers = adapter.request_headers()
        requested_at = datetime.now(UTC)
        started = time.perf_counter()
        response = await self.client.get(url, headers=headers)
        fetched_at = datetime.now(UTC)
        return RawQuoteBatch(
            provider=adapter.provider,
            requested_symbols=symbols,
            requested_at=requested_at,
            fetched_at=fetched_at,
            elapsed_ms=(time.perf_counter() - started) * 1_000,
            status_code=response.status_code,
            encoding=adapter.encoding,
            request_url=str(response.request.url),
            body=response.content,
        )


def replay_archive(
    path: Path,
    *,
    expected_trade_date: date | None = None,
) -> ProviderCollectionResult:
    """Replay one archived provider response without network access."""
    archive = RawQuoteArchive(path.parent)
    batch = archive.read(path)
    if batch.provider is not QuoteProvider.TENCENT:
        raise ValueError("only Tencent archives are supported by the single-source collector")
    adapter: QuoteProviderAdapter = TencentQuoteAdapter()
    quotes, parse_issues = adapter.parse(batch)
    quote_issues: dict[str, list[QuoteIssue]] = {}
    batch_issues: list[QuoteIssue] = []
    for issue in parse_issues:
        if issue.symbol is None:
            batch_issues.append(issue)
        else:
            quote_issues.setdefault(issue.symbol, []).append(issue)
    for symbol, quote in quotes.items():
        quote_issues.setdefault(symbol, []).extend(
            validate_quote(quote, expected_trade_date=expected_trade_date)
        )
    return ProviderCollectionResult(
        provider=batch.provider,
        quotes=quotes,
        quote_issues={key: tuple(value) for key, value in quote_issues.items()},
        batch_issues=tuple(batch_issues),
        request_dispatch_ready_at=(batch.requested_at,),
        elapsed_ms=batch.elapsed_ms,
        circuit_state="replay",
    )


def parse_symbols(values: list[str]) -> tuple[QuoteSymbol, ...]:
    """Parse CLI/config symbol strings and reject duplicates deterministically."""
    return _deduplicate_symbols(tuple(QuoteSymbol.parse(value) for value in values))


def _deduplicate_symbols(symbols: tuple[QuoteSymbol, ...]) -> tuple[QuoteSymbol, ...]:
    return tuple({symbol.ts_code: symbol for symbol in symbols}.values())


def _chunks(symbols: tuple[QuoteSymbol, ...], size: int) -> tuple[tuple[QuoteSymbol, ...], ...]:
    return tuple(symbols[index : index + size] for index in range(0, len(symbols), size))


def _record_breaker_failure(
    breaker: CircuitBreaker,
    batch_issues: list[QuoteIssue],
) -> None:
    previous_state = breaker.state
    breaker.record_failure()
    if breaker.state.value == "open" and previous_state.value != "open":
        batch_issues.append(
            QuoteIssue(
                code="circuit_opened",
                message="provider circuit opened after consecutive failures",
                severity=IssueSeverity.WARNING,
            )
        )


def _assess_quotes(
    symbols: tuple[QuoteSymbol, ...],
    provider_result: ProviderCollectionResult,
) -> dict[str, ReconciledQuote]:
    """Assign a single-source quality state for each requested symbol."""
    output: dict[str, ReconciledQuote] = {}
    for symbol in symbols:
        quote = provider_result.valid_quotes.get(symbol.ts_code)
        issues = provider_result.quote_issues.get(symbol.ts_code, ())
        reason: tuple[str, ...]
        if quote is not None:
            state = DataQualityState.COMPLETE
            reason = ()
        elif any(issue.code == "stale_trade_date" for issue in issues):
            state = DataQualityState.STALE
            reason = ("Tencent quote failed the expected trade-date check",)
        else:
            state = DataQualityState.BLOCKED
            reason = ("Tencent did not provide a validated quote",)
        output[symbol.ts_code] = ReconciledQuote(
            symbol=symbol,
            state=state,
            selected_provider=provider_result.provider if quote is not None else None,
            selected_quote=quote,
            reasons=reason,
        )
    return output
