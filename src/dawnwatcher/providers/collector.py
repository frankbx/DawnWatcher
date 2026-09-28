"""Concurrent dual-provider market-data collection."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import httpx

from dawnwatcher.config import Settings
from dawnwatcher.domain.quotes import (
    IssueSeverity,
    MarketCollectionResult,
    ProviderCollectionResult,
    QuoteIssue,
    QuoteProvider,
    QuoteSymbol,
    RawArchiveRecord,
    RawQuoteBatch,
)
from dawnwatcher.market import MarketPhase
from dawnwatcher.providers.archive import RawQuoteArchive
from dawnwatcher.providers.base import QuoteProviderAdapter
from dawnwatcher.providers.circuit_breaker import CircuitBreaker
from dawnwatcher.providers.sina import SinaQuoteAdapter
from dawnwatcher.providers.tencent import TencentQuoteAdapter
from dawnwatcher.reconciliation.quotes import reconcile_quotes
from dawnwatcher.reconciliation.validation import validate_quote


class _RequestStartCoordinator:
    """Release corresponding provider batches only after all active sources arrive."""

    def __init__(self, providers: set[QuoteProvider]) -> None:
        self._active_providers = providers.copy()
        self._arrivals: dict[tuple[int, int], set[QuoteProvider]] = {}
        self._condition = asyncio.Condition()

    async def wait(self, provider: QuoteProvider, batch_index: int, stage: int) -> None:
        async with self._condition:
            key = (batch_index, stage)
            self._arrivals.setdefault(key, set()).add(provider)
            await self._condition.wait_for(
                lambda: self._active_providers.issubset(self._arrivals[key])
            )
            self._condition.notify_all()

    async def leave(self, provider: QuoteProvider) -> None:
        async with self._condition:
            self._active_providers.discard(provider)
            self._condition.notify_all()


class MarketDataCollector:
    """Fetch, archive, parse, validate, and reconcile one market snapshot."""

    def __init__(
        self,
        settings: Settings,
        *,
        client: httpx.AsyncClient | None = None,
        adapters: tuple[QuoteProviderAdapter, ...] | None = None,
    ) -> None:
        self.settings = settings
        self._owns_client = client is None
        self.client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(settings.market_request_timeout_seconds),
            follow_redirects=False,
        )
        selected_adapters = adapters or (SinaQuoteAdapter(), TencentQuoteAdapter())
        self.adapters = {adapter.provider: adapter for adapter in selected_adapters}
        self.breakers = {
            provider: CircuitBreaker(
                failure_threshold=settings.circuit_failure_threshold,
                cooldown_seconds=settings.circuit_cooldown_seconds,
            )
            for provider in self.adapters
        }
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
        """Collect both providers concurrently without retries."""
        normalized_symbols = _deduplicate_symbols(symbols)
        if not normalized_symbols:
            raise ValueError("at least one symbol is required")
        started_at = datetime.now(UTC)
        collection_id = str(uuid4())
        key = idempotency_key or f"market:{started_at.isoformat()}:{collection_id}"
        should_archive = self.settings.archive_raw_quotes if archive_raw is None else archive_raw

        start_coordinator = _RequestStartCoordinator(set(self.adapters))
        results = await asyncio.gather(
            *(
                self._collect_provider(
                    adapter,
                    normalized_symbols,
                    expected_trade_date=expected_trade_date,
                    archive_raw=should_archive,
                    start_coordinator=start_coordinator,
                )
                for adapter in self.adapters.values()
            )
        )
        providers = {result.provider: result for result in results}
        reconciled = reconcile_quotes(normalized_symbols, providers)
        return MarketCollectionResult(
            collection_id=collection_id,
            idempotency_key=key,
            requested_symbols=normalized_symbols,
            expected_trade_date=expected_trade_date,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            providers=providers,
            reconciled=reconciled,
            market_phase=market_phase,
        )

    async def _collect_provider(
        self,
        adapter: QuoteProviderAdapter,
        symbols: tuple[QuoteSymbol, ...],
        *,
        expected_trade_date: date | None,
        archive_raw: bool,
        start_coordinator: _RequestStartCoordinator,
    ) -> ProviderCollectionResult:
        breaker = self.breakers[adapter.provider]
        if not breaker.allow_request():
            await start_coordinator.leave(adapter.provider)
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
            for batch_index, chunk in enumerate(_chunks(symbols, self.settings.market_batch_size)):
                batch = await self._fetch_batch(
                    adapter,
                    chunk,
                    start_coordinator=start_coordinator,
                    batch_index=batch_index,
                )
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
                        message="provider returned no quote eligible for reconciliation",
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
        finally:
            await start_coordinator.leave(adapter.provider)

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
        adapter: QuoteProviderAdapter,
        symbols: tuple[QuoteSymbol, ...],
        *,
        start_coordinator: _RequestStartCoordinator,
        batch_index: int,
    ) -> RawQuoteBatch:
        url = adapter.build_url(symbols)
        headers = adapter.request_headers()
        await start_coordinator.wait(adapter.provider, batch_index, stage=0)
        requested_at = datetime.now(UTC)
        await start_coordinator.wait(adapter.provider, batch_index, stage=1)
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
    adapter: QuoteProviderAdapter
    if batch.provider is QuoteProvider.SINA:
        adapter = SinaQuoteAdapter()
    elif batch.provider is QuoteProvider.TENCENT:
        adapter = TencentQuoteAdapter()
    else:  # pragma: no cover - exhaustive for the current enum
        raise ValueError(f"unsupported archived provider: {batch.provider}")
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
