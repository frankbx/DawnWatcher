"""Sina/Tencent reliability comparison kept outside the production collector."""

from __future__ import annotations

import asyncio
import json
import time
from collections import Counter
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx

from regimebeacon.config import Settings
from regimebeacon.diagnostics.sina import DiagnosticSinaQuoteAdapter
from regimebeacon.domain.quotes import (
    DataQualityState,
    IssueSeverity,
    MarketQuote,
    ProviderCollectionResult,
    QuoteIssue,
    QuoteProvider,
    QuoteSymbol,
    RawArchiveRecord,
    RawQuoteBatch,
)
from regimebeacon.providers.archive import RawQuoteArchive
from regimebeacon.providers.base import QuoteProviderAdapter
from regimebeacon.providers.circuit_breaker import CircuitBreaker
from regimebeacon.providers.tencent import TencentQuoteAdapter
from regimebeacon.reconciliation.validation import validate_quote

_PRICE_FIELDS = ("latest", "open", "previous_close", "high", "low", "bid1_price", "ask1_price")
_INFORMATIONAL_FIELDS = ("volume_shares", "amount_cny", "quote_time_seconds")


class ComparisonState(StrEnum):
    """The field-level comparison states used by the earlier dual-source design."""

    MATCH = "match"
    NEAR = "near"
    CONFLICT = "conflict"
    INFORMATIONAL = "informational"


class DiagnosticComparisonSummary:
    """Counters accumulated by one afternoon comparison process."""

    def __init__(self) -> None:
        self.cycles = 0
        self.source_failures: Counter[str] = Counter()
        self.inconsistencies = Counter[str]()
        self.quality_counts: Counter[str] = Counter()
        self.provider_runs: Counter[str] = Counter()
        self.provider_successes: Counter[str] = Counter()
        self.provider_requested_quotes: Counter[str] = Counter()
        self.provider_valid_quotes: Counter[str] = Counter()
        self.provider_latencies: dict[str, list[float]] = {"sina": [], "tencent": []}
        self.provider_circuit_opened: Counter[str] = Counter()
        self.provider_circuit_suppressed: Counter[str] = Counter()
        self.start_skews_ms: list[float] = []
        self.conflicts_by_field: Counter[str] = Counter()
        self.conflicted_symbols: set[tuple[str, str]] = set()

    def record(self, cycle: dict[str, Any]) -> None:
        self.cycles += 1
        self.quality_counts.update(cycle.get("quality_counts", {}))
        self.inconsistencies["total"] += int(cycle.get("inconsistency_count", 0))
        self.inconsistencies.update(cycle.get("inconsistency_counts", {}))
        for item in cycle.get("inconsistencies", []):
            if item.get("state") == ComparisonState.CONFLICT.value:
                self.conflicts_by_field[str(item.get("field", "unknown"))] += 1
                self.conflicted_symbols.add((str(cycle.get("cycle_id")), str(item.get("symbol"))))
        for provider, payload in cycle.get("sources", {}).items():
            self.provider_runs[provider] += 1
            requested_count = len(cycle.get("symbols", []))
            self.provider_requested_quotes[provider] += requested_count
            self.provider_valid_quotes[provider] += int(payload.get("valid_quote_count", 0))
            if not payload.get("failure") and payload.get("valid_quote_count", 0) > 0:
                self.provider_successes[provider] += 1
            latency = payload.get("elapsed_ms")
            if isinstance(latency, (int, float)):
                self.provider_latencies.setdefault(provider, []).append(float(latency))
            issue_codes = {issue.get("code") for issue in payload.get("batch_issues", [])}
            if "circuit_opened" in issue_codes:
                self.provider_circuit_opened[provider] += 1
            if "circuit_open" in issue_codes:
                self.provider_circuit_suppressed[provider] += 1
            if payload.get("failure"):
                self.source_failures[provider] += 1
        dispatch_times = [
            payload.get("request_dispatch_ready_at", [None])[0]
            for payload in cycle.get("sources", {}).values()
        ]
        if len(dispatch_times) == 2 and all(isinstance(value, str) for value in dispatch_times):
            first = datetime.fromisoformat(dispatch_times[0])
            second = datetime.fromisoformat(dispatch_times[1])
            self.start_skews_ms.append(abs((first - second).total_seconds()) * 1_000)

    def to_dict(self) -> dict[str, Any]:
        return {
            "cycles": self.cycles,
            "source_failures": dict(self.source_failures),
            "inconsistencies": dict(self.inconsistencies),
            "quality_counts": dict(self.quality_counts),
            "providers": {
                provider: {
                    "run_count": self.provider_runs[provider],
                    "successful_run_count": self.provider_successes[provider],
                    "successful_run_rate_pct": _percentage(
                        self.provider_successes[provider], self.provider_runs[provider]
                    ),
                    "requested_quote_count": self.provider_requested_quotes[provider],
                    "valid_quote_count": self.provider_valid_quotes[provider],
                    "valid_quote_rate_pct": _percentage(
                        self.provider_valid_quotes[provider],
                        self.provider_requested_quotes[provider],
                    ),
                    "latency_ms": _distribution(self.provider_latencies.get(provider, [])),
                    "circuit_opened_count": self.provider_circuit_opened[provider],
                    "circuit_suppressed_count": self.provider_circuit_suppressed[provider],
                }
                for provider in ("sina", "tencent")
            },
            "request_start_skew_ms": _distribution(self.start_skews_ms),
            "conflicts_by_field": dict(self.conflicts_by_field),
            "conflicted_field_count": sum(self.conflicts_by_field.values()),
            "conflicted_symbol_cycle_count": len(self.conflicted_symbols),
        }


class DiagnosticComparisonRunner:
    """Fetch both public quote endpoints concurrently and append diagnostic JSONL logs."""

    def __init__(
        self,
        settings: Settings,
        symbols: tuple[QuoteSymbol, ...],
        *,
        expected_trade_date: date | None = None,
        report_directory: Path | None = None,
        client: httpx.AsyncClient | None = None,
        archive_raw: bool = True,
    ) -> None:
        if not symbols:
            raise ValueError("at least one symbol is required")
        self.settings = settings
        self.symbols = symbols
        self.expected_trade_date = expected_trade_date
        self.archive_raw = archive_raw
        self._owns_client = client is None
        self.client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(settings.market_request_timeout_seconds),
            follow_redirects=False,
        )
        self.adapters: tuple[QuoteProviderAdapter, ...] = (
            DiagnosticSinaQuoteAdapter(),
            TencentQuoteAdapter(),
        )
        self.breakers = {
            adapter.provider: CircuitBreaker(
                failure_threshold=settings.circuit_failure_threshold,
                cooldown_seconds=settings.circuit_cooldown_seconds,
            )
            for adapter in self.adapters
        }
        self.archive = RawQuoteArchive(settings.data_dir / "raw" / "quotes")
        day = datetime.now(UTC).astimezone(ZoneInfo(settings.timezone)).date().isoformat()
        self.report_directory = report_directory or settings.data_dir / "reports" / (
            f"afternoon-stability-{day}"
        )
        self.report_directory.mkdir(parents=True, exist_ok=True)
        self.comparison_log = self.report_directory / "compare.jsonl"
        self.inconsistency_log = self.report_directory / "inconsistencies.jsonl"
        self.comparison_log.touch(exist_ok=True)
        self.inconsistency_log.touch(exist_ok=True)
        self.summary = DiagnosticComparisonSummary()

    async def __aenter__(self) -> DiagnosticComparisonRunner:
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc_type, exc, traceback
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def collect_once(self, run_number: int) -> dict[str, Any]:
        """Run one synchronized two-source request and append its logs."""
        cycle_id = str(uuid4())
        started_at = datetime.now(UTC)
        results = await asyncio.gather(
            *(
                self._collect_provider(adapter, self.breakers[adapter.provider])
                for adapter in self.adapters
            )
        )
        by_provider = {result.provider: result for result in results}
        inconsistencies, quality_counts = _compare_results(
            self.symbols,
            by_provider.get(QuoteProvider.SINA),
            by_provider.get(QuoteProvider.TENCENT),
            cycle_id=cycle_id,
            run_number=run_number,
        )
        finished_at = datetime.now(UTC)
        cycle = {
            "event": "market.source_comparison.completed",
            "cycle_id": cycle_id,
            "run_number": run_number,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "expected_trade_date": (
                self.expected_trade_date.isoformat() if self.expected_trade_date else None
            ),
            "symbols": [symbol.ts_code for symbol in self.symbols],
            "sources": {result.provider.value: _source_summary(result) for result in results},
            "quality_counts": dict(quality_counts),
            "inconsistency_count": len(inconsistencies),
            "inconsistency_counts": dict(Counter(item["state"] for item in inconsistencies)),
            "inconsistencies": inconsistencies,
        }
        self._append_json(self.comparison_log, cycle)
        for item in inconsistencies:
            self._append_json(self.inconsistency_log, item)
        self.summary.record(cycle)
        return cycle

    async def _collect_provider(
        self,
        adapter: QuoteProviderAdapter,
        breaker: CircuitBreaker,
    ) -> ProviderCollectionResult:
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
        quotes: dict[str, MarketQuote] = {}
        quote_issues: dict[str, list[QuoteIssue]] = {}
        batch_issues: list[QuoteIssue] = []
        archives: list[RawArchiveRecord] = []
        dispatch: list[datetime] = []
        try:
            for chunk in _chunks(self.symbols, self.settings.market_batch_size):
                batch = await self._fetch_batch(adapter, chunk)
                dispatch.append(batch.requested_at)
                if self.archive_raw:
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
                    validate_quote(quote, expected_trade_date=self.expected_trade_date)
                )
            valid_count = sum(
                not any(
                    issue.severity is IssueSeverity.ERROR for issue in quote_issues.get(symbol, ())
                )
                for symbol in quotes
            )
            if valid_count == 0:
                batch_issues.append(
                    QuoteIssue(
                        code="no_valid_quotes",
                        message=f"{adapter.provider.value} returned no validated quote",
                        severity=IssueSeverity.ERROR,
                    )
                )
                breaker.record_failure()
            else:
                breaker.record_success()
        except (httpx.HTTPError, UnicodeError, ValueError, OSError) as exc:
            breaker.record_failure()
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
            request_dispatch_ready_at=tuple(dispatch),
            elapsed_ms=(time.perf_counter() - started) * 1_000,
            circuit_state=breaker.state.value,
        )

    async def _fetch_batch(
        self,
        adapter: QuoteProviderAdapter,
        symbols: tuple[QuoteSymbol, ...],
    ) -> RawQuoteBatch:
        url = adapter.build_url(symbols)
        requested_at = datetime.now(UTC)
        started = time.perf_counter()
        response = await self.client.get(url, headers=adapter.request_headers())
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

    @staticmethod
    def _append_json(path: Path, payload: dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
            handle.write("\n")


def _compare_results(
    symbols: tuple[QuoteSymbol, ...],
    sina_result: ProviderCollectionResult | None,
    tencent_result: ProviderCollectionResult | None,
    *,
    cycle_id: str,
    run_number: int,
) -> tuple[list[dict[str, Any]], Counter[str]]:
    sina_quotes = sina_result.valid_quotes if sina_result else {}
    tencent_quotes = tencent_result.valid_quotes if tencent_result else {}
    quality_counts: Counter[str] = Counter()
    inconsistencies: list[dict[str, Any]] = []
    for symbol in symbols:
        sina = sina_quotes.get(symbol.ts_code)
        tencent = tencent_quotes.get(symbol.ts_code)
        if sina is None and tencent is None:
            state = (
                DataQualityState.STALE.value
                if _has_stale_issue(symbol.ts_code, sina_result, tencent_result)
                else DataQualityState.BLOCKED.value
            )
            quality_counts[state] += 1
            inconsistencies.append(
                _availability_issue(
                    cycle_id, run_number, symbol, state, sina_result, tencent_result
                )
            )
            continue
        if sina is None or tencent is None:
            state = DataQualityState.DEGRADED.value
            quality_counts[state] += 1
            available = "sina" if sina is not None else "tencent"
            inconsistencies.append(
                {
                    "event": "market.source_comparison.inconsistency",
                    "cycle_id": cycle_id,
                    "run_number": run_number,
                    "symbol": symbol.ts_code,
                    "field": "availability",
                    "state": state,
                    "sina_value": "available" if sina is not None else "missing",
                    "tencent_value": "available" if tencent is not None else "missing",
                    "reason": f"only {available} supplied a validated quote",
                }
            )
            continue

        comparisons = [
            _compare_price(field, getattr(sina, field), getattr(tencent, field))
            for field in _PRICE_FIELDS
        ]
        comparisons.extend(
            _compare_informational(field, getattr(sina, field), getattr(tencent, field))
            for field in _INFORMATIONAL_FIELDS[:2]
        )
        comparisons.append(
            _compare_informational(
                "quote_time_seconds",
                int(sina.quote_at.timestamp()),
                int(tencent.quote_at.timestamp()),
            )
        )
        if sina.quote_at.date() != tencent.quote_at.date():
            quality = DataQualityState.CONFLICTED.value
            inconsistencies.append(
                _comparison_issue(
                    cycle_id,
                    run_number,
                    symbol,
                    "quote_date",
                    ComparisonState.CONFLICT.value,
                    str(sina.quote_at.date()),
                    str(tencent.quote_at.date()),
                    "providers report different quote dates",
                )
            )
        elif any(item["state"] == ComparisonState.CONFLICT.value for item in comparisons):
            quality = DataQualityState.CONFLICTED.value
        elif any(item["state"] == ComparisonState.NEAR.value for item in comparisons):
            quality = DataQualityState.NEAR.value
        else:
            quality = DataQualityState.COMPLETE.value
        quality_counts[quality] += 1
        for item in comparisons:
            if item["state"] in {ComparisonState.NEAR.value, ComparisonState.CONFLICT.value}:
                inconsistencies.append(
                    {
                        "event": "market.source_comparison.inconsistency",
                        "cycle_id": cycle_id,
                        "run_number": run_number,
                        "symbol": symbol.ts_code,
                        **item,
                    }
                )
        if sina.name != tencent.name:
            inconsistencies.append(
                _comparison_issue(
                    cycle_id,
                    run_number,
                    symbol,
                    "name",
                    "informational",
                    sina.name,
                    tencent.name,
                    "provider security names differ",
                )
            )
    return inconsistencies, quality_counts


def _compare_price(field: str, left: Decimal, right: Decimal) -> dict[str, str]:
    difference = abs(left - right)
    reference = max(abs(left), abs(right))
    match_tolerance = max(Decimal("0.01"), reference * Decimal("0.0002"))
    near_tolerance = max(Decimal("0.03"), reference * Decimal("0.0005"))
    if difference <= match_tolerance:
        state = ComparisonState.MATCH.value
    elif difference <= near_tolerance:
        state = ComparisonState.NEAR.value
    else:
        state = ComparisonState.CONFLICT.value
    return {
        "field": field,
        "state": state,
        "sina_value": str(left),
        "tencent_value": str(right),
        "absolute_difference": str(difference),
    }


def _compare_informational(field: str, left: Any, right: Any) -> dict[str, str]:
    return {
        "field": field,
        "state": ComparisonState.INFORMATIONAL.value,
        "sina_value": str(left),
        "tencent_value": str(right),
        "absolute_difference": str(abs(left - right)),
    }


def _comparison_issue(
    cycle_id: str,
    run_number: int,
    symbol: QuoteSymbol,
    field: str,
    state: str,
    sina_value: str,
    tencent_value: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "event": "market.source_comparison.inconsistency",
        "cycle_id": cycle_id,
        "run_number": run_number,
        "symbol": symbol.ts_code,
        "field": field,
        "state": state,
        "sina_value": sina_value,
        "tencent_value": tencent_value,
        "reason": reason,
    }


def _availability_issue(
    cycle_id: str,
    run_number: int,
    symbol: QuoteSymbol,
    state: str,
    sina_result: ProviderCollectionResult | None,
    tencent_result: ProviderCollectionResult | None,
) -> dict[str, Any]:
    return {
        "event": "market.source_comparison.inconsistency",
        "cycle_id": cycle_id,
        "run_number": run_number,
        "symbol": symbol.ts_code,
        "field": "availability",
        "state": state,
        "sina_value": "missing",
        "tencent_value": "missing",
        "reason": "neither source supplied a validated quote",
        "sina_issues": _issues_for_symbol(sina_result, symbol.ts_code),
        "tencent_issues": _issues_for_symbol(tencent_result, symbol.ts_code),
    }


def _issues_for_symbol(
    result: ProviderCollectionResult | None, symbol: str
) -> list[dict[str, Any]]:
    if result is None:
        return []
    return [issue.to_dict() for issue in result.quote_issues.get(symbol, ())] + [
        issue.to_dict() for issue in result.batch_issues
    ]


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


def _source_summary(result: ProviderCollectionResult) -> dict[str, Any]:
    payload = result.to_summary()
    payload["failure"] = any(issue.severity is IssueSeverity.ERROR for issue in result.batch_issues)
    payload["quote_issues"] = {
        symbol: [issue.to_dict() for issue in issues]
        for symbol, issues in result.quote_issues.items()
    }
    return payload


def _chunks(symbols: tuple[QuoteSymbol, ...], size: int) -> tuple[tuple[QuoteSymbol, ...], ...]:
    return tuple(symbols[index : index + size] for index in range(0, len(symbols), size))


def _percentage(numerator: int, denominator: int) -> float:
    return round(numerator / denominator * 100, 3) if denominator else 0.0


def _distribution(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "average": None, "p50": None, "p95": None, "max": None}
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "average": round(sum(ordered) / len(ordered), 3),
        "p50": round(_percentile(ordered, 0.50), 3),
        "p95": round(_percentile(ordered, 0.95), 3),
        "max": round(ordered[-1], 3),
    }


def _percentile(ordered: list[float], quantile: float) -> float:
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction
