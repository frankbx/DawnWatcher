"""End-to-end Tencent collection, archive replay, and persistence tests."""

from __future__ import annotations

import asyncio
from datetime import date
from pathlib import Path

import httpx
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from dawnwatcher.config import Settings
from dawnwatcher.domain import DataQualityState, QuoteProvider
from dawnwatcher.market import MarketPhase
from dawnwatcher.providers.collector import MarketDataCollector, parse_symbols, replay_archive
from dawnwatcher.storage.market_quotes import persist_market_collection
from dawnwatcher.storage.models import (
    MarketCollectionRun,
    ProviderQuoteSnapshot,
    ReconciledQuoteSnapshot,
)
from tests.quote_samples import tencent_line


def test_tencent_collection_archives_replays_and_persists(
    tmp_path: Path,
    session_factory_fixture: sessionmaker[Session],
) -> None:
    settings = Settings(data_dir=tmp_path / "collector", _env_file=None)

    async def exercise():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_single_symbol_handler)
        ) as client:
            collector = MarketDataCollector(settings, client=client)
            return await collector.collect(
                parse_symbols(["600000.SH"]),
                expected_trade_date=date(2026, 9, 24),
                idempotency_key="quotes:2026-09-24:10:00:00",
                market_phase=MarketPhase.MORNING_CONTINUOUS,
            )

    result = asyncio.run(exercise())

    assert result.provider is QuoteProvider.TENCENT
    assert result.reconciled["600000.SH"].state is DataQualityState.COMPLETE
    assert len(result.provider_result.archives) == 1
    archive_path = Path(result.provider_result.archives[0].path)
    assert archive_path.is_file()
    replayed = replay_archive(archive_path, expected_trade_date=date(2026, 9, 24))
    assert set(replayed.valid_quotes) == {"600000.SH"}

    with session_factory_fixture.begin() as session:
        first = persist_market_collection(session, result)
        duplicate = persist_market_collection(session, result)
        collection_count = session.scalar(select(func.count()).select_from(MarketCollectionRun))
        provider_count = session.scalar(select(func.count()).select_from(ProviderQuoteSnapshot))
        reconciled_count = session.scalar(select(func.count()).select_from(ReconciledQuoteSnapshot))
        provider_symbols = set(session.scalars(select(ProviderQuoteSnapshot.symbol)))
        reconciled_symbols = set(session.scalars(select(ReconciledQuoteSnapshot.symbol)))

    assert duplicate.id == first.id
    assert first.requested_symbols == ["600000.SH"]
    assert first.market_phase is MarketPhase.MORNING_CONTINUOUS
    assert collection_count == 1
    assert provider_count == 1
    assert reconciled_count == 1
    assert provider_symbols == {"600000.SH"}
    assert reconciled_symbols == {"600000.SH"}


def test_one_batch_supports_fifty_symbols(tmp_path: Path) -> None:
    symbols = [f"{600000 + offset:06d}.SH" for offset in range(50)]
    settings = Settings(data_dir=tmp_path, market_batch_size=50, _env_file=None)

    def handler(request: httpx.Request) -> httpx.Response:
        provider_codes = str(request.url).split("=", maxsplit=1)[1].split(",")
        body = "\n".join(tencent_line(code) for code in provider_codes).encode("gb18030")
        return httpx.Response(200, content=body)

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            collector = MarketDataCollector(settings, client=client)
            return await collector.collect(
                parse_symbols(symbols),
                expected_trade_date=date(2026, 9, 24),
                archive_raw=False,
            )

    result = asyncio.run(exercise())

    assert len(result.provider_result.valid_quotes) == 50
    assert all(item.state is DataQualityState.COMPLETE for item in result.reconciled.values())


def test_collection_uses_one_tencent_request_per_batch(tmp_path: Path) -> None:
    settings = Settings(data_dir=tmp_path, market_batch_size=1, _env_file=None)
    request_urls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        request_urls.append(str(request.url))
        provider_code = str(request.url).split("=", maxsplit=1)[1]
        await asyncio.sleep(0.01)
        return httpx.Response(200, content=tencent_line(provider_code).encode("gb18030"))

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            collector = MarketDataCollector(settings, client=client)
            return await collector.collect(
                parse_symbols(["600000.SH", "600001.SH"]),
                expected_trade_date=date(2026, 9, 24),
                archive_raw=False,
            )

    result = asyncio.run(exercise())

    assert len(request_urls) == 2
    assert all(url.startswith("https://qt.gtimg.cn/q=") for url in request_urls)
    assert result.provider_result.provider is QuoteProvider.TENCENT
    assert all(item.state is DataQualityState.COMPLETE for item in result.reconciled.values())


def test_tencent_failure_opens_and_suppresses_circuit(tmp_path: Path) -> None:
    settings = Settings(data_dir=tmp_path, circuit_failure_threshold=1, _env_file=None)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            collector = MarketDataCollector(settings, client=client)
            symbols = parse_symbols(["600000.SH"])
            first = await collector.collect(symbols, archive_raw=False)
            second = await collector.collect(symbols, archive_raw=False)
            return first, second

    first, second = asyncio.run(exercise())
    first_codes = {item.code for item in first.provider_result.batch_issues}
    second_codes = {item.code for item in second.provider_result.batch_issues}

    assert "circuit_opened" in first_codes
    assert "circuit_open" in second_codes
    assert second.reconciled["600000.SH"].state is DataQualityState.BLOCKED


def _single_symbol_handler(request: httpx.Request) -> httpx.Response:
    provider_code = str(request.url).split("=", maxsplit=1)[1]
    return httpx.Response(200, content=tencent_line(provider_code).encode("gb18030"))
