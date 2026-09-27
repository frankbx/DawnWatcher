"""End-to-end dual-provider collection, archive replay, and persistence tests."""

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
from tests.quote_samples import sina_line, tencent_line


def test_dual_collection_archives_replays_and_persists(
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

    assert result.reconciled["600000.SH"].state is DataQualityState.COMPLETE
    assert len(result.request_start_skew_ms) == 1
    assert result.request_start_skew_ms[0] < 50
    assert len(result.providers[QuoteProvider.SINA].archives) == 1
    assert len(result.providers[QuoteProvider.TENCENT].archives) == 1
    for provider_result in result.providers.values():
        archive_path = Path(provider_result.archives[0].path)
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
    assert provider_count == 2
    assert reconciled_count == 1
    assert provider_symbols == {"600000.SH"}
    assert reconciled_symbols == {"600000.SH"}


def test_one_batch_supports_fifty_symbols(tmp_path: Path) -> None:
    symbols = [f"{600000 + offset:06d}.SH" for offset in range(50)]
    settings = Settings(data_dir=tmp_path, market_batch_size=50, _env_file=None)

    def handler(request: httpx.Request) -> httpx.Response:
        provider_codes = str(request.url).split("=", maxsplit=1)[1].split(",")
        if request.url.host == "hq.sinajs.cn":
            body = "\n".join(sina_line(code) for code in provider_codes).encode("gb18030")
        else:
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

    assert len(result.providers[QuoteProvider.SINA].valid_quotes) == 50
    assert len(result.providers[QuoteProvider.TENCENT].valid_quotes) == 50
    assert all(item.state is DataQualityState.COMPLETE for item in result.reconciled.values())


def test_each_provider_batch_starts_together_after_slow_response(tmp_path: Path) -> None:
    settings = Settings(data_dir=tmp_path, market_batch_size=1, _env_file=None)

    async def handler(request: httpx.Request) -> httpx.Response:
        provider_code = str(request.url).split("=", maxsplit=1)[1]
        if request.url.host == "hq.sinajs.cn":
            if provider_code == "sh600000":
                await asyncio.sleep(0.1)
            body = sina_line(provider_code).encode("gb18030")
        else:
            body = tencent_line(provider_code).encode("gb18030")
        return httpx.Response(200, content=body)

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            collector = MarketDataCollector(settings, client=client)
            return await collector.collect(
                parse_symbols(["600000.SH", "600001.SH"]),
                expected_trade_date=date(2026, 9, 24),
                archive_raw=False,
            )

    result = asyncio.run(exercise())

    assert len(result.request_start_skew_ms) == 2
    assert max(result.request_start_skew_ms) < 50
    assert all(item.state is DataQualityState.COMPLETE for item in result.reconciled.values())


def test_provider_failure_releases_other_provider_from_start_barrier(tmp_path: Path) -> None:
    settings = Settings(data_dir=tmp_path, market_batch_size=1, _env_file=None)

    def handler(request: httpx.Request) -> httpx.Response:
        provider_code = str(request.url).split("=", maxsplit=1)[1]
        if request.url.host == "hq.sinajs.cn":
            return httpx.Response(500)
        return httpx.Response(200, content=tencent_line(provider_code).encode("gb18030"))

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            collector = MarketDataCollector(settings, client=client)
            return await asyncio.wait_for(
                collector.collect(
                    parse_symbols(["600000.SH", "600001.SH"]),
                    expected_trade_date=date(2026, 9, 24),
                    archive_raw=False,
                ),
                timeout=1,
            )

    result = asyncio.run(exercise())

    assert result.providers[QuoteProvider.SINA].valid_quotes == {}
    assert len(result.providers[QuoteProvider.TENCENT].valid_quotes) == 2
    assert all(item.state is DataQualityState.DEGRADED for item in result.reconciled.values())


def _single_symbol_handler(request: httpx.Request) -> httpx.Response:
    if request.url.host == "hq.sinajs.cn":
        return httpx.Response(200, content=sina_line().encode("gb18030"))
    return httpx.Response(200, content=tencent_line().encode("gb18030"))
