"""Quote validation and dual-source reconciliation tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal

from dawnwatcher.domain import DataQualityState, QuoteProvider, QuoteSymbol
from dawnwatcher.domain.quotes import ProviderCollectionResult
from dawnwatcher.providers.sina import SinaQuoteAdapter
from dawnwatcher.providers.tencent import TencentQuoteAdapter
from dawnwatcher.reconciliation.quotes import reconcile_quotes
from dawnwatcher.reconciliation.validation import validate_quote
from tests.quote_samples import raw_batch, sina_line, tencent_line


def test_matching_quotes_are_complete() -> None:
    symbol, sina, tencent = _quotes()
    results = {
        QuoteProvider.SINA: ProviderCollectionResult(
            provider=QuoteProvider.SINA, quotes={symbol.ts_code: sina}
        ),
        QuoteProvider.TENCENT: ProviderCollectionResult(
            provider=QuoteProvider.TENCENT, quotes={symbol.ts_code: tencent}
        ),
    }

    reconciled = reconcile_quotes((symbol,), results)[symbol.ts_code]

    assert reconciled.state is DataQualityState.COMPLETE
    assert reconciled.selected_provider is QuoteProvider.SINA
    assert reconciled.selected_quote is sina


def test_conflicting_price_blocks_dual_source_quote() -> None:
    symbol, sina, tencent = _quotes()
    tencent = replace(tencent, latest=Decimal("9.20"), high=Decimal("9.20"))
    results = {
        QuoteProvider.SINA: ProviderCollectionResult(
            provider=QuoteProvider.SINA, quotes={symbol.ts_code: sina}
        ),
        QuoteProvider.TENCENT: ProviderCollectionResult(
            provider=QuoteProvider.TENCENT, quotes={symbol.ts_code: tencent}
        ),
    }

    reconciled = reconcile_quotes((symbol,), results)[symbol.ts_code]

    assert reconciled.state is DataQualityState.CONFLICTED


def test_one_valid_provider_is_degraded() -> None:
    symbol, sina, _ = _quotes()
    results = {
        QuoteProvider.SINA: ProviderCollectionResult(
            provider=QuoteProvider.SINA, quotes={symbol.ts_code: sina}
        )
    }

    reconciled = reconcile_quotes((symbol,), results)[symbol.ts_code]

    assert reconciled.state is DataQualityState.DEGRADED


def test_wrong_expected_date_is_stale() -> None:
    symbol, sina, tencent = _quotes()
    expected = date(2026, 9, 25)
    results = {
        QuoteProvider.SINA: ProviderCollectionResult(
            provider=QuoteProvider.SINA,
            quotes={symbol.ts_code: sina},
            quote_issues={symbol.ts_code: validate_quote(sina, expected_trade_date=expected)},
        ),
        QuoteProvider.TENCENT: ProviderCollectionResult(
            provider=QuoteProvider.TENCENT,
            quotes={symbol.ts_code: tencent},
            quote_issues={symbol.ts_code: validate_quote(tencent, expected_trade_date=expected)},
        ),
    }

    reconciled = reconcile_quotes((symbol,), results)[symbol.ts_code]

    assert reconciled.state is DataQualityState.STALE


def _quotes():
    symbol = QuoteSymbol.parse("600000.SH")
    sina = SinaQuoteAdapter().parse(
        raw_batch(QuoteProvider.SINA, symbol, sina_line().encode("gb18030"))
    )[0][symbol.ts_code]
    tencent = TencentQuoteAdapter().parse(
        raw_batch(QuoteProvider.TENCENT, symbol, tencent_line().encode("gb18030"))
    )[0][symbol.ts_code]
    return symbol, sina, tencent
