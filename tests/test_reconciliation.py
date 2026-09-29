"""Single-source quote validation tests."""

from __future__ import annotations

from datetime import date

from regimebeacon.domain import QuoteProvider, QuoteSymbol
from regimebeacon.providers.tencent import TencentQuoteAdapter
from regimebeacon.reconciliation.validation import validate_quote
from tests.quote_samples import raw_batch, tencent_line


def test_tencent_quote_passes_expected_trade_date_validation() -> None:
    symbol = QuoteSymbol.parse("600000.SH")
    quote = TencentQuoteAdapter().parse(
        raw_batch(QuoteProvider.TENCENT, symbol, tencent_line().encode("gb18030"))
    )[0][symbol.ts_code]

    assert validate_quote(quote, expected_trade_date=date(2026, 9, 24)) == ()


def test_tencent_quote_with_wrong_trade_date_is_stale() -> None:
    symbol = QuoteSymbol.parse("600000.SH")
    quote = TencentQuoteAdapter().parse(
        raw_batch(QuoteProvider.TENCENT, symbol, tencent_line().encode("gb18030"))
    )[0][symbol.ts_code]

    issues = validate_quote(quote, expected_trade_date=date(2026, 9, 25))

    assert len(issues) == 1
    assert issues[0].code == "stale_trade_date"
