"""Provider parser and normalized field contract tests."""

from __future__ import annotations

from decimal import Decimal

from dawnwatcher.domain import QuoteProvider, QuoteSymbol
from dawnwatcher.providers.tencent import TencentQuoteAdapter
from tests.quote_samples import raw_batch, tencent_line


def test_tencent_parser_normalizes_lots_to_shares() -> None:
    symbol = QuoteSymbol.parse("600000.SH")
    batch = raw_batch(QuoteProvider.TENCENT, symbol, tencent_line().encode("gb18030"))

    quotes, issues = TencentQuoteAdapter().parse(batch)

    assert issues == ()
    quote = quotes["600000.SH"]
    assert quote.latest == Decimal("9.00")
    assert quote.volume_shares == 52_836_400
    assert quote.amount_cny == Decimal("475964884")
    assert quote.bid1_volume_shares == 188_700
    assert quote.ask1_volume_shares == 105_300
    assert quote.volume_precision_shares == 100
    assert quote.raw_field_count == 88


def test_parser_fails_closed_when_provider_schema_is_short() -> None:
    symbol = QuoteSymbol.parse("600000.SH")
    body = b'v_sh600000="1~too~short";'

    quotes, issues = TencentQuoteAdapter().parse(raw_batch(QuoteProvider.TENCENT, symbol, body))

    assert quotes == {}
    assert issues[0].code == "parse_error"
    assert "schema changed" in issues[0].message
