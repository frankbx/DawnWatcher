"""Canonical Tushare security-code tests."""

from __future__ import annotations

import pytest

from dawnwatcher.domain import Exchange, QuoteSymbol
from dawnwatcher.providers.collector import parse_symbols


@pytest.mark.parametrize(
    ("value", "expected", "exchange", "provider_code"),
    [
        ("600000.SH", "600000.SH", Exchange.SSE, "sh600000"),
        ("000001.sz", "000001.SZ", Exchange.SZSE, "sz000001"),
        ("920001.BJ", "920001.BJ", Exchange.BSE, "bj920001"),
        ("sh600000", "600000.SH", Exchange.SSE, "sh600000"),
        ("000001", "000001.SZ", Exchange.SZSE, "sz000001"),
        ("000001.SH", "000001.SH", Exchange.SSE, "sh000001"),
        ("510300.SH", "510300.SH", Exchange.SSE, "sh510300"),
        ("399001.SZ", "399001.SZ", Exchange.SZSE, "sz399001"),
        ("159915.SZ", "159915.SZ", Exchange.SZSE, "sz159915"),
    ],
)
def test_symbol_inputs_normalize_to_tushare_format(
    value: str,
    expected: str,
    exchange: Exchange,
    provider_code: str,
) -> None:
    symbol = QuoteSymbol.parse(value)

    assert symbol.ts_code == expected
    assert symbol.exchange is exchange
    assert symbol.provider_code == provider_code


def test_tushare_suffix_must_match_inferred_exchange() -> None:
    with pytest.raises(ValueError, match="disagree"):
        QuoteSymbol.parse("600000.SZ")


def test_legacy_aliases_deduplicate_to_one_tushare_symbol() -> None:
    symbols = parse_symbols(["600000", "sh600000", "600000.SH"])

    assert [symbol.ts_code for symbol in symbols] == ["600000.SH"]
