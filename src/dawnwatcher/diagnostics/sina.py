"""Sina quote parser used only by the opt-in source comparison diagnostic."""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

from dawnwatcher.domain.quotes import (
    IssueSeverity,
    MarketQuote,
    QuoteIssue,
    QuoteProvider,
    QuoteSymbol,
    RawQuoteBatch,
)
from dawnwatcher.providers.base import QuoteParseError, decimal_field, integer_field

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_RESPONSE_PATTERN = re.compile(r'var hq_str_(?P<code>[a-z]{2}\d{6})="(?P<data>[^"]*)";')


class DiagnosticSinaQuoteAdapter:
    """Parse Sina's comma-separated endpoint without entering production code paths."""

    provider = QuoteProvider.SINA
    encoding = "gb18030"
    minimum_fields = 32

    def build_url(self, symbols: tuple[QuoteSymbol, ...]) -> str:
        codes = ",".join(symbol.provider_code for symbol in symbols)
        return f"https://hq.sinajs.cn/list={codes}"

    def request_headers(self) -> dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0 DawnWatcher/0.1",
            "Referer": "https://finance.sina.com.cn/",
            "Accept": "*/*",
        }

    def parse(self, batch: RawQuoteBatch) -> tuple[dict[str, MarketQuote], tuple[QuoteIssue, ...]]:
        text = batch.body.decode(batch.encoding, errors="strict")
        requested = {symbol.provider_code: symbol for symbol in batch.requested_symbols}
        quotes: dict[str, MarketQuote] = {}
        issues: list[QuoteIssue] = []
        for match in _RESPONSE_PATTERN.finditer(text):
            provider_code = match.group("code")
            symbol = requested.get(provider_code)
            if symbol is None:
                issues.append(
                    QuoteIssue(
                        code="unexpected_symbol",
                        message=f"Sina returned unrequested symbol {provider_code}",
                        severity=IssueSeverity.WARNING,
                    )
                )
                continue
            data = match.group("data")
            if not data:
                issues.append(
                    QuoteIssue(
                        code="empty_quote",
                        message="Sina returned an empty quote payload",
                        severity=IssueSeverity.ERROR,
                        symbol=symbol.ts_code,
                    )
                )
                continue
            try:
                quotes[symbol.ts_code] = self._parse_fields(symbol, data.split(","), batch)
            except (QuoteParseError, ValueError, IndexError) as exc:
                issues.append(
                    QuoteIssue(
                        code="parse_error",
                        message=str(exc),
                        severity=IssueSeverity.ERROR,
                        symbol=symbol.ts_code,
                    )
                )

        returned = set(quotes) | {issue.symbol for issue in issues if issue.symbol is not None}
        for symbol in batch.requested_symbols:
            if symbol.ts_code not in returned:
                issues.append(
                    QuoteIssue(
                        code="missing_symbol",
                        message="Sina response did not contain the requested symbol",
                        severity=IssueSeverity.ERROR,
                        symbol=symbol.ts_code,
                    )
                )
        return quotes, tuple(issues)

    def _parse_fields(
        self,
        symbol: QuoteSymbol,
        fields: list[str],
        batch: RawQuoteBatch,
    ) -> MarketQuote:
        if len(fields) < self.minimum_fields:
            raise QuoteParseError(
                f"Sina schema changed: expected at least {self.minimum_fields} fields, got {len(fields)}"
            )
        quote_at = datetime.strptime(f"{fields[30]} {fields[31]}", "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=_SHANGHAI
        )
        return MarketQuote(
            provider=self.provider,
            symbol=symbol,
            name=fields[0].strip(),
            quote_at=quote_at,
            fetched_at=batch.fetched_at,
            open=decimal_field(fields[1], field_name="open"),
            previous_close=decimal_field(fields[2], field_name="previous_close"),
            latest=decimal_field(fields[3], field_name="latest"),
            high=decimal_field(fields[4], field_name="high"),
            low=decimal_field(fields[5], field_name="low"),
            volume_shares=integer_field(fields[8], field_name="volume_shares"),
            amount_cny=decimal_field(fields[9], field_name="amount_cny"),
            bid1_volume_shares=integer_field(fields[10], field_name="bid1_volume_shares"),
            bid1_price=decimal_field(fields[11], field_name="bid1_price"),
            ask1_volume_shares=integer_field(fields[20], field_name="ask1_volume_shares"),
            ask1_price=decimal_field(fields[21], field_name="ask1_price"),
            volume_precision_shares=1,
            raw_field_count=len(fields),
        )
