"""Tencent batch quote adapter."""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

from regimebeacon.domain.quotes import (
    IssueSeverity,
    MarketQuote,
    QuoteIssue,
    QuoteProvider,
    QuoteSymbol,
    RawQuoteBatch,
)
from regimebeacon.providers.base import QuoteParseError, decimal_field, integer_field

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_RESPONSE_PATTERN = re.compile(r'v_(?P<code>[a-z]{2}\d{6})="(?P<data>[^"]*)";')


class TencentQuoteAdapter:
    """Parse the tilde-separated Tencent quote protocol."""

    provider = QuoteProvider.TENCENT
    encoding = "gb18030"
    minimum_fields = 38

    def build_url(self, symbols: tuple[QuoteSymbol, ...]) -> str:
        codes = ",".join(symbol.provider_code for symbol in symbols)
        return f"https://qt.gtimg.cn/q={codes}"

    def request_headers(self) -> dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0 RegimeBeacon/0.1",
            "Referer": "https://gu.qq.com/",
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
                        message=f"Tencent returned unrequested symbol {provider_code}",
                        severity=IssueSeverity.WARNING,
                    )
                )
                continue
            data = match.group("data")
            if not data:
                issues.append(
                    QuoteIssue(
                        code="empty_quote",
                        message="Tencent returned an empty quote payload",
                        severity=IssueSeverity.ERROR,
                        symbol=symbol.ts_code,
                    )
                )
                continue
            fields = data.split("~")
            try:
                quotes[symbol.ts_code] = self._parse_fields(symbol, fields, batch)
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
                        message="Tencent response did not contain the requested symbol",
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
                f"Tencent schema changed: expected at least {self.minimum_fields} fields, got {len(fields)}"
            )
        composite = fields[35].split("/")
        if len(composite) != 3:
            raise QuoteParseError(f"invalid price/volume/amount composite: {fields[35]!r}")
        quote_at = datetime.strptime(fields[30], "%Y%m%d%H%M%S").replace(tzinfo=_SHANGHAI)
        latest = decimal_field(fields[3], field_name="latest")
        composite_latest = decimal_field(composite[0], field_name="composite_latest")
        if latest != composite_latest:
            raise QuoteParseError(
                f"latest price disagrees with composite: {latest} != {composite_latest}"
            )
        return MarketQuote(
            provider=self.provider,
            symbol=symbol,
            name=fields[1].strip(),
            quote_at=quote_at,
            fetched_at=batch.fetched_at,
            open=decimal_field(fields[5], field_name="open"),
            previous_close=decimal_field(fields[4], field_name="previous_close"),
            latest=latest,
            high=decimal_field(fields[33], field_name="high"),
            low=decimal_field(fields[34], field_name="low"),
            volume_shares=integer_field(composite[1], field_name="volume_lots") * 100,
            amount_cny=decimal_field(composite[2], field_name="amount_cny"),
            bid1_price=decimal_field(fields[9], field_name="bid1_price"),
            bid1_volume_shares=integer_field(fields[10], field_name="bid1_lots") * 100,
            ask1_price=decimal_field(fields[19], field_name="ask1_price"),
            ask1_volume_shares=integer_field(fields[20], field_name="ask1_lots") * 100,
            volume_precision_shares=100,
            raw_field_count=len(fields),
        )
