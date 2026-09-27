"""Minimal Tushare Pro trade_cal client without a pandas dependency."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from dawnwatcher.market import TradingCalendarRecord


class TushareAPIError(RuntimeError):
    """Raised when Tushare rejects or returns an invalid calendar response."""


class TushareCalendarClient:
    """Fetch SSE trading dates through Tushare's documented HTTP API."""

    def __init__(
        self,
        *,
        token: str,
        api_url: str,
        timeout_seconds: float,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not token.strip():
            raise ValueError("a non-empty Tushare token is required")
        self._token = token
        self._api_url = api_url
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds),
            follow_redirects=False,
        )

    async def __aenter__(self) -> TushareCalendarClient:
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc_type, exc, traceback
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def fetch_calendar(
        self,
        *,
        exchange: str,
        start_date: date,
        end_date: date,
    ) -> tuple[TradingCalendarRecord, ...]:
        """Fetch and strictly normalize one inclusive calendar range."""
        if end_date < start_date:
            raise ValueError("calendar end_date cannot be before start_date")
        response = await self._client.post(
            self._api_url,
            json={
                "api_name": "trade_cal",
                "token": self._token,
                "params": {
                    "exchange": exchange,
                    "start_date": start_date.strftime("%Y%m%d"),
                    "end_date": end_date.strftime("%Y%m%d"),
                },
                "fields": "exchange,cal_date,is_open,pretrade_date",
            },
        )
        response.raise_for_status()
        try:
            payload: Any = response.json()
        except ValueError as exc:
            raise TushareAPIError("Tushare returned non-JSON calendar data") from exc
        if not isinstance(payload, dict):
            raise TushareAPIError("Tushare calendar response must be an object")
        code = int(payload.get("code", -1))
        if code != 0:
            message = str(payload.get("msg") or "unknown Tushare error")
            raise TushareAPIError(f"Tushare trade_cal failed with code {code}: {message}")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise TushareAPIError("Tushare calendar response has no data object")
        fields = data.get("fields")
        items = data.get("items")
        if not isinstance(fields, list) or not isinstance(items, list):
            raise TushareAPIError("Tushare calendar data must contain fields and items arrays")
        required = {"exchange", "cal_date", "is_open", "pretrade_date"}
        if not required.issubset(set(fields)):
            raise TushareAPIError("Tushare trade_cal schema is missing required fields")
        indexes = {name: fields.index(name) for name in required}
        records: list[TradingCalendarRecord] = []
        seen_dates: set[date] = set()
        for item in items:
            if not isinstance(item, list) or len(item) < len(fields):
                raise TushareAPIError("Tushare trade_cal returned a malformed row")
            record = _parse_record(item, indexes)
            if record.exchange != exchange:
                raise TushareAPIError(
                    f"Tushare returned exchange {record.exchange}, expected {exchange}"
                )
            if not start_date <= record.cal_date <= end_date:
                raise TushareAPIError("Tushare returned a date outside the requested range")
            if record.cal_date in seen_dates:
                raise TushareAPIError(f"Tushare returned duplicate date {record.cal_date}")
            seen_dates.add(record.cal_date)
            records.append(record)
        expected_dates = {
            start_date + timedelta(days=offset)
            for offset in range((end_date - start_date).days + 1)
        }
        missing_dates = expected_dates - seen_dates
        if missing_dates:
            raise TushareAPIError(
                f"Tushare trade_cal omitted {len(missing_dates)} requested calendar dates"
            )
        return tuple(sorted(records, key=lambda item: item.cal_date))


def read_tushare_token(path: Path) -> str:
    """Read a Tushare token from a dedicated secret file without exposing it."""
    try:
        token = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ValueError(f"cannot read Tushare token file {path}") from exc
    if not token:
        raise ValueError(f"Tushare token file {path} is empty")
    if any(character.isspace() for character in token):
        raise ValueError(f"Tushare token file {path} must contain exactly one token")
    return token


def _parse_record(item: list[Any], indexes: dict[str, int]) -> TradingCalendarRecord:
    exchange = str(item[indexes["exchange"]]).upper()
    cal_date = datetime.strptime(str(item[indexes["cal_date"]]), "%Y%m%d").date()
    raw_is_open = item[indexes["is_open"]]
    if str(raw_is_open) not in {"0", "1"}:
        raise TushareAPIError(f"invalid Tushare is_open value: {raw_is_open!r}")
    raw_pretrade = item[indexes["pretrade_date"]]
    pretrade_date = (
        datetime.strptime(str(raw_pretrade), "%Y%m%d").date()
        if raw_pretrade not in {None, ""}
        else None
    )
    return TradingCalendarRecord(
        exchange=exchange,
        cal_date=cal_date,
        is_open=str(raw_is_open) == "1",
        pretrade_date=pretrade_date,
    )
