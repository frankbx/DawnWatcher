"""Tushare trade_cal HTTP contract and local token-file tests."""

from __future__ import annotations

import asyncio
import json
from datetime import date
from pathlib import Path

import httpx
import pytest

from regimebeacon.providers.tushare_calendar import (
    TushareAPIError,
    TushareCalendarClient,
    read_tushare_token,
)


def test_trade_cal_request_and_response_are_strictly_normalized() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        assert payload == {
            "api_name": "trade_cal",
            "token": "secret-token",
            "params": {
                "exchange": "SSE",
                "start_date": "20260924",
                "end_date": "20260925",
            },
            "fields": "exchange,cal_date,is_open,pretrade_date",
        }
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": None,
                "data": {
                    "fields": ["cal_date", "is_open", "exchange", "pretrade_date"],
                    "items": [
                        ["20260925", 0, "SSE", "20260924"],
                        ["20260924", 1, "SSE", "20260923"],
                    ],
                },
            },
        )

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
            async with TushareCalendarClient(
                token="secret-token",
                api_url="https://api.tushare.test",
                timeout_seconds=2,
                client=http_client,
            ) as client:
                return await client.fetch_calendar(
                    exchange="SSE",
                    start_date=date(2026, 9, 24),
                    end_date=date(2026, 9, 25),
                )

    records = asyncio.run(exercise())

    assert [record.cal_date for record in records] == [date(2026, 9, 24), date(2026, 9, 25)]
    assert [record.is_open for record in records] == [True, False]


def test_trade_cal_rejects_incomplete_date_range() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": None,
                "data": {
                    "fields": ["exchange", "cal_date", "is_open", "pretrade_date"],
                    "items": [["SSE", "20260924", 1, "20260923"]],
                },
            },
        )

    async def exercise() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
            client = TushareCalendarClient(
                token="secret-token",
                api_url="https://api.tushare.test",
                timeout_seconds=2,
                client=http_client,
            )
            await client.fetch_calendar(
                exchange="SSE",
                start_date=date(2026, 9, 24),
                end_date=date(2026, 9, 25),
            )

    with pytest.raises(TushareAPIError, match="omitted 1"):
        asyncio.run(exercise())


def test_token_is_read_from_dedicated_file(tmp_path: Path) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("secret-token\n", encoding="utf-8")

    assert read_tushare_token(token_file) == "secret-token"


def test_token_file_rejects_multiple_values(tmp_path: Path) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("first\nsecond\n", encoding="utf-8")

    with pytest.raises(ValueError, match="exactly one token"):
        read_tushare_token(token_file)
