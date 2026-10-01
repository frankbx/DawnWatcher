"""Small, bounded Tushare Pro client for unadjusted daily bars and factors."""

from __future__ import annotations

import time
from datetime import date
from typing import Any

import httpx


class TushareDailyError(RuntimeError):
    """A daily endpoint failed or returned an invalid response."""


class TushareDailyClient:
    """Fetch one complete market date per endpoint; never log the token."""

    ENDPOINTS = {"daily", "fund_daily", "adj_factor", "fund_adj"}

    def __init__(
        self,
        *,
        token: str,
        api_url: str,
        timeout_seconds: float = 20.0,
        client: httpx.Client | None = None,
    ) -> None:
        if not token.strip():
            raise ValueError("a non-empty Tushare token is required")
        self._token = token.strip()
        self._api_url = api_url
        self._client = client or httpx.Client(timeout=timeout_seconds, follow_redirects=False)
        self._owns_client = client is None

    def __enter__(self) -> TushareDailyClient:
        return self

    def __exit__(self, *_args: object) -> None:
        if self._owns_client:
            self._client.close()

    def fetch(self, endpoint: str, trade_date: date) -> list[dict[str, Any]]:
        """Return named rows, retrying only transient transport/server failures."""
        if endpoint not in self.ENDPOINTS:
            raise ValueError(f"unsupported Tushare daily endpoint: {endpoint}")
        fields = (
            "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount"
            if endpoint in {"daily", "fund_daily"}
            else "ts_code,trade_date,adj_factor"
        )
        for attempt in range(3):
            try:
                response = self._client.post(
                    self._api_url,
                    json={
                        "api_name": endpoint,
                        "token": self._token,
                        "params": {"trade_date": trade_date.strftime("%Y%m%d")},
                        "fields": fields,
                    },
                )
                response.raise_for_status()
                payload = response.json()
                break
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                transient = not isinstance(exc, httpx.HTTPStatusError) or (
                    exc.response.status_code in {429, 500, 502, 503, 504}
                )
                if attempt == 2 or not transient:
                    raise TushareDailyError(f"{endpoint} request failed: {exc}") from exc
                time.sleep(0.5 * 2**attempt)
            except ValueError as exc:
                raise TushareDailyError(f"{endpoint} returned invalid JSON") from exc
        if not isinstance(payload, dict) or payload.get("code") != 0:
            message = payload.get("msg") if isinstance(payload, dict) else "malformed response"
            raise TushareDailyError(f"{endpoint} rejected: {message}")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise TushareDailyError(f"{endpoint} has no data object")
        columns, items = data.get("fields"), data.get("items")
        required = set(fields.split(","))
        if not isinstance(columns, list) or not required.issubset(columns):
            raise TushareDailyError(f"{endpoint} is missing required fields")
        if not isinstance(items, list):
            raise TushareDailyError(f"{endpoint} has no rows array")
        result: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, list) or len(item) != len(columns):
                raise TushareDailyError(f"{endpoint} has a malformed row")
            result.append(dict(zip(columns, item, strict=True)))
        return result
