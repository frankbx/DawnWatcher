"""The unattended analysis worker must not lock quote writes during restart."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from scripts import watch_market_analysis


@pytest.mark.parametrize("no_initial_report", [False, True])
def test_safe_startup_skips_full_day_write_transaction(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    no_initial_report: bool,
) -> None:
    class NoopFeishuClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        async def __aenter__(self) -> NoopFeishuClient:
            return self

        async def __aexit__(self, *args: object) -> None:
            pass

    def forbidden_build(*args: object, **kwargs: object) -> None:
        raise AssertionError("startup full-day build must not run")

    monkeypatch.setattr(watch_market_analysis, "build_features", forbidden_build)
    monkeypatch.setattr(watch_market_analysis, "load_pool_members", lambda path: ())
    monkeypatch.setattr(watch_market_analysis, "load_industry_map", lambda path: {})
    monkeypatch.setattr(
        watch_market_analysis.FeishuCredentials, "from_files", lambda *args: object()
    )
    monkeypatch.setattr(watch_market_analysis, "FeishuWebhookClient", NoopFeishuClient)
    past = datetime.now(ZoneInfo("Asia/Shanghai")) - timedelta(hours=1)
    args = argparse.Namespace(
        pool_file=Path("unused-pool.json"),
        industry_map=Path("unused-industry.json"),
        market_benchmark="510300.SH",
        window_minutes=15,
        until=past.isoformat(),
        no_initial_report=no_initial_report,
    )

    asyncio.run(watch_market_analysis.run(args))

    event = json.loads(capsys.readouterr().out)
    assert event["event"] == "market.minute_features.initial_rebuild.skipped"


def test_analysis_skips_non_sealable_minutes_and_lunch() -> None:
    zone = ZoneInfo("Asia/Shanghai")
    day = datetime(2026, 9, 30, 11, 31, 8, tzinfo=zone)

    assert not watch_market_analysis.is_sealable_minute(datetime(2026, 9, 30, 11, 30, tzinfo=zone))
    assert watch_market_analysis.is_sealable_minute(datetime(2026, 9, 30, 13, 0, tzinfo=zone))
    assert watch_market_analysis.is_sealable_minute(datetime(2026, 9, 30, 14, 59, tzinfo=zone))
    assert watch_market_analysis.next_session_build_at(day) == datetime(
        2026, 9, 30, 13, 1, 8, tzinfo=zone
    )
