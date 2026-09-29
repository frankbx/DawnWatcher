#!/usr/bin/env python3
"""Finalize and seal selected trading sessions at safe post-session times."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from regimebeacon.config import Settings
from regimebeacon.storage.database import create_database_engine, create_session_factory
from regimebeacon.storage.minute_features import build_minute_features
from regimebeacon.storage.minute_parquet import (
    MinuteTradingSession,
    load_instrument_metadata,
    merge_minute_day,
    seal_minute_session,
)

_SEAL_TIMES = {
    MinuteTradingSession.MORNING: time(11, 32),
    MinuteTradingSession.AFTERNOON: time(15, 2),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, help="Defaults to local today.")
    parser.add_argument(
        "--sessions",
        nargs="+",
        choices=[item.value for item in MinuteTradingSession],
        default=[item.value for item in MinuteTradingSession],
    )
    parser.add_argument(
        "--pool-file",
        type=Path,
        default=Path("config/stock_pools/initial-v1/pool.json"),
    )
    parser.add_argument(
        "--industry-map",
        type=Path,
        default=Path("config/stock_pools/initial-v1/industry_benchmarks.json"),
    )
    parser.add_argument("--market-benchmark", default="510300.SH")
    parser.add_argument(
        "--output-root",
        type=Path,
        help="Defaults to data/lake/minute_market.",
    )
    return parser.parse_args()


def load_industry_map(path: Path) -> dict[str, str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in payload.items()
    ):
        raise ValueError("industry map must be a string-to-string JSON object")
    return payload


def finalize_and_seal(
    settings: Settings,
    *,
    trade_date: date,
    trading_session: MinuteTradingSession,
    pool_file: Path,
    industry_map_file: Path,
    market_benchmark: str,
    output_root: Path,
    observed_at: datetime,
) -> dict[str, Any]:
    instrument_metadata = load_instrument_metadata(pool_file)
    industry_map = load_industry_map(industry_map_file)
    engine = create_database_engine(settings)
    try:
        factory = create_session_factory(engine)
        with factory.begin() as database_session:
            feature_report = build_minute_features(
                database_session,
                trade_date=trade_date,
                timezone=settings.timezone,
                expected_interval_seconds=settings.market_poll_interval_seconds,
                market_benchmark_symbol=market_benchmark,
                industry_benchmarks=industry_map,
                relative_volume_lookback_days=20,
                relative_volume_minimum_history_days=5,
            )
        with factory() as database_session:
            seal_report = seal_minute_session(
                database_session,
                trade_date=trade_date,
                trading_session=trading_session,
                timezone=settings.timezone,
                output_root=output_root,
                instrument_metadata=instrument_metadata,
                observed_at=observed_at,
            )
    finally:
        engine.dispose()
    return {
        "event": "market.minute_session.sealed",
        "observed_at": observed_at.isoformat(),
        "feature_build": feature_report.to_dict(),
        "seal": seal_report.to_dict(),
    }


async def run(args: argparse.Namespace) -> None:
    settings = Settings()
    zone = ZoneInfo(settings.timezone)
    now = datetime.now(zone)
    trade_date = args.date or now.date()
    if trade_date > now.date():
        raise ValueError("--date cannot be in the future")
    output_root = args.output_root or settings.data_dir / "lake" / "minute_market"
    sessions = sorted(
        {MinuteTradingSession(value) for value in args.sessions},
        key=lambda value: _SEAL_TIMES[value],
    )
    failed = False
    for trading_session in sessions:
        due = datetime.combine(trade_date, _SEAL_TIMES[trading_session], tzinfo=zone)
        await asyncio.sleep(max(0.0, (due - datetime.now(zone)).total_seconds()))
        observed_at = datetime.now(zone)
        seal_succeeded = False
        try:
            payload = await asyncio.to_thread(
                finalize_and_seal,
                settings,
                trade_date=trade_date,
                trading_session=trading_session,
                pool_file=args.pool_file,
                industry_map_file=args.industry_map,
                market_benchmark=args.market_benchmark,
                output_root=output_root,
                observed_at=observed_at,
            )
            seal_succeeded = True
            if not payload["seal"]["complete"]:
                failed = True
                payload["event"] = "market.minute_session.sealed_incomplete"
        except Exception as exc:
            failed = True
            payload = {
                "event": "market.minute_session.seal_failed",
                "observed_at": observed_at.isoformat(),
                "trade_date": trade_date.isoformat(),
                "session": trading_session.value,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        print(json.dumps(payload, ensure_ascii=False), flush=True)
        if trading_session is MinuteTradingSession.AFTERNOON and seal_succeeded:
            try:
                day_report = await asyncio.to_thread(
                    merge_minute_day,
                    trade_date=trade_date,
                    timezone=settings.timezone,
                    output_root=output_root,
                    observed_at=datetime.now(zone),
                )
                day_payload = {
                    "event": "market.minute_day.sealed",
                    "observed_at": datetime.now(zone).isoformat(),
                    "seal": day_report.to_dict(),
                }
                if not day_report.complete:
                    failed = True
                    day_payload["event"] = "market.minute_day.sealed_incomplete"
            except Exception as exc:
                failed = True
                day_payload = {
                    "event": "market.minute_day.seal_failed",
                    "observed_at": datetime.now(zone).isoformat(),
                    "trade_date": trade_date.isoformat(),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            print(json.dumps(day_payload, ensure_ascii=False), flush=True)
    if failed:
        raise SystemExit(1)


def main() -> None:
    asyncio.run(run(parse_args()))


if __name__ == "__main__":
    main()
