#!/usr/bin/env python3
"""Enqueue one durable holdings-review card per 5-minute trading slot."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from regimebeacon.config import Settings
from regimebeacon.notifications.outbox import enqueue_notification
from regimebeacon.portfolio.holdings import (
    build_holdings_report,
    format_holdings_report,
    load_holdings,
)
from regimebeacon.portfolio.minute_history import load_history_reference
from regimebeacon.storage.database import create_database_engine, create_session_factory


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--holdings-file", type=Path, required=True)
    parser.add_argument("--until", type=datetime.fromisoformat, required=True)
    parser.add_argument("--interval-minutes", type=int, default=5)
    parser.add_argument("--offset-seconds", type=int, default=40)
    parser.add_argument("--benchmark", default="510300.SH")
    parser.add_argument(
        "--history-root",
        type=Path,
        default=Path("data/lake/akshare_sina_full_day"),
    )
    return parser.parse_args()


def _next_due(now: datetime, interval_minutes: int, offset_seconds: int) -> datetime:
    if interval_minutes < 1 or 60 % interval_minutes:
        raise ValueError("interval must be a positive divisor of 60")
    if not 0 <= offset_seconds <= 59:
        raise ValueError("offset seconds must be between 0 and 59")
    current_minute = (now.minute // interval_minutes) * interval_minutes
    current_due = now.replace(minute=current_minute, second=offset_seconds, microsecond=0)
    if now <= current_due:
        return current_due
    next_minute = current_minute + interval_minutes
    if next_minute == 60:
        return now.replace(minute=0, second=offset_seconds, microsecond=0) + timedelta(hours=1)
    return now.replace(minute=next_minute, second=offset_seconds, microsecond=0)


def _reportable_slot(slot: datetime, window_minutes: int) -> bool:
    clock = slot.time()
    boundary = (slot - timedelta(minutes=window_minutes)).time()
    return time(9, 30) <= boundary < clock <= time(11, 30) or time(
        13, 0
    ) <= boundary < clock <= time(15, 0)


def enqueue_once(
    settings: Settings,
    *,
    holdings_file: Path,
    observed_at: datetime,
    slot: datetime,
    interval_minutes: int,
    benchmark: str,
    history_root: Path = Path("data/lake/akshare_sina_full_day"),
) -> str:
    holdings = load_holdings(holdings_file)
    history = load_history_reference(
        history_root,
        symbols=tuple(holding.symbol for holding in holdings),
        window_end=slot,
        window_minutes=interval_minutes,
    )
    engine = create_database_engine(settings)
    try:
        with create_session_factory(engine).begin() as session:
            report = build_holdings_report(
                session,
                holdings=holdings,
                observed_at=observed_at,
                timezone=settings.timezone,
                window_minutes=interval_minutes,
                benchmark_symbol=benchmark,
                history=history,
                window_end=slot,
            )
            key = f"portfolio.holdings_review:{slot.isoformat()}"
            notification = enqueue_notification(
                session,
                idempotency_key=key,
                event_type="portfolio.holdings_review.completed",
                channel="feishu",
                recipient="portfolio_holdings",
                payload={
                    "slot": slot.isoformat(),
                    "expires_at": (slot + timedelta(minutes=interval_minutes)).isoformat(),
                    "data_complete": report.data_complete,
                    "markdown": format_holdings_report(report),
                    "report": report.to_dict(),
                },
            )
            return notification.id
    finally:
        engine.dispose()


async def run(args: argparse.Namespace) -> None:
    settings = Settings()
    zone = ZoneInfo(settings.timezone)
    if args.until.tzinfo is None or args.until.utcoffset() is None:
        raise ValueError("--until must be timezone-aware")
    until = args.until.astimezone(zone)
    load_holdings(args.holdings_file)
    due = _next_due(datetime.now(zone), args.interval_minutes, args.offset_seconds)
    final_due = until + timedelta(seconds=args.offset_seconds)
    while due <= final_due:
        await asyncio.sleep(max(0.0, (due - datetime.now(zone)).total_seconds()))
        slot = due.replace(second=0, microsecond=0)
        if _reportable_slot(slot, args.interval_minutes):
            try:
                notification_id = await asyncio.to_thread(
                    enqueue_once,
                    settings,
                    holdings_file=args.holdings_file,
                    observed_at=datetime.now(zone),
                    slot=slot,
                    interval_minutes=args.interval_minutes,
                    benchmark=args.benchmark,
                    history_root=args.history_root,
                )
                print(
                    json.dumps(
                        {
                            "event": "portfolio.holdings_review.enqueued",
                            "slot": slot.isoformat(),
                            "notification_id": notification_id,
                        }
                    ),
                    flush=True,
                )
            except Exception as exc:
                print(
                    json.dumps(
                        {
                            "event": "portfolio.holdings_review.failed",
                            "slot": slot.isoformat(),
                            "error_type": type(exc).__name__,
                        }
                    ),
                    flush=True,
                )
        due += timedelta(minutes=args.interval_minutes)


if __name__ == "__main__":
    asyncio.run(run(parse_args()))
