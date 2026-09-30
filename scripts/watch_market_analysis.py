#!/usr/bin/env python3
"""Build minute features and push aligned 15-minute market overview cards."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, time, timedelta
from pathlib import Path
from time import perf_counter
from typing import Any
from zoneinfo import ZoneInfo

from regimebeacon.analysis import (
    build_market_overview,
    format_market_overview_markdown,
    load_pool_members,
)
from regimebeacon.config import Settings
from regimebeacon.notifications.feishu import (
    FeishuCredentials,
    FeishuWebhookClient,
    build_market_analysis_card,
)
from regimebeacon.storage.database import create_database_engine, create_session_factory
from regimebeacon.storage.minute_features import build_minute_features


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
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
    parser.add_argument("--window-minutes", type=int, default=15)
    parser.add_argument(
        "--until",
        required=True,
        help="Inclusive timezone-aware ISO deadline.",
    )
    parser.add_argument(
        "--no-initial-report",
        action="store_true",
        help="Retained for compatibility; safe startup without a full-day rebuild is now the default.",
    )
    parser.add_argument(
        "--rebuild-on-start",
        action="store_true",
        help="Explicit offline repair: rebuild the full day before aligned minute builds.",
    )
    return parser.parse_args()


def parse_aware(value: str, *, label: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{label} must include a timezone offset")
    return parsed


def load_industry_map(path: Path) -> dict[str, str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in payload.items()
    ):
        raise ValueError("industry map must be a string-to-string JSON object")
    return payload


def build_features(
    settings: Settings,
    *,
    trade_date: datetime,
    market_benchmark: str,
    industry_map: dict[str, str],
    minute_start: datetime | None,
) -> dict[str, Any]:
    engine = create_database_engine(settings)
    try:
        with create_session_factory(engine).begin() as session:
            report = build_minute_features(
                session,
                trade_date=trade_date.date(),
                timezone=settings.timezone,
                expected_interval_seconds=settings.market_poll_interval_seconds,
                market_benchmark_symbol=market_benchmark,
                industry_benchmarks=industry_map,
                relative_volume_lookback_days=20,
                relative_volume_minimum_history_days=5,
                minute_start=minute_start,
            )
        return report.to_dict()
    finally:
        engine.dispose()


async def send_overview(
    client: FeishuWebhookClient,
    settings: Settings,
    *,
    observed_at: datetime,
    members: tuple[Any, ...],
    market_benchmark: str,
    window_minutes: int,
) -> dict[str, Any]:
    build_started = perf_counter()
    engine = create_database_engine(settings)
    try:
        with create_session_factory(engine)() as session:
            overview = build_market_overview(
                session,
                members=members,
                observed_at=observed_at,
                timezone=settings.timezone,
                window_minutes=window_minutes,
                market_benchmark_symbol=market_benchmark,
            )
    finally:
        engine.dispose()
    build_duration_ms = round((perf_counter() - build_started) * 1000, 1)
    markdown = format_market_overview_markdown(overview)
    send_started = perf_counter()
    receipt = await client.send_card(
        build_market_analysis_card(
            markdown,
            direction=overview.temperature.label,
            report=overview.to_dict(),
        )
    )
    return {
        "event": "market.analysis.sent",
        "observed_at": observed_at.isoformat(),
        "provider_message_id": receipt.provider_message_id,
        "build_duration_ms": build_duration_ms,
        "delivery_duration_ms": round((perf_counter() - send_started) * 1000, 1),
        "overview": overview.to_dict(),
    }


def next_minute_build_at(now: datetime) -> datetime:
    """Run eight seconds after the boundary so the last 15-second tick is persisted."""
    return now.replace(second=8, microsecond=0) + (
        timedelta(minutes=1) if now.second >= 8 else timedelta()
    )


def is_sealable_minute(value: datetime) -> bool:
    """Only build minutes belonging to a session retained in daily Parquet."""
    clock = value.time()
    return time(9, 30) <= clock < time(11, 30) or time(13, 0) <= clock < time(15, 0)


def next_session_build_at(due: datetime) -> datetime:
    """Jump over opening and lunch instead of doing empty database work each minute."""
    target = due - timedelta(minutes=1)
    clock = target.time()
    if clock < time(9, 30):
        return due.replace(hour=9, minute=31, second=8, microsecond=0)
    if time(11, 30) <= clock < time(13, 0):
        return due.replace(hour=13, minute=1, second=8, microsecond=0)
    return due


async def run(args: argparse.Namespace) -> None:
    settings = Settings()
    zone = ZoneInfo(settings.timezone)
    until = parse_aware(args.until, label="--until").astimezone(zone)
    if args.window_minutes < 1 or 60 % args.window_minutes != 0:
        raise ValueError("--window-minutes must be a positive divisor of 60")
    members = load_pool_members(args.pool_file)
    industry_map = load_industry_map(args.industry_map)
    credentials = FeishuCredentials.from_files(
        settings.feishu_webhook_file,
        settings.feishu_signing_secret_file,
    )
    now = datetime.now(zone)
    rebuild_on_start = bool(getattr(args, "rebuild_on_start", False))
    if rebuild_on_start and args.no_initial_report:
        raise ValueError("--rebuild-on-start and --no-initial-report are mutually exclusive")
    if not rebuild_on_start:
        # A full-day write transaction while the quote watcher is active can
        # block its SQLite inserts. The post-session sealer does the catch-up.
        print(json.dumps({"event": "market.minute_features.initial_rebuild.skipped"}), flush=True)
    else:
        initial_features = await asyncio.to_thread(
            build_features,
            settings,
            trade_date=now,
            market_benchmark=args.market_benchmark,
            industry_map=industry_map,
            minute_start=None,
        )
        print(
            json.dumps(
                {"event": "market.minute_features.initialized", "report": initial_features},
                ensure_ascii=False,
            ),
            flush=True,
        )
    async with FeishuWebhookClient(
        credentials,
        timeout_seconds=settings.feishu_request_timeout_seconds,
    ) as client:
        if rebuild_on_start:
            payload = await send_overview(
                client,
                settings,
                observed_at=datetime.now(zone),
                members=members,
                market_benchmark=args.market_benchmark,
                window_minutes=args.window_minutes,
            )
            print(json.dumps(payload, ensure_ascii=False), flush=True)

        due = next_minute_build_at(datetime.now(zone))
        final_due = until.replace(second=8, microsecond=0)
        while due <= final_due:
            next_session_due = next_session_build_at(due)
            if next_session_due != due:
                print(
                    json.dumps(
                        {
                            "event": "market.minute_features.paused",
                            "resume_at": next_session_due.isoformat(),
                        }
                    ),
                    flush=True,
                )
                due = next_session_due
                continue
            await asyncio.sleep(max(0.0, (due - datetime.now(zone)).total_seconds()))
            target_minute = due.replace(second=0, microsecond=0) - timedelta(minutes=1)
            if not is_sealable_minute(target_minute):
                break
            try:
                started = perf_counter()
                feature_report = await asyncio.to_thread(
                    build_features,
                    settings,
                    trade_date=due,
                    market_benchmark=args.market_benchmark,
                    industry_map=industry_map,
                    minute_start=target_minute,
                )
                print(
                    json.dumps(
                        {
                            "event": "market.minute_features.completed",
                            "minute_start": target_minute.isoformat(),
                            "duration_ms": round((perf_counter() - started) * 1000, 1),
                            "report": feature_report,
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
                if due.minute % args.window_minutes == 0:
                    payload = await send_overview(
                        client,
                        settings,
                        observed_at=datetime.now(zone),
                        members=members,
                        market_benchmark=args.market_benchmark,
                        window_minutes=args.window_minutes,
                    )
                    print(json.dumps(payload, ensure_ascii=False), flush=True)
            except Exception as exc:
                print(
                    json.dumps(
                        {
                            "event": "market.analysis.failed",
                            "observed_at": datetime.now(zone).isoformat(),
                            "error_type": type(exc).__name__,
                            "error": str(exc),
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
            next_due = due + timedelta(minutes=1)
            resume_due = next_minute_build_at(datetime.now(zone))
            if next_due < resume_due:
                print(
                    json.dumps(
                        {
                            "event": "market.minute_features.backlog_skipped",
                            "from_due": next_due.isoformat(),
                            "resume_at": resume_due.isoformat(),
                        }
                    ),
                    flush=True,
                )
            due = max(next_due, resume_due)


def main() -> None:
    asyncio.run(run(parse_args()))


if __name__ == "__main__":
    main()
