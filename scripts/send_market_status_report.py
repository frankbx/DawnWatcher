#!/usr/bin/env python3
"""Send one or recurring market collection status cards to Feishu."""

from __future__ import annotations

import argparse
import asyncio
import json
import shutil
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select

from regimebeacon.config import Settings
from regimebeacon.notifications.feishu import (
    FeishuCredentials,
    FeishuWebhookClient,
    build_market_status_card,
)
from regimebeacon.ops.monitoring import (
    latest_quote_watcher_heartbeat,
    recent_quote_watcher_stop_at,
    recent_usable_collection_at,
)
from regimebeacon.storage.database import create_database_engine, create_session_factory
from regimebeacon.storage.market_metrics import build_market_metrics_report
from regimebeacon.storage.models import OperationalAlert

_CURRENT_STATUS_WINDOW = timedelta(minutes=15)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start-at",
        required=True,
        help="Beginning of the reporting window as a timezone-aware ISO timestamp.",
    )
    parser.add_argument(
        "--interval-minutes",
        type=int,
        help="Send repeatedly on aligned wall-clock boundaries; omit for one report.",
    )
    parser.add_argument(
        "--until",
        help="Inclusive timezone-aware ISO deadline required for recurring reports.",
    )
    parser.add_argument(
        "--offset-seconds",
        type=int,
        default=0,
        help="Delay recurring sends after each aligned boundary (default: 0).",
    )
    parser.add_argument(
        "--pool-file",
        type=Path,
        default=Path("config/stock_pools/initial-v1/pool.json"),
    )
    return parser.parse_args()


def parse_aware(value: str, *, label: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{label} must include a timezone offset")
    return parsed


def pool_counts(path: Path) -> dict[str, int]:
    document = json.loads(path.read_text(encoding="utf-8"))
    members = document.get("members", [])
    if not isinstance(members, list):
        raise ValueError(f"invalid members list in {path}")
    stocks = sum(
        isinstance(member, dict) and member.get("instrument_type") == "stock" for member in members
    )
    return {"total": len(members), "stocks": stocks, "etfs": len(members) - stocks}


def build_status(
    settings: Settings,
    *,
    start_at: datetime,
    observed_at: datetime,
    counts: dict[str, int],
) -> tuple[str, bool, dict[str, Any]]:
    current_start_at = max(start_at, observed_at - _CURRENT_STATUS_WINDOW)
    engine = create_database_engine(settings)
    try:
        with create_session_factory(engine)() as session:
            report = build_market_metrics_report(
                session,
                start_at=start_at,
                end_at=observed_at + timedelta(microseconds=1),
                expected_interval_seconds=settings.market_poll_interval_seconds,
            )
            current_report = build_market_metrics_report(
                session,
                start_at=current_start_at,
                end_at=observed_at + timedelta(microseconds=1),
                expected_interval_seconds=settings.market_poll_interval_seconds,
            )
            active_alert_keys = list(
                session.scalars(
                    select(OperationalAlert.alert_key).where(OperationalAlert.status == "active")
                )
            )
            heartbeat = latest_quote_watcher_heartbeat(
                session, trade_date=observed_at.date(), observed_at=observed_at
            )
            recent_collection_at = recent_usable_collection_at(
                session,
                trade_date=observed_at.date(),
                observed_at=observed_at,
                within_seconds=settings.heartbeat_stale_seconds,
            )
            recent_stop_at = recent_quote_watcher_stop_at(
                session,
                trade_date=observed_at.date(),
                observed_at=observed_at,
                within_seconds=settings.heartbeat_stale_seconds,
            )
    finally:
        engine.dispose()

    heartbeat_age = (
        max(0.0, (observed_at - heartbeat.heartbeat_at).total_seconds())
        if heartbeat is not None
        else None
    )
    local_clock = observed_at.timetz().replace(tzinfo=None)
    collection_expected = time(9, 30) <= local_clock <= time(11, 30) or time(
        13, 0
    ) <= local_clock < time(14, 57)
    phase_start = time(9, 30) if local_clock < time(12) else time(13)
    phase_start_grace = (
        collection_expected
        and 0
        <= (
            observed_at
            - datetime.combine(observed_at.date(), phase_start, tzinfo=observed_at.tzinfo)
        ).total_seconds()
        <= settings.heartbeat_stale_seconds
    )
    heartbeat_ok = (
        (heartbeat_age is not None and heartbeat_age <= settings.heartbeat_stale_seconds)
        or recent_collection_at is not None
        or recent_stop_at is not None
        or phase_start_grace
        or not collection_expected
    )
    disk = shutil.disk_usage(settings.data_dir.resolve())
    metrics = report["metrics"]
    latency = metrics["latency_ms"]
    quality = report["quality_counts"]
    gaps = report["collection_gaps"]
    historical_quality_ok = bool(
        report["collection_count"] > 0
        and metrics["successful_run_rate_pct"] >= settings.acceptance_min_successful_run_rate_pct
        and metrics["valid_quote_rate_pct"] >= settings.acceptance_min_valid_quote_rate_pct
        and (
            gaps["max_seconds"] is None
            or gaps["max_seconds"] <= settings.acceptance_max_gap_seconds
        )
        and metrics["circuit_opened_count"] == 0
        and metrics["circuit_open_state_run_count"] == 0
    )
    current_metrics = current_report["metrics"]
    current_gaps = current_report["collection_gaps"]
    current_quality_ok = bool(
        not collection_expected
        or (phase_start_grace and current_report["collection_count"] == 0)
        or (
            current_report["collection_count"] > 0
            and current_metrics["successful_run_rate_pct"]
            >= settings.acceptance_min_successful_run_rate_pct
            and current_metrics["valid_quote_rate_pct"]
            >= settings.acceptance_min_valid_quote_rate_pct
            and (
                current_gaps["max_seconds"] is None
                or current_gaps["max_seconds"] <= settings.acceptance_max_gap_seconds
            )
            and current_metrics["circuit_opened_count"] == 0
            and current_metrics["circuit_open_state_run_count"] == 0
        )
    )
    healthy = bool(
        current_quality_ok
        and not active_alert_keys
        and heartbeat_ok
        and disk.free > settings.disk_warning_free_bytes
    )
    status_text = "当前运行正常" if healthy else "当前运行需关注"
    complete = int(quality.get("complete", 0))
    non_complete = sum(int(value) for key, value in quality.items() if key != "complete")
    max_gap = f"{gaps['max_seconds']:.2f} 秒" if gaps["max_seconds"] is not None else "无"
    heartbeat_text = (
        f"{heartbeat_age:.1f} 秒"
        if heartbeat_age is not None and heartbeat_age <= settings.heartbeat_stale_seconds
        else (
            "心跳暂缺，行情仍在更新"
            if recent_collection_at is not None
            else (
                "采集进程重启中"
                if recent_stop_at is not None
                else "开盘启动宽限期"
                if phase_start_grace
                else "当前时段不要求运行"
                if not collection_expected
                else "缺失或过期"
            )
        )
    )
    lines = [
        f"**状态**：{status_text}",
        f"**报告时间**：{observed_at.strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"**统计窗口**：{start_at.strftime('%H:%M:%S')} 至 {observed_at.strftime('%H:%M:%S')}",
        f"**当日数据质量**：{'达标' if historical_quality_ok else '存在历史未达标项（不代表当前故障）'}",
        "",
        f"**监控范围**：{counts['total']} 只（主板股票 {counts['stocks']}，参考 ETF {counts['etfs']}）",
        f"**累计轮次**：{report['collection_count']}，完整轮次率 {percentage(metrics['successful_run_rate_pct'])}",
        f"**有效行情**：{metrics['valid_quote_count']}/{metrics['requested_quote_count']}（{percentage(metrics['valid_quote_rate_pct'])}）",
        f"**健康阈值**：完整轮次 ≥{settings.acceptance_min_successful_run_rate_pct:g}%，有效行情 ≥{settings.acceptance_min_valid_quote_rate_pct:g}%，最大缺口 ≤{settings.acceptance_max_gap_seconds:g} 秒",
        f"**质量状态**：完整 {complete}，非完整 {non_complete}",
        f"**采集延迟**：平均 {milliseconds(latency['average'])}，P95 {milliseconds(latency['p95'])}，最大 {milliseconds(latency['max'])}",
        f"**采集缺口**：{gaps['count']} 次，最大 {max_gap}",
        f"**熔断**：触发 {metrics['circuit_opened_count']} 次，抑制 {metrics['circuit_suppressed_count']} 次",
        f"**运行心跳**：{heartbeat_text}",
        f"**磁盘剩余**：{disk.free / 1024**3:.2f} GiB（{disk.free / disk.total * 100:.1f}%）",
    ]
    details = {
        "healthy": healthy,
        "historical_quality_ok": historical_quality_ok,
        "current_quality_ok": current_quality_ok,
        "active_alert_keys": active_alert_keys,
        "observed_at": observed_at.isoformat(),
        "heartbeat_age_seconds": heartbeat_age,
        "disk_free_bytes": disk.free,
        "report": report,
        "current_report": current_report,
    }
    return "\n".join(lines), healthy, details


def percentage(value: object) -> str:
    return f"{float(value):.2f}%" if isinstance(value, int | float) else "无数据"


def milliseconds(value: object) -> str:
    return f"{float(value):.1f} ms" if isinstance(value, int | float) else "无数据"


async def send_once(
    client: FeishuWebhookClient,
    settings: Settings,
    *,
    start_at: datetime,
    observed_at: datetime,
    counts: dict[str, int],
) -> dict[str, Any]:
    message, healthy, details = build_status(
        settings,
        start_at=start_at,
        observed_at=observed_at,
        counts=counts,
    )
    receipt = await client.send_card(build_market_status_card(message, healthy=healthy))
    return {
        "event": "market.status_report.sent",
        "observed_at": observed_at.isoformat(),
        "healthy": healthy,
        "collection_count": details["report"]["collection_count"],
        "provider_message_id": receipt.provider_message_id,
    }


def next_aligned_time(now: datetime, interval_minutes: int) -> datetime:
    if interval_minutes <= 0 or 60 % interval_minutes != 0:
        raise ValueError("--interval-minutes must be a positive divisor of 60")
    boundary_minute = (now.minute // interval_minutes + 1) * interval_minutes
    if boundary_minute >= 60:
        return now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return now.replace(minute=boundary_minute, second=0, microsecond=0)


async def run(args: argparse.Namespace) -> None:
    settings = Settings()
    local_zone = ZoneInfo(settings.timezone)
    start_at = parse_aware(args.start_at, label="--start-at").astimezone(local_zone)
    counts = pool_counts(args.pool_file)
    credentials = FeishuCredentials.from_files(
        settings.feishu_webhook_file,
        settings.feishu_signing_secret_file,
    )
    until = parse_aware(args.until, label="--until").astimezone(local_zone) if args.until else None
    if (args.interval_minutes is None) != (until is None):
        raise ValueError("--interval-minutes and --until must be supplied together")
    if not 0 <= args.offset_seconds <= 300:
        raise ValueError("--offset-seconds must be between 0 and 300")

    async with FeishuWebhookClient(
        credentials,
        timeout_seconds=settings.feishu_request_timeout_seconds,
    ) as client:
        if args.interval_minutes is None:
            payload = await send_once(
                client,
                settings,
                start_at=start_at,
                observed_at=datetime.now(local_zone),
                counts=counts,
            )
            print(json.dumps(payload, ensure_ascii=False), flush=True)
            return

        due = next_aligned_time(datetime.now(local_zone), args.interval_minutes) + timedelta(
            seconds=args.offset_seconds
        )
        final_due = until + timedelta(seconds=args.offset_seconds) if until is not None else None
        while final_due is not None and due <= final_due:
            delay = max(0.0, (due - datetime.now(local_zone)).total_seconds())
            await asyncio.sleep(delay)
            try:
                payload = await send_once(
                    client,
                    settings,
                    start_at=start_at,
                    observed_at=datetime.now(local_zone),
                    counts=counts,
                )
            except Exception as exc:
                payload = {
                    "event": "market.status_report.failed",
                    "observed_at": datetime.now(local_zone).isoformat(),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            print(json.dumps(payload, ensure_ascii=False), flush=True)
            due += timedelta(minutes=args.interval_minutes)


def main() -> None:
    asyncio.run(run(parse_args()))


if __name__ == "__main__":
    main()
