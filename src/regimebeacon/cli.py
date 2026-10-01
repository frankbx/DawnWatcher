"""Command-line entry point for RegimeBeacon."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
from collections.abc import Sequence
from dataclasses import asdict
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from regimebeacon import __version__
from regimebeacon.analysis.daily_query import query_daily_history, summarize_daily_pool
from regimebeacon.config import Settings
from regimebeacon.diagnostics.comparison import DiagnosticComparisonRunner
from regimebeacon.domain.quotes import MarketCollectionResult, QuoteSymbol
from regimebeacon.logging import configure_logging
from regimebeacon.market import MarketPhase, MarketSessionStatus
from regimebeacon.market.gate import TushareTradingSessionGate
from regimebeacon.notifications.feishu import (
    FeishuConfigurationError,
    FeishuCredentials,
    FeishuWebhookClient,
)
from regimebeacon.notifications.worker import NotificationDeliveryWorker
from regimebeacon.ops.daily_acceptance import run_daily_acceptance
from regimebeacon.ops.health import run_startup_checks
from regimebeacon.ops.monitoring import (
    QUOTE_WATCHER_SERVICE,
    run_operational_checks,
    start_runtime_heartbeat,
    stop_runtime_heartbeat,
    touch_runtime_heartbeat,
)
from regimebeacon.ops.recovery import run_startup_recovery
from regimebeacon.providers.collector import MarketDataCollector, parse_symbols, replay_archive
from regimebeacon.providers.tushare_calendar import TushareCalendarClient, read_tushare_token
from regimebeacon.providers.tushare_daily import TushareDailyClient
from regimebeacon.runtime import RuntimeAlreadyRunningError, TradingDayRuntimeSupervisor
from regimebeacon.storage.backup import online_backup
from regimebeacon.storage.daily_lake import (
    list_daily_partitions,
    load_daily_members,
    sync_daily_date,
)
from regimebeacon.storage.database import create_database_engine, create_session_factory
from regimebeacon.storage.market_metrics import build_market_metrics_report
from regimebeacon.storage.market_quotes import persist_market_collection
from regimebeacon.storage.minute_features import build_minute_features, list_minute_features
from regimebeacon.storage.minute_parquet import (
    MinuteTradingSession,
    load_instrument_metadata,
    merge_minute_day,
    seal_minute_session,
)
from regimebeacon.storage.schema import inspect_schema, upgrade_database
from regimebeacon.workflows.interval import FixedIntervalScheduler, IntervalScheduleResult

logger = logging.getLogger(__name__)


def _poll_interval_seconds(value: str) -> float:
    """Parse a safe provider polling interval from the CLI."""
    try:
        interval = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("interval must be a number") from exc
    if not 1.0 <= interval <= 3_600.0:
        raise argparse.ArgumentTypeError("interval must be between 1 and 3600 seconds")
    return interval


def _positive_integer(value: str) -> int:
    """Parse a strictly positive integer from the CLI."""
    try:
        result = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be an integer") from exc
    if result < 1:
        raise argparse.ArgumentTypeError("value must be at least one")
    return result


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level command parser."""
    parser = argparse.ArgumentParser(
        prog="regimebeacon",
        description="Intraday monitoring and post-close review platform.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("doctor", help="Validate local runtime prerequisites.")
    subparsers.add_parser("config", help="Print the effective non-secret configuration.")

    database_parser = subparsers.add_parser("db", help="Manage the local SQLite database.")
    database_commands = database_parser.add_subparsers(dest="database_command", required=True)
    database_commands.add_parser("upgrade", help="Apply all pending schema migrations.")
    database_commands.add_parser("check", help="Check revision, integrity, and PRAGMAs.")
    database_commands.add_parser("recover", help="Recover work interrupted by a crash.")
    backup_parser = database_commands.add_parser(
        "backup", help="Create and verify a consistent online backup."
    )
    backup_parser.add_argument(
        "--output",
        type=Path,
        help="Destination file; defaults to a timestamped file under data/backups.",
    )

    daily_parser = subparsers.add_parser(
        "daily", help="Sync Tushare raw daily bars and separate factors into the data lake."
    )
    daily_commands = daily_parser.add_subparsers(dest="daily_command", required=True)
    daily_sync = daily_commands.add_parser("sync", help="Cache one completed trading date.")
    daily_sync.add_argument("--date", type=date.fromisoformat, required=True)
    daily_sync.add_argument(
        "--pool-file", type=Path, default=Path("config/stock_pools/initial-v1/pool.json")
    )
    daily_sync.add_argument(
        "--holdings-file", type=Path, default=Path("data/private/holdings.json")
    )
    daily_sync.add_argument("--refresh", action="store_true", help="Re-fetch even if complete.")
    daily_backfill = daily_commands.add_parser(
        "backfill", help="Cache an inclusive range of open SSE trading dates."
    )
    daily_backfill.add_argument("--start-date", type=date.fromisoformat, required=True)
    daily_backfill.add_argument("--end-date", type=date.fromisoformat, required=True)
    daily_backfill.add_argument(
        "--pool-file", type=Path, default=Path("config/stock_pools/initial-v1/pool.json")
    )
    daily_backfill.add_argument(
        "--holdings-file", type=Path, default=Path("data/private/holdings.json")
    )
    daily_backfill.add_argument("--refresh", action="store_true")
    daily_backfill.add_argument(
        "--max-trading-days",
        type=_positive_integer,
        default=30,
        help="Safety cap for API calls (default: 30).",
    )
    daily_status = daily_commands.add_parser(
        "status", help="Show SQLite partition validation state."
    )
    daily_status.add_argument("--date", type=date.fromisoformat)
    daily_history = daily_commands.add_parser(
        "history", help="Query raw OHLCV and factor by symbol."
    )
    daily_history.add_argument("--symbol", required=True)
    daily_history.add_argument("--start-date", type=date.fromisoformat, required=True)
    daily_history.add_argument("--end-date", type=date.fromisoformat, required=True)
    daily_summary = daily_commands.add_parser("summary", help="Summarize the cached sample date.")
    daily_summary.add_argument("--date", type=date.fromisoformat, required=True)

    calendar_parser = subparsers.add_parser(
        "calendar", help="Synchronize and inspect the cached Tushare trading calendar."
    )
    calendar_commands = calendar_parser.add_subparsers(dest="calendar_command", required=True)
    sync_parser = calendar_commands.add_parser(
        "sync", help="Fetch an inclusive trade_cal range from Tushare."
    )
    sync_parser.add_argument("--start-date", type=date.fromisoformat)
    sync_parser.add_argument("--end-date", type=date.fromisoformat)
    status_parser = calendar_commands.add_parser(
        "status", help="Classify a time using the local calendar, refreshing if needed."
    )
    status_parser.add_argument(
        "--at",
        type=datetime.fromisoformat,
        help="Timezone-aware ISO timestamp; defaults to now.",
    )

    quote_parser = subparsers.add_parser("quotes", help="Collect or replay market quotes.")
    quote_commands = quote_parser.add_subparsers(dest="quote_command", required=True)
    collect_parser = quote_commands.add_parser("collect", help="Collect one Tencent snapshot.")
    collect_parser.add_argument(
        "symbols", nargs="+", help="Tushare ts_code values such as 600000.SH."
    )
    collect_parser.add_argument(
        "--expected-date",
        type=date.fromisoformat,
        help="Expected quote trade date in YYYY-MM-DD form.",
    )
    collect_parser.add_argument("--idempotency-key", help="Stable persistence key for this cycle.")
    collect_parser.add_argument(
        "--no-archive", action="store_true", help="Do not archive raw provider responses."
    )
    collect_parser.add_argument(
        "--no-persist", action="store_true", help="Do not persist normalized snapshots."
    )
    collect_parser.add_argument(
        "--ignore-market-gate",
        action="store_true",
        help="Diagnostic override: request quotes outside an active auction phase.",
    )
    probe_parser = quote_commands.add_parser(
        "probe-opening",
        help="Fetch an opening-auction Tencent sample without archiving or persisting it.",
    )
    probe_parser.add_argument(
        "symbols", nargs="+", help="Small Tushare-format verification sample."
    )
    watch_parser = quote_commands.add_parser(
        "watch", help="Continuously collect non-overlapping Tencent snapshots."
    )
    watch_parser.add_argument(
        "symbols", nargs="+", help="Tushare ts_code values such as 600000.SH."
    )
    watch_parser.add_argument(
        "--expected-date",
        type=date.fromisoformat,
        help="Expected quote trade date in YYYY-MM-DD form.",
    )
    watch_parser.add_argument(
        "--interval",
        "--interval-seconds",
        dest="interval_seconds",
        type=_poll_interval_seconds,
        help="Seconds between scheduled starts; defaults to configured value (15).",
    )
    watch_parser.add_argument(
        "--max-runs",
        type=_positive_integer,
        help="Stop after this many runs; omitted means run until SIGINT or SIGTERM.",
    )
    watch_parser.add_argument(
        "--no-archive", action="store_true", help="Do not archive raw provider responses."
    )
    watch_parser.add_argument(
        "--no-persist", action="store_true", help="Do not persist normalized snapshots."
    )
    compare_parser = quote_commands.add_parser(
        "compare", help="Diagnostic-only concurrent Sina/Tencent reliability comparison."
    )
    compare_parser.add_argument(
        "symbols", nargs="+", help="Tushare ts_code values such as 600000.SH."
    )
    compare_parser.add_argument(
        "--expected-date",
        type=date.fromisoformat,
        help="Expected quote trade date in YYYY-MM-DD form.",
    )
    compare_parser.add_argument(
        "--interval",
        "--interval-seconds",
        dest="interval_seconds",
        type=_poll_interval_seconds,
        help="Seconds between synchronized requests; defaults to configured value (15).",
    )
    compare_parser.add_argument(
        "--max-runs",
        type=_positive_integer,
        help="Stop after this many comparison cycles; omitted means run until stopped.",
    )
    compare_parser.add_argument(
        "--until",
        type=datetime.fromisoformat,
        help="Timezone-aware ISO timestamp at which the diagnostic run stops.",
    )
    compare_parser.add_argument(
        "--report-dir",
        type=Path,
        help="Directory for compare.jsonl and the separate inconsistencies.jsonl log.",
    )
    compare_parser.add_argument(
        "--no-archive", action="store_true", help="Do not archive raw Sina/Tencent responses."
    )
    compare_parser.add_argument(
        "--ignore-market-gate",
        action="store_true",
        help="Diagnostic override: request outside an active auction phase.",
    )
    replay_parser = quote_commands.add_parser(
        "replay", help="Parse and validate one archived provider response."
    )
    replay_parser.add_argument("archive", type=Path)
    replay_parser.add_argument("--expected-date", type=date.fromisoformat)
    stats_parser = quote_commands.add_parser(
        "stats", help="Summarize Tencent reliability for one local trading date."
    )
    stats_parser.add_argument(
        "--date",
        type=date.fromisoformat,
        help="Local date in YYYY-MM-DD form; defaults to today.",
    )

    feature_parser = subparsers.add_parser(
        "features", help="Build and inspect auditable one-minute market features."
    )
    feature_commands = feature_parser.add_subparsers(dest="feature_command", required=True)
    feature_build = feature_commands.add_parser(
        "build", help="Build minute bars and features from persisted Tencent snapshots."
    )
    feature_build.add_argument(
        "symbols",
        nargs="*",
        help="Optional Tushare ts_code values; omitted means all persisted symbols.",
    )
    feature_build.add_argument(
        "--date",
        type=date.fromisoformat,
        help="Local trading date; defaults to today.",
    )
    feature_build.add_argument(
        "--market-benchmark",
        help="Tushare index or ETF code, for example 000001.SH or 510300.SH.",
    )
    feature_build.add_argument(
        "--industry-map",
        type=Path,
        help="JSON object mapping each stock ts_code to an industry benchmark ts_code.",
    )
    feature_build.add_argument(
        "--lookback-days",
        type=_positive_integer,
        default=20,
        help="Maximum same-minute history used for relative volume (default: 20).",
    )
    feature_build.add_argument(
        "--minimum-history-days",
        type=_positive_integer,
        default=5,
        help="Minimum history required before relative volume is emitted (default: 5).",
    )
    feature_build.add_argument(
        "--interval",
        dest="interval_seconds",
        type=_poll_interval_seconds,
        help="Expected snapshot interval for coverage; defaults to configured value (15).",
    )
    feature_show = feature_commands.add_parser(
        "show", help="Print persisted one-minute features as JSON."
    )
    feature_show.add_argument(
        "--date",
        type=date.fromisoformat,
        help="Local trading date; defaults to today.",
    )
    feature_show.add_argument("--symbol", help="Optional Tushare ts_code filter.")
    feature_seal = feature_commands.add_parser(
        "seal", help="Seal one completed morning or afternoon minute partition to Parquet."
    )
    feature_seal.add_argument(
        "--date",
        type=date.fromisoformat,
        help="Local trading date; defaults to today.",
    )
    feature_seal.add_argument(
        "--session",
        dest="trading_session",
        required=True,
        choices=[item.value for item in MinuteTradingSession],
    )
    feature_seal.add_argument(
        "--pool-file",
        type=Path,
        default=Path("config/stock_pools/initial-v1/pool.json"),
        help="Pool JSON supplying expected symbols and analytical labels.",
    )
    feature_seal.add_argument(
        "--output-root",
        type=Path,
        help="Dataset root; defaults to data/lake/minute_market.",
    )
    feature_merge_day = feature_commands.add_parser(
        "merge-day", help="Merge sealed morning and afternoon partitions into one daily file."
    )
    feature_merge_day.add_argument(
        "--date",
        type=date.fromisoformat,
        help="Local trading date; defaults to today.",
    )
    feature_merge_day.add_argument(
        "--output-root",
        type=Path,
        help="Dataset root; defaults to data/lake/minute_market.",
    )

    monitor_parser = subparsers.add_parser(
        "monitor", help="Check or continuously monitor runtime operational health."
    )
    monitor_commands = monitor_parser.add_subparsers(dest="monitor_command", required=True)
    monitor_check = monitor_commands.add_parser(
        "check", help="Check heartbeats, collection freshness, and disk space once."
    )
    monitor_check.add_argument(
        "--at",
        type=datetime.fromisoformat,
        help="Timezone-aware ISO timestamp; defaults to now.",
    )
    monitor_watch = monitor_commands.add_parser(
        "watch", help="Continuously run operational checks."
    )
    monitor_watch.add_argument(
        "--interval",
        dest="interval_seconds",
        type=_poll_interval_seconds,
        help="Seconds between checks; defaults to configured value (30).",
    )
    monitor_watch.add_argument(
        "--max-runs",
        type=_positive_integer,
        help="Stop after this many checks; omitted means run until stopped.",
    )

    notification_parser = subparsers.add_parser(
        "notifications", help="Deliver durable notification outbox messages."
    )
    notification_commands = notification_parser.add_subparsers(
        dest="notification_command", required=True
    )
    test_parser = notification_commands.add_parser(
        "test", help="Send one direct Card JSON 2.0 message without writing to the outbox."
    )
    test_parser.add_argument("text", help="Message text to place in the Feishu card.")
    deliver_parser = notification_commands.add_parser(
        "deliver", help="Deliver one bounded batch of pending Feishu messages."
    )
    deliver_parser.add_argument(
        "--max-items",
        type=_positive_integer,
        help="Maximum messages; defaults to the configured batch size (20).",
    )
    notification_watch = notification_commands.add_parser(
        "watch", help="Continuously deliver pending Feishu messages."
    )
    notification_watch.add_argument(
        "--interval",
        dest="interval_seconds",
        type=_poll_interval_seconds,
        help="Seconds between outbox polls; defaults to configured value (5).",
    )
    notification_watch.add_argument(
        "--max-items",
        type=_positive_integer,
        help="Maximum messages per poll; defaults to the configured batch size (20).",
    )
    notification_watch.add_argument(
        "--max-runs",
        type=_positive_integer,
        help="Stop after this many polls; omitted means run until stopped.",
    )

    acceptance_parser = subparsers.add_parser(
        "acceptance", help="Validate completed trading-day collection and sealed minute files."
    )
    acceptance_commands = acceptance_parser.add_subparsers(dest="acceptance_command", required=True)
    acceptance_run = acceptance_commands.add_parser(
        "run", help="Write one daily verdict and notify."
    )
    acceptance_run.add_argument("--date", type=date.fromisoformat, required=True)
    acceptance_run.add_argument(
        "--pool-file",
        type=Path,
        default=Path("config/stock_pools/initial-v1/pool.json"),
    )

    runtime_parser = subparsers.add_parser(
        "runtime", help="Run the unattended trading-day service supervisor."
    )
    runtime_commands = runtime_parser.add_subparsers(dest="runtime_command", required=True)
    runtime_run = runtime_commands.add_parser(
        "run", help="Continuously start and stop trading-day services from the calendar."
    )
    runtime_run.add_argument(
        "--project-root",
        type=Path,
        default=Path.cwd(),
        help="Project root containing scripts and config; defaults to the current directory.",
    )
    runtime_run.add_argument(
        "--pool-file",
        type=Path,
        default=Path("config/stock_pools/initial-v1/pool.json"),
    )
    runtime_run.add_argument(
        "--symbols-file",
        type=Path,
        default=Path("config/stock_pools/initial-v1/all_symbols.txt"),
    )
    runtime_run.add_argument(
        "--holdings-file",
        type=Path,
        default=Path("data/private/holdings.json"),
        help="Optional private holdings file; when present, adds held symbols and a separate card.",
    )
    runtime_run.add_argument(
        "--industry-map",
        type=Path,
        default=Path("config/stock_pools/initial-v1/industry_benchmarks.json"),
    )
    runtime_run.add_argument("--market-benchmark", default="510300.SH")
    runtime_run.add_argument(
        "--analysis-window-minutes",
        type=_positive_integer,
        default=15,
    )
    runtime_run.add_argument(
        "--status-interval-minutes",
        type=_positive_integer,
        default=15,
    )
    runtime_run.add_argument(
        "--poll-interval",
        type=_poll_interval_seconds,
        default=5.0,
        help="Seconds between service reconciliation cycles (default: 5).",
    )
    runtime_run.add_argument(
        "--max-cycles",
        type=_positive_integer,
        help="Diagnostic bound; omitted means run continuously.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the RegimeBeacon CLI and return a process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0

    settings = Settings()
    configure_logging(settings.log_level)

    if args.command == "doctor":
        report = run_startup_checks(settings)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["status"] == "ok" else 1

    if args.command == "config":
        print(json.dumps(settings.public_dict(), ensure_ascii=False, indent=2))
        return 0

    if args.command == "db":
        return _run_database_command(settings, args)

    if args.command == "daily":
        return _run_daily_command(settings, args)

    if args.command == "calendar":
        return _run_calendar_command(settings, args)

    if args.command == "quotes":
        return _run_quote_command(settings, args)

    if args.command == "features":
        return _run_feature_command(settings, args)

    if args.command == "monitor":
        return _run_monitor_command(settings, args)

    if args.command == "notifications":
        return _run_notification_command(settings, args)

    if args.command == "acceptance":
        return _run_acceptance_command(settings, args)

    if args.command == "runtime":
        return _run_runtime_command(settings, args)

    parser.error(f"unknown command: {args.command}")


def _run_daily_command(settings: Settings, args: argparse.Namespace) -> int:
    """Keep network/Parquet work outside short SQLite control-plane transactions."""
    lake_root = settings.data_dir / "lake" / "tushare_daily"
    if args.daily_command in {"sync", "backfill"}:
        local_now = datetime.now(ZoneInfo(settings.timezone))
        end_date = args.date if args.daily_command == "sync" else args.end_date
        if end_date > local_now.date() or (
            end_date == local_now.date() and local_now.time() < time(17, 30)
        ):
            raise ValueError("daily sync requires a completed date (today after 17:30 local)")
        if args.daily_command == "backfill" and args.end_date < args.start_date:
            raise ValueError("backfill end_date cannot precede start_date")
        members = load_daily_members(args.pool_file, args.holdings_file)
        token = read_tushare_token(settings.tushare_token_file)
        if args.daily_command == "sync":
            dates = [args.date]
        else:

            async def trading_dates() -> list[date]:
                async with TushareCalendarClient(
                    token=token,
                    api_url=settings.tushare_api_url,
                    timeout_seconds=20.0,
                ) as calendar:
                    records = await calendar.fetch_calendar(
                        exchange="SSE", start_date=args.start_date, end_date=args.end_date
                    )
                return [record.cal_date for record in records if record.is_open]

            dates = asyncio.run(trading_dates())
            if len(dates) > args.max_trading_days:
                raise ValueError(
                    f"backfill has {len(dates)} trading days, exceeds --max-trading-days={args.max_trading_days}"
                )
        upgrade_database(settings)
        engine = create_database_engine(settings)
        try:
            with TushareDailyClient(
                token=token,
                api_url=settings.tushare_api_url,
                timeout_seconds=20.0,
            ) as source:
                factory = create_session_factory(engine)
                report = [
                    item
                    for trade_date in dates
                    for item in sync_daily_date(
                        source=source,
                        session_factory=factory,
                        lake_root=lake_root,
                        trade_date=trade_date,
                        members=members,
                        refresh=args.refresh,
                    )
                ]
        finally:
            engine.dispose()
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if all(row["status"] == "complete" for row in report) else 3

    engine = create_database_engine(settings)
    try:
        with create_session_factory(engine)() as session:
            read_report: object
            if args.daily_command == "status":
                read_report = list_daily_partitions(session, trade_date=args.date)
            elif args.daily_command == "history":
                symbol = QuoteSymbol.parse(args.symbol).ts_code
                read_report = query_daily_history(
                    session,
                    symbol=symbol,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    lake_root=lake_root,
                )
            elif args.daily_command == "summary":
                read_report = summarize_daily_pool(
                    session, trade_date=args.date, lake_root=lake_root
                )
            else:
                raise ValueError(f"unsupported daily command: {args.daily_command}")
    finally:
        engine.dispose()
    print(json.dumps(read_report, ensure_ascii=False, indent=2))
    return 0


def _run_database_command(settings: Settings, args: argparse.Namespace) -> int:
    """Execute a local database administration command."""
    if args.database_command == "upgrade":
        upgrade_database(settings)
        status = inspect_schema(settings)
        print(json.dumps(asdict(status), ensure_ascii=False, indent=2))
        return 0 if status.ok else 1

    if args.database_command == "check":
        status = inspect_schema(settings)
        print(json.dumps(asdict(status), ensure_ascii=False, indent=2))
        return 0 if status.ok else 1

    if args.database_command == "recover":
        engine = create_database_engine(settings)
        try:
            report = run_startup_recovery(create_session_factory(engine))
        finally:
            engine.dispose()
        print(json.dumps(asdict(report), ensure_ascii=False, indent=2))
        return 0

    if args.database_command == "backup":
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        output = args.output or settings.data_dir / "backups" / f"regimebeacon-{timestamp}.sqlite3"
        result = online_backup(
            settings.database_path,
            output,
            busy_timeout_ms=settings.sqlite_busy_timeout_ms,
        )
        print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
        return 0

    raise ValueError(f"unsupported database command: {args.database_command}")


def _run_calendar_command(settings: Settings, args: argparse.Namespace) -> int:
    """Synchronize or inspect the cached Tushare trade_cal calendar."""
    local_now = datetime.now(ZoneInfo(settings.timezone))
    if args.calendar_command == "sync":
        start_date = args.start_date or date(local_now.year, 1, 1)
        end_date = args.end_date or date(local_now.year, 12, 31)
        if end_date < start_date:
            raise ValueError("calendar end date cannot be before start date")
        payload = asyncio.run(_synchronize_calendar(settings, start_date, end_date))
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    if args.calendar_command == "status":
        observed_at = args.at or datetime.now(UTC)
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("--at must include a timezone offset")
        payload = asyncio.run(_calendar_status(settings, observed_at))
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0 if payload["calendar_date_known"] else 1

    raise ValueError(f"unsupported calendar command: {args.calendar_command}")


def _run_quote_command(settings: Settings, args: argparse.Namespace) -> int:
    """Execute a live collection or deterministic raw-response replay."""
    if args.quote_command == "collect":
        market_phase: MarketPhase | None = None
        expected_trade_date = args.expected_date
        local_clock = datetime.now(ZoneInfo(settings.timezone)).time()
        opening_probe = time(9, 15) <= local_clock <= time(9, 25)
        if not args.ignore_market_gate:
            session_status, coverage = asyncio.run(
                _load_market_session_status(settings, datetime.now(UTC))
            )
            if not session_status.collect_production_quotes:
                print(
                    json.dumps(
                        {
                            "event": "market.collection.skipped",
                            **session_status.to_dict(),
                            "calendar_coverage": coverage,
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                )
                return 0
            market_phase = session_status.phase
            expected_trade_date = expected_trade_date or session_status.trade_date
        result = asyncio.run(
            _collect_quotes(
                settings,
                args.symbols,
                expected_trade_date=expected_trade_date,
                idempotency_key=args.idempotency_key,
                archive_raw=not args.no_archive and not opening_probe,
                market_phase=market_phase,
            )
        )
        if not args.no_persist and not opening_probe:
            engine = create_database_engine(settings)
            try:
                with create_session_factory(engine).begin() as session:
                    persist_market_collection(session, result)
            finally:
                engine.dispose()
        print(
            json.dumps(
                {
                    **result.to_dict(),
                    "opening_probe_mode": opening_probe,
                    "persisted": not args.no_persist and not opening_probe,
                    "archived": not args.no_archive and not opening_probe,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    if args.quote_command == "probe-opening":
        session_status, coverage = asyncio.run(
            _load_market_session_status(settings, datetime.now(UTC))
        )
        if (
            session_status.phase is not MarketPhase.OPENING_CALL_AUCTION
            or not session_status.collect_quotes
        ):
            print(
                json.dumps(
                    {
                        "event": "market.opening_probe.skipped",
                        **session_status.to_dict(),
                        "calendar_coverage": coverage,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0
        result = asyncio.run(
            _collect_quotes(
                settings,
                args.symbols,
                expected_trade_date=session_status.trade_date,
                idempotency_key=None,
                archive_raw=False,
                market_phase=session_status.phase,
            )
        )
        print(
            json.dumps(
                {
                    "event": "market.opening_probe.completed",
                    "persisted": False,
                    "archived": False,
                    **result.to_dict(include_quotes=False),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    if args.quote_command == "replay":
        replayed = replay_archive(
            args.archive,
            expected_trade_date=args.expected_date,
        )
        payload = {
            "summary": replayed.to_summary(),
            "quotes": {symbol: quote.to_dict() for symbol, quote in replayed.quotes.items()},
            "quote_issues": {
                symbol: [issue.to_dict() for issue in issues]
                for symbol, issues in replayed.quote_issues.items()
            },
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    if args.quote_command == "stats":
        local_zone = ZoneInfo(settings.timezone)
        report_date = args.date or datetime.now(local_zone).date()
        start_at = datetime.combine(report_date, time.min, tzinfo=local_zone)
        end_at = start_at + timedelta(days=1)
        engine = create_database_engine(settings)
        try:
            with create_session_factory(engine)() as session:
                report = build_market_metrics_report(
                    session,
                    start_at=start_at,
                    end_at=end_at,
                    expected_interval_seconds=settings.market_poll_interval_seconds,
                )
        finally:
            engine.dispose()
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    if args.quote_command == "watch":
        interval_seconds = args.interval_seconds or settings.market_poll_interval_seconds
        try:
            schedule = asyncio.run(
                _watch_quotes(
                    settings,
                    args.symbols,
                    expected_trade_date=args.expected_date,
                    interval_seconds=interval_seconds,
                    max_runs=args.max_runs,
                    archive_raw=not args.no_archive,
                    persist=not args.no_persist,
                )
            )
        except KeyboardInterrupt:
            return 130
        print(
            json.dumps(
                {"event": "market.schedule.stopped", **schedule.to_dict()},
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            flush=True,
        )
        return 0 if schedule.failed_runs == 0 else 1

    if args.quote_command == "compare":
        interval_seconds = args.interval_seconds or settings.market_poll_interval_seconds
        try:
            schedule, summary = asyncio.run(
                _compare_quotes(
                    settings,
                    args.symbols,
                    expected_trade_date=args.expected_date,
                    interval_seconds=interval_seconds,
                    max_runs=args.max_runs,
                    until=args.until,
                    report_directory=args.report_dir,
                    archive_raw=not args.no_archive,
                    ignore_market_gate=args.ignore_market_gate,
                )
            )
        except KeyboardInterrupt:
            return 130
        print(
            json.dumps(
                {
                    "event": "market.source_comparison.stopped",
                    **schedule.to_dict(),
                    "summary": summary,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            flush=True,
        )
        return 0 if schedule.failed_runs == 0 else 1

    raise ValueError(f"unsupported quote command: {args.quote_command}")


def _run_feature_command(settings: Settings, args: argparse.Namespace) -> int:
    """Build or inspect persisted minute-level features."""
    local_today = datetime.now(ZoneInfo(settings.timezone)).date()
    trade_date = args.date or local_today
    engine = create_database_engine(settings)
    try:
        if args.feature_command == "build":
            selected_symbols = {symbol.ts_code for symbol in parse_symbols(args.symbols)} or None
            market_benchmark = (
                QuoteSymbol.parse(args.market_benchmark).ts_code if args.market_benchmark else None
            )
            industry_benchmarks = _load_industry_benchmarks(args.industry_map)
            with create_session_factory(engine).begin() as session:
                report = build_minute_features(
                    session,
                    trade_date=trade_date,
                    timezone=settings.timezone,
                    expected_interval_seconds=(
                        args.interval_seconds or settings.market_poll_interval_seconds
                    ),
                    market_benchmark_symbol=market_benchmark,
                    industry_benchmarks=industry_benchmarks,
                    relative_volume_lookback_days=args.lookback_days,
                    relative_volume_minimum_history_days=args.minimum_history_days,
                    symbols=selected_symbols,
                )
            print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
            return 0

        if args.feature_command == "show":
            symbol = QuoteSymbol.parse(args.symbol).ts_code if args.symbol else None
            with create_session_factory(engine)() as session:
                rows = list_minute_features(
                    session,
                    trade_date=trade_date,
                    timezone=settings.timezone,
                    symbol=symbol,
                )
            print(
                json.dumps(
                    {
                        "trade_date": trade_date.isoformat(),
                        "symbol": symbol,
                        "count": len(rows),
                        "features": rows,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0

        if args.feature_command == "seal":
            metadata = load_instrument_metadata(args.pool_file)
            output_root = args.output_root or settings.data_dir / "lake" / "minute_market"
            with create_session_factory(engine)() as session:
                seal_report = seal_minute_session(
                    session,
                    trade_date=trade_date,
                    trading_session=MinuteTradingSession(args.trading_session),
                    timezone=settings.timezone,
                    output_root=output_root,
                    instrument_metadata=metadata,
                    observed_at=datetime.now(UTC),
                )
            print(json.dumps(seal_report.to_dict(), ensure_ascii=False, indent=2))
            return 0 if seal_report.complete else 3

        if args.feature_command == "merge-day":
            output_root = args.output_root or settings.data_dir / "lake" / "minute_market"
            day_report = merge_minute_day(
                trade_date=trade_date,
                timezone=settings.timezone,
                output_root=output_root,
                observed_at=datetime.now(UTC),
            )
            print(json.dumps(day_report.to_dict(), ensure_ascii=False, indent=2))
            return 0 if day_report.complete else 3
    finally:
        engine.dispose()

    raise ValueError(f"unsupported feature command: {args.feature_command}")


def _load_industry_benchmarks(path: Path | None) -> dict[str, str]:
    """Load and canonicalize a stock-to-industry-benchmark JSON mapping."""
    if path is None:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("industry map must be a JSON object")
    result: dict[str, str] = {}
    for raw_symbol, raw_benchmark in payload.items():
        if not isinstance(raw_symbol, str) or not isinstance(raw_benchmark, str):
            raise ValueError("industry map keys and values must be strings")
        symbol = QuoteSymbol.parse(raw_symbol).ts_code
        benchmark = QuoteSymbol.parse(raw_benchmark).ts_code
        result[symbol] = benchmark
    return result


def _run_monitor_command(settings: Settings, args: argparse.Namespace) -> int:
    """Execute one or repeated operational health checks."""
    if args.monitor_command == "check":
        observed_at = args.at or datetime.now(UTC)
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("--at must include a timezone offset")
        report = asyncio.run(_monitor_once(settings, observed_at))
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["ok"] else 1

    if args.monitor_command == "watch":
        interval_seconds = args.interval_seconds or settings.monitor_interval_seconds
        try:
            result = asyncio.run(
                _watch_monitor(
                    settings,
                    interval_seconds=interval_seconds,
                    max_runs=args.max_runs,
                )
            )
        except KeyboardInterrupt:
            return 130
        print(
            json.dumps(
                {"event": "monitor.schedule.stopped", **result.to_dict()},
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            flush=True,
        )
        return 0 if result.failed_runs == 0 else 1

    raise ValueError(f"unsupported monitor command: {args.monitor_command}")


def _run_notification_command(settings: Settings, args: argparse.Namespace) -> int:
    """Deliver outbox records through the configured Feishu custom bot."""
    max_items = getattr(args, "max_items", None) or settings.notification_batch_size
    try:
        credentials = FeishuCredentials.from_files(
            settings.feishu_webhook_file,
            settings.feishu_signing_secret_file,
        )
    except FeishuConfigurationError as exc:
        print(
            json.dumps(
                {"event": "notification.configuration.error", "error": str(exc)},
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            flush=True,
        )
        return 2

    if args.notification_command == "test":
        test_result = asyncio.run(
            _send_notification_test(
                settings,
                credentials=credentials,
                text=args.text,
            )
        )
        print(
            json.dumps(
                {"event": "notification.test.completed", **test_result},
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            flush=True,
        )
        return 0

    if args.notification_command == "deliver":
        delivery_result = asyncio.run(
            _deliver_notifications_once(
                settings,
                credentials=credentials,
                max_items=max_items,
            )
        )
        print(
            json.dumps(
                {"event": "notification.delivery.completed", **delivery_result},
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            flush=True,
        )
        return 0 if delivery_result["failed"] == 0 else 1

    if args.notification_command == "watch":
        interval_seconds = args.interval_seconds or settings.notification_poll_interval_seconds
        try:
            schedule, totals = asyncio.run(
                _watch_notifications(
                    settings,
                    credentials=credentials,
                    interval_seconds=interval_seconds,
                    max_runs=args.max_runs,
                    max_items=max_items,
                )
            )
        except KeyboardInterrupt:
            return 130
        print(
            json.dumps(
                {
                    "event": "notification.schedule.stopped",
                    **schedule.to_dict(),
                    "delivery_totals": totals,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            flush=True,
        )
        return 0 if schedule.failed_runs == 0 else 1

    raise ValueError(f"unsupported notification command: {args.notification_command}")


def _run_runtime_command(settings: Settings, args: argparse.Namespace) -> int:
    """Run the single-instance unattended trading-day supervisor."""
    if args.runtime_command != "run":
        raise ValueError(f"unsupported runtime command: {args.runtime_command}")
    project_root = args.project_root.resolve()
    settings = settings.model_copy(
        update={
            "data_dir": _resolve_runtime_path(project_root, settings.data_dir),
            "tushare_token_file": _resolve_runtime_path(project_root, settings.tushare_token_file),
            "feishu_webhook_file": _resolve_runtime_path(
                project_root, settings.feishu_webhook_file
            ),
            "feishu_signing_secret_file": _resolve_runtime_path(
                project_root, settings.feishu_signing_secret_file
            ),
        }
    )
    settings.ensure_runtime_directories()
    upgrade_database(settings)
    engine = create_database_engine(settings)
    try:
        factory = create_session_factory(engine)
        run_startup_recovery(factory)
        gate = _build_trading_session_gate(settings, factory)
        supervisor = TradingDayRuntimeSupervisor(
            settings,
            factory,
            gate,
            project_root=project_root,
            pool_file=args.pool_file,
            industry_map_file=args.industry_map,
            symbols_file=args.symbols_file,
            holdings_file=args.holdings_file,
            market_benchmark=args.market_benchmark,
            analysis_window_minutes=args.analysis_window_minutes,
            status_interval_minutes=args.status_interval_minutes,
            poll_interval_seconds=args.poll_interval,
        )
        try:
            asyncio.run(supervisor.run(max_cycles=args.max_cycles))
        except KeyboardInterrupt:
            return 130
        except RuntimeAlreadyRunningError as exc:
            print(
                json.dumps(
                    {"event": "runtime.supervisor.already_running", "error": str(exc)},
                    ensure_ascii=False,
                ),
                flush=True,
            )
            return 2
        return 0
    finally:
        engine.dispose()


def _run_acceptance_command(settings: Settings, args: argparse.Namespace) -> int:
    """Validate one finished date and queue a durable final report."""
    if args.acceptance_command != "run":
        raise ValueError(f"unsupported acceptance command: {args.acceptance_command}")
    settings.ensure_runtime_directories()
    upgrade_database(settings)
    engine = create_database_engine(settings)
    try:
        factory = create_session_factory(engine)
        report = run_daily_acceptance(
            settings,
            factory,
            trade_date=args.date,
            pool_file=args.pool_file,
        )
    finally:
        engine.dispose()
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


def _resolve_runtime_path(project_root: Path, path: Path) -> Path:
    return path.resolve() if path.is_absolute() else (project_root / path).resolve()


async def _send_notification_test(
    settings: Settings,
    *,
    credentials: FeishuCredentials,
    text: str,
) -> dict[str, str | None]:
    """Send a direct Feishu test message without touching durable alert state."""
    async with FeishuWebhookClient(
        credentials,
        timeout_seconds=settings.feishu_request_timeout_seconds,
    ) as client:
        receipt = await client.send_text(text)
    return {"provider_message_id": receipt.provider_message_id}


async def _collect_quotes(
    settings: Settings,
    symbol_values: list[str],
    *,
    expected_trade_date: date | None,
    idempotency_key: str | None,
    archive_raw: bool,
    market_phase: MarketPhase | None,
) -> MarketCollectionResult:
    symbols = parse_symbols(symbol_values)
    async with MarketDataCollector(settings) as collector:
        return await collector.collect(
            symbols,
            expected_trade_date=expected_trade_date,
            idempotency_key=idempotency_key,
            archive_raw=archive_raw,
            market_phase=market_phase,
        )


async def _watch_quotes(
    settings: Settings,
    symbol_values: list[str],
    *,
    expected_trade_date: date | None,
    interval_seconds: float,
    max_runs: int | None,
    archive_raw: bool,
    persist: bool,
) -> IntervalScheduleResult:
    """Continuously collect quotes while retaining one collector and connection pool."""
    symbols = parse_symbols(symbol_values)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered_signals: list[signal.Signals] = []
    for watched_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(watched_signal, stop_event.set)
        except (NotImplementedError, RuntimeError):
            continue
        registered_signals.append(watched_signal)

    engine = create_database_engine(settings)
    session_factory = create_session_factory(engine)
    gate = _build_trading_session_gate(settings, session_factory)
    heartbeat_instance = str(uuid4())
    heartbeat_details: dict[str, object] = {
        "symbol_count": len(symbols),
        "archive_raw": archive_raw,
        "persist_quotes": persist,
        "attempted_runs": 0,
        "last_collection_at": None,
        "last_usable_collection_at": None,
    }
    _touch_quote_watcher_heartbeat(
        session_factory,
        instance_id=heartbeat_instance,
        interval_seconds=interval_seconds,
        details=heartbeat_details,
    )
    initial_status = await gate.status_at(datetime.now(UTC))
    print(
        json.dumps(
            {
                "event": "market.schedule.started",
                "interval_seconds": interval_seconds,
                "symbols": [symbol.ts_code for symbol in symbols],
                "archive_raw": archive_raw,
                "persist": persist,
                "max_runs": max_runs,
                "market_gate": initial_status.to_dict(),
                "calendar_coverage": _coverage_payload(gate),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        flush=True,
    )

    try:
        async with MarketDataCollector(settings) as collector:
            last_session: tuple[date, str] | None = None

            async def collect_once(run_number: int) -> bool:
                nonlocal last_session
                session_status = await gate.status_at(datetime.now(UTC))
                heartbeat_details.update(
                    {
                        "attempted_runs": run_number,
                        "last_tick_at": datetime.now(UTC).isoformat(),
                        "trade_date": session_status.trade_date.isoformat(),
                        "market_phase": session_status.phase.value,
                    }
                )
                session_key = (session_status.trade_date, session_status.phase.value)
                if session_key != last_session:
                    print(
                        json.dumps(
                            {"event": "market.session.changed", **session_status.to_dict()},
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                        flush=True,
                    )
                    last_session = session_key
                if not session_status.collect_production_quotes:
                    _touch_quote_watcher_heartbeat(
                        session_factory,
                        instance_id=heartbeat_instance,
                        interval_seconds=interval_seconds,
                        details=heartbeat_details,
                    )
                    return False
                result = await collector.collect(
                    symbols,
                    expected_trade_date=expected_trade_date or session_status.trade_date,
                    archive_raw=archive_raw,
                    market_phase=session_status.phase,
                )
                if persist:
                    with session_factory.begin() as session:
                        persist_market_collection(session, result)
                heartbeat_details["last_collection_at"] = result.finished_at.isoformat()
                if any(item.selected_quote is not None for item in result.reconciled.values()):
                    heartbeat_details["last_usable_collection_at"] = result.finished_at.isoformat()
                _touch_quote_watcher_heartbeat(
                    session_factory,
                    instance_id=heartbeat_instance,
                    interval_seconds=interval_seconds,
                    details=heartbeat_details,
                )
                payload = result.to_dict(include_quotes=False)
                print(
                    json.dumps(
                        {
                            "event": "market.collection.completed",
                            "run_number": run_number,
                            **payload,
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                    flush=True,
                )
                return True

            schedule_result = await FixedIntervalScheduler(interval_seconds).run(
                collect_once,
                stop_event=stop_event,
                max_runs=max_runs,
            )
            heartbeat_details["schedule_result"] = schedule_result.to_dict()
            return schedule_result
    finally:
        try:
            with session_factory.begin() as session:
                stop_runtime_heartbeat(
                    session,
                    instance_id=heartbeat_instance,
                    now=datetime.now(UTC),
                    details=dict(heartbeat_details),
                )
        except Exception:
            # Shutdown must continue even if the heartbeat database is unavailable.
            pass
        engine.dispose()
        for registered_signal in registered_signals:
            loop.remove_signal_handler(registered_signal)


def _touch_quote_watcher_heartbeat(
    session_factory: sessionmaker[Session],
    *,
    instance_id: str,
    interval_seconds: float,
    details: dict[str, object],
) -> bool:
    """Best-effort heartbeat, including recovery from a locked initial registration."""
    try:
        with session_factory.begin() as session:
            now = datetime.now(UTC)
            try:
                touch_runtime_heartbeat(
                    session,
                    instance_id=instance_id,
                    now=now,
                    details=dict(details),
                )
            except ValueError as exc:
                if not str(exc).startswith("unknown runtime heartbeat instance:"):
                    raise
                start_runtime_heartbeat(
                    session,
                    service_name=QUOTE_WATCHER_SERVICE,
                    instance_id=instance_id,
                    interval_seconds=interval_seconds,
                    now=now,
                    details=dict(details),
                )
    except OperationalError as exc:
        if "database is locked" not in str(exc.orig).lower():
            raise
        logger.warning("quote watcher heartbeat delayed by SQLite writer lock")
        return False
    return True


async def _compare_quotes(
    settings: Settings,
    symbol_values: list[str],
    *,
    expected_trade_date: date | None,
    interval_seconds: float,
    max_runs: int | None,
    until: datetime | None,
    report_directory: Path | None,
    archive_raw: bool,
    ignore_market_gate: bool,
) -> tuple[IntervalScheduleResult, dict[str, object]]:
    """Run the isolated Sina/Tencent diagnostic comparison on a fixed cadence."""
    symbols = parse_symbols(symbol_values)
    if until is not None and (until.tzinfo is None or until.utcoffset() is None):
        raise ValueError("--until must include a timezone offset")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered_signals: list[signal.Signals] = []
    for watched_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(watched_signal, stop_event.set)
        except (NotImplementedError, RuntimeError):
            continue
        registered_signals.append(watched_signal)

    engine = create_database_engine(settings)
    session_factory = create_session_factory(engine)
    gate = _build_trading_session_gate(settings, session_factory)
    initial_status: MarketSessionStatus | None = None
    if not ignore_market_gate:
        initial_status = await gate.status_at(datetime.now(UTC))
        if expected_trade_date is None and initial_status.trade_date is not None:
            expected_trade_date = initial_status.trade_date

    async def stop_at_deadline() -> None:
        if until is None:
            return
        delay = (until.astimezone(UTC) - datetime.now(UTC)).total_seconds()
        if delay > 0:
            await asyncio.sleep(delay)
        stop_event.set()

    deadline_task = asyncio.create_task(stop_at_deadline()) if until is not None else None
    print(
        json.dumps(
            {
                "event": "market.source_comparison.started",
                "interval_seconds": interval_seconds,
                "symbols": [symbol.ts_code for symbol in symbols],
                "expected_trade_date": (
                    expected_trade_date.isoformat() if expected_trade_date else None
                ),
                "until": until.isoformat() if until else None,
                "archive_raw": archive_raw,
                "report_directory": str(report_directory) if report_directory else None,
                "market_gate": (
                    initial_status.to_dict() if initial_status is not None else "ignored"
                ),
                "calendar_coverage": _coverage_payload(gate),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        flush=True,
    )

    try:
        async with DiagnosticComparisonRunner(
            settings,
            symbols,
            expected_trade_date=expected_trade_date,
            report_directory=report_directory,
            archive_raw=archive_raw,
        ) as runner:

            async def compare_once(run_number: int) -> bool:
                if not ignore_market_gate:
                    status = await gate.status_at(datetime.now(UTC))
                    if not status.collect_quotes:
                        print(
                            json.dumps(
                                {
                                    "event": "market.source_comparison.skipped",
                                    "run_number": run_number,
                                    **status.to_dict(),
                                },
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ),
                            flush=True,
                        )
                        return False
                    if expected_trade_date is None:
                        runner.expected_trade_date = status.trade_date
                cycle = await runner.collect_once(run_number)
                console_cycle = {
                    key: value for key, value in cycle.items() if key != "inconsistencies"
                }
                print(
                    json.dumps(console_cycle, ensure_ascii=False, separators=(",", ":")), flush=True
                )
                return True

            schedule = await FixedIntervalScheduler(interval_seconds).run(
                compare_once,
                stop_event=stop_event,
                max_runs=max_runs,
            )
            summary = runner.summary.to_dict()
            summary_payload = {
                "event": "market.source_comparison.summary",
                "generated_at": datetime.now(UTC).isoformat(),
                "report_directory": str(runner.report_directory.resolve()),
                "comparison_log": str(runner.comparison_log.resolve()),
                "inconsistency_log": str(runner.inconsistency_log.resolve()),
                "schedule": schedule.to_dict(),
                "summary": summary,
            }
            (runner.report_directory / "summary.json").write_text(
                json.dumps(summary_payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            summary = {
                **summary,
                "report_directory": str(runner.report_directory.resolve()),
                "comparison_log": str(runner.comparison_log.resolve()),
                "inconsistency_log": str(runner.inconsistency_log.resolve()),
                "summary_file": str((runner.report_directory / "summary.json").resolve()),
            }
    finally:
        if deadline_task is not None:
            deadline_task.cancel()
            await asyncio.gather(deadline_task, return_exceptions=True)
        engine.dispose()
        for registered_signal in registered_signals:
            loop.remove_signal_handler(registered_signal)
    return schedule, summary


async def _monitor_once(settings: Settings, observed_at: datetime) -> dict[str, object]:
    """Run one operational check and synchronize alert state transactionally."""
    settings.ensure_runtime_directories()
    engine = create_database_engine(settings)
    try:
        session_factory = create_session_factory(engine)
        gate = _build_trading_session_gate(settings, session_factory)
        market_status = await gate.status_at(observed_at)
        with session_factory.begin() as session:
            return run_operational_checks(
                session,
                settings=settings,
                market_status=market_status,
                observed_at=observed_at,
            )
    finally:
        engine.dispose()


async def _watch_monitor(
    settings: Settings,
    *,
    interval_seconds: float,
    max_runs: int | None,
) -> IntervalScheduleResult:
    """Continuously evaluate operational checks in a process separate from quote watch."""
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered_signals: list[signal.Signals] = []
    for watched_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(watched_signal, stop_event.set)
        except (NotImplementedError, RuntimeError):
            continue
        registered_signals.append(watched_signal)

    print(
        json.dumps(
            {
                "event": "monitor.schedule.started",
                "interval_seconds": interval_seconds,
                "max_runs": max_runs,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        flush=True,
    )

    settings.ensure_runtime_directories()
    engine = create_database_engine(settings)
    session_factory = create_session_factory(engine)
    gate = _build_trading_session_gate(settings, session_factory)

    async def check_once(run_number: int) -> bool:
        observed_at = datetime.now(UTC)
        market_status = await gate.status_at(observed_at)
        with session_factory.begin() as session:
            report = run_operational_checks(
                session,
                settings=settings,
                market_status=market_status,
                observed_at=observed_at,
            )
        print(
            json.dumps(
                {"event": "monitor.check.completed", "run_number": run_number, **report},
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            flush=True,
        )
        return True

    try:
        return await FixedIntervalScheduler(interval_seconds).run(
            check_once,
            stop_event=stop_event,
            max_runs=max_runs,
        )
    finally:
        engine.dispose()
        for registered_signal in registered_signals:
            loop.remove_signal_handler(registered_signal)


async def _deliver_notifications_once(
    settings: Settings,
    *,
    credentials: FeishuCredentials,
    max_items: int,
) -> dict[str, int]:
    """Drain one bounded Feishu outbox batch."""
    engine = create_database_engine(settings)
    try:
        async with FeishuWebhookClient(
            credentials,
            timeout_seconds=settings.feishu_request_timeout_seconds,
        ) as client:
            worker = NotificationDeliveryWorker(
                create_session_factory(engine),
                client,
                lease_seconds=settings.notification_lease_seconds,
                retry_base_seconds=settings.notification_retry_base_seconds,
                retry_max_seconds=settings.notification_retry_max_seconds,
            )
            return (await worker.deliver_batch(max_items=max_items)).to_dict()
    finally:
        engine.dispose()


async def _watch_notifications(
    settings: Settings,
    *,
    credentials: FeishuCredentials,
    interval_seconds: float,
    max_runs: int | None,
    max_items: int,
) -> tuple[IntervalScheduleResult, dict[str, int]]:
    """Continuously drain Feishu outbox records with clean signal handling."""
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered_signals: list[signal.Signals] = []
    for watched_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(watched_signal, stop_event.set)
        except (NotImplementedError, RuntimeError):
            continue
        registered_signals.append(watched_signal)

    totals = {"claimed": 0, "sent": 0, "failed": 0, "dead": 0, "recovered": 0}
    print(
        json.dumps(
            {
                "event": "notification.schedule.started",
                "channel": "feishu",
                "interval_seconds": interval_seconds,
                "max_items_per_poll": max_items,
                "max_runs": max_runs,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        flush=True,
    )

    engine = create_database_engine(settings)
    try:
        async with FeishuWebhookClient(
            credentials,
            timeout_seconds=settings.feishu_request_timeout_seconds,
        ) as client:
            worker = NotificationDeliveryWorker(
                create_session_factory(engine),
                client,
                lease_seconds=settings.notification_lease_seconds,
                retry_base_seconds=settings.notification_retry_base_seconds,
                retry_max_seconds=settings.notification_retry_max_seconds,
            )

            async def deliver_once(run_number: int) -> bool:
                result = await worker.deliver_batch(max_items=max_items)
                payload = result.to_dict()
                for key, value in payload.items():
                    totals[key] += value
                print(
                    json.dumps(
                        {
                            "event": "notification.delivery.completed",
                            "run_number": run_number,
                            **payload,
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                    flush=True,
                )
                return result.claimed > 0

            schedule = await FixedIntervalScheduler(interval_seconds).run(
                deliver_once,
                stop_event=stop_event,
                max_runs=max_runs,
            )
            return schedule, totals
    finally:
        engine.dispose()
        for registered_signal in registered_signals:
            loop.remove_signal_handler(registered_signal)


def _build_trading_session_gate(
    settings: Settings,
    session_factory: sessionmaker[Session],
) -> TushareTradingSessionGate:
    """Build the Tushare-backed market gate from non-secret settings."""
    return TushareTradingSessionGate(
        session_factory,
        timezone=settings.timezone,
        exchange=settings.trading_calendar_exchange,
        token_file=settings.tushare_token_file,
        api_url=settings.tushare_api_url,
        timeout_seconds=settings.market_request_timeout_seconds,
        refresh_hours=settings.trading_calendar_refresh_hours,
    )


async def _synchronize_calendar(
    settings: Settings,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    engine = create_database_engine(settings)
    try:
        gate = _build_trading_session_gate(settings, create_session_factory(engine))
        await gate.force_refresh(start_date=start_date, end_date=end_date)
        return {
            "ok": True,
            "source": "tushare.trade_cal",
            **_coverage_payload(gate),
        }
    finally:
        engine.dispose()


async def _calendar_status(settings: Settings, observed_at: datetime) -> dict[str, object]:
    status, coverage = await _load_market_session_status(settings, observed_at)
    return {**status.to_dict(), "calendar_coverage": coverage}


async def _load_market_session_status(
    settings: Settings,
    observed_at: datetime,
) -> tuple[MarketSessionStatus, dict[str, object]]:
    engine = create_database_engine(settings)
    try:
        gate = _build_trading_session_gate(settings, create_session_factory(engine))
        status = await gate.status_at(observed_at)
        return status, _coverage_payload(gate)
    finally:
        engine.dispose()


def _coverage_payload(gate: TushareTradingSessionGate) -> dict[str, object]:
    coverage = gate.coverage
    return {
        "exchange": coverage.exchange,
        "start_date": coverage.start_date.isoformat() if coverage.start_date else None,
        "end_date": coverage.end_date.isoformat() if coverage.end_date else None,
        "row_count": coverage.row_count,
        "last_fetched_at": (
            coverage.last_fetched_at.isoformat() if coverage.last_fetched_at else None
        ),
    }
