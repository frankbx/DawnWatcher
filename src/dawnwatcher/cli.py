"""Command-line entry point for DawnWatcher."""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
from collections.abc import Sequence
from dataclasses import asdict
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session, sessionmaker

from dawnwatcher import __version__
from dawnwatcher.config import Settings
from dawnwatcher.diagnostics.comparison import DiagnosticComparisonRunner
from dawnwatcher.domain.quotes import MarketCollectionResult
from dawnwatcher.logging import configure_logging
from dawnwatcher.market import MarketPhase, MarketSessionStatus
from dawnwatcher.market.gate import TushareTradingSessionGate
from dawnwatcher.ops.health import run_startup_checks
from dawnwatcher.ops.monitoring import (
    QUOTE_WATCHER_SERVICE,
    run_operational_checks,
    start_runtime_heartbeat,
    stop_runtime_heartbeat,
    touch_runtime_heartbeat,
)
from dawnwatcher.ops.recovery import run_startup_recovery
from dawnwatcher.providers.collector import MarketDataCollector, parse_symbols, replay_archive
from dawnwatcher.storage.backup import online_backup
from dawnwatcher.storage.database import create_database_engine, create_session_factory
from dawnwatcher.storage.market_metrics import build_market_metrics_report
from dawnwatcher.storage.market_quotes import persist_market_collection
from dawnwatcher.storage.schema import inspect_schema, upgrade_database
from dawnwatcher.workflows.interval import FixedIntervalScheduler, IntervalScheduleResult


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
        prog="dawnwatcher",
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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the DawnWatcher CLI and return a process exit code."""
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

    if args.command == "calendar":
        return _run_calendar_command(settings, args)

    if args.command == "quotes":
        return _run_quote_command(settings, args)

    if args.command == "monitor":
        return _run_monitor_command(settings, args)

    parser.error(f"unknown command: {args.command}")


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
        output = args.output or settings.data_dir / "backups" / f"dawnwatcher-{timestamp}.sqlite3"
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
        if not args.ignore_market_gate:
            session_status, coverage = asyncio.run(
                _load_market_session_status(settings, datetime.now(UTC))
            )
            if not session_status.collect_quotes:
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
                archive_raw=not args.no_archive,
                market_phase=market_phase,
            )
        )
        if not args.no_persist:
            engine = create_database_engine(settings)
            try:
                with create_session_factory(engine).begin() as session:
                    persist_market_collection(session, result)
            finally:
                engine.dispose()
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
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
        "symbols": [symbol.ts_code for symbol in symbols],
        "archive_raw": archive_raw,
        "persist_quotes": persist,
        "attempted_runs": 0,
        "last_collection_at": None,
        "last_usable_collection_at": None,
    }
    with session_factory.begin() as session:
        start_runtime_heartbeat(
            session,
            service_name=QUOTE_WATCHER_SERVICE,
            instance_id=heartbeat_instance,
            interval_seconds=interval_seconds,
            now=datetime.now(UTC),
            details=dict(heartbeat_details),
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
                with session_factory.begin() as session:
                    touch_runtime_heartbeat(
                        session,
                        instance_id=heartbeat_instance,
                        now=datetime.now(UTC),
                        details=dict(heartbeat_details),
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
                if not session_status.collect_quotes:
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
                with session_factory.begin() as session:
                    touch_runtime_heartbeat(
                        session,
                        instance_id=heartbeat_instance,
                        now=datetime.now(UTC),
                        details=dict(heartbeat_details),
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
