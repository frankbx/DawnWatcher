"""Command-line entry point for DawnWatcher."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from dawnwatcher import __version__
from dawnwatcher.config import Settings
from dawnwatcher.logging import configure_logging
from dawnwatcher.ops.health import run_startup_checks
from dawnwatcher.ops.recovery import run_startup_recovery
from dawnwatcher.storage.backup import online_backup
from dawnwatcher.storage.database import create_database_engine, create_session_factory
from dawnwatcher.storage.schema import inspect_schema, upgrade_database


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
