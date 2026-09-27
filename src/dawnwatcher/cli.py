"""Command-line entry point for DawnWatcher."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence

from dawnwatcher import __version__
from dawnwatcher.config import Settings
from dawnwatcher.logging import configure_logging
from dawnwatcher.ops.health import run_startup_checks


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

    parser.error(f"unknown command: {args.command}")
