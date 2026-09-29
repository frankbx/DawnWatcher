#!/usr/bin/env python3
"""Install and bootstrap the RegimeBeacon unattended macOS launch agent."""

from __future__ import annotations

import argparse
import os
import plistlib
import subprocess
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4

_LABEL = "com.regimebeacon.runtime"
_LEGACY_LABEL = "com.dawnwatcher.runtime"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--output",
        type=Path,
        default=Path.home() / "Library" / "LaunchAgents" / f"{_LABEL}.plist",
    )
    parser.add_argument(
        "--no-bootstrap",
        action="store_true",
        help="Write the plist without loading it into the current GUI session.",
    )
    return parser.parse_args()


def build_launch_agent(project_root: Path) -> dict[str, Any]:
    root = project_root.resolve()
    python = root / ".venv" / "bin" / "python"
    if not python.is_file():
        raise FileNotFoundError(f"Python virtual environment is missing: {python}")
    for relative in (
        "token",
        "config/stock_pools/initial-v1/pool.json",
        "config/stock_pools/initial-v1/all_symbols.txt",
        "config/stock_pools/initial-v1/industry_benchmarks.json",
    ):
        path = root / relative
        if not path.is_file():
            raise FileNotFoundError(path)
    reports = root / "data" / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    return {
        "Label": _LABEL,
        "ProgramArguments": [
            str(python),
            "-m",
            "regimebeacon",
            "runtime",
            "run",
            "--project-root",
            str(root),
        ],
        "WorkingDirectory": str(root),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ProcessType": "Background",
        "ThrottleInterval": 10,
        "StandardOutPath": str(reports / "runtime-supervisor.log"),
        "StandardErrorPath": str(reports / "runtime-supervisor-error.log"),
        "EnvironmentVariables": {
            "PYTHONUNBUFFERED": "1",
            "PATH": "/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin",
        },
    }


def write_plist_atomic(destination: Path, payload: dict[str, Any]) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.partial")
    try:
        with temporary.open("xb") as handle:
            plistlib.dump(payload, handle, fmt=plistlib.FMT_XML, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def bootstrap(destination: Path) -> None:
    if sys.platform != "darwin":
        raise RuntimeError("launchd installation is supported only on macOS")
    domain = f"gui/{os.getuid()}"
    for label in (_LABEL, _LEGACY_LABEL):
        subprocess.run(
            ["launchctl", "bootout", f"{domain}/{label}"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    service = f"{domain}/{_LABEL}"
    subprocess.run(["launchctl", "bootstrap", domain, str(destination)], check=True)
    subprocess.run(["launchctl", "kickstart", "-k", service], check=True)
    legacy_plist = Path.home() / "Library" / "LaunchAgents" / f"{_LEGACY_LABEL}.plist"
    if legacy_plist != destination:
        legacy_plist.unlink(missing_ok=True)


def main() -> None:
    args = parse_args()
    root = args.project_root.resolve()
    destination = args.output.resolve()
    write_plist_atomic(destination, build_launch_agent(root))
    if not args.no_bootstrap:
        bootstrap(destination)
    print(
        f"installed {_LABEL} at {destination}"
        + (" (not loaded)" if args.no_bootstrap else " and loaded it")
    )


if __name__ == "__main__":
    main()
