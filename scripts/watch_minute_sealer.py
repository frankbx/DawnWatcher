#!/usr/bin/env python3
"""Finalize and seal selected trading sessions at safe post-session times."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import os
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
    parser.add_argument(
        "--result-file", type=Path, help="Write a structured result for the supervisor."
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
    incomplete = False
    transient_failure = False
    results: list[dict[str, Any]] = []
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
                incomplete = True
                payload["event"] = "market.minute_session.sealed_incomplete"
        except Exception as exc:
            if isinstance(exc, ValueError) and "no minute bars found" in str(exc):
                incomplete = True
            else:
                transient_failure = True
            payload = {
                "event": "market.minute_session.seal_failed",
                "observed_at": observed_at.isoformat(),
                "trade_date": trade_date.isoformat(),
                "session": trading_session.value,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        results.append(payload)
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
                    incomplete = True
                    day_payload["event"] = "market.minute_day.sealed_incomplete"
            except Exception as exc:
                if isinstance(exc, FileNotFoundError) and incomplete:
                    pass
                else:
                    transient_failure = True
                day_payload = {
                    "event": "market.minute_day.seal_failed",
                    "observed_at": datetime.now(zone).isoformat(),
                    "trade_date": trade_date.isoformat(),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            results.append(day_payload)
            print(json.dumps(day_payload, ensure_ascii=False), flush=True)
    # A temporary I/O or database failure must still be retried even when an
    # earlier session was also incomplete. The bounded supervisor retry will
    # eventually publish the final incomplete verdict if nothing recovers.
    failure_kind = (
        "transient_failure" if transient_failure else "data_incomplete" if incomplete else None
    )
    result = {
        "trade_date": trade_date.isoformat(),
        "success": failure_kind is None,
        "failure_kind": failure_kind,
        "retryable": failure_kind == "transient_failure",
        "events": results,
    }
    if args.result_file is not None:
        _write_result(args.result_file, result)
    if failure_kind is not None:
        raise SystemExit(75 if failure_kind == "transient_failure" else 2)


def _write_result(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _terminal_result(path: Path, trade_date: date) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict) or payload.get("trade_date") != trade_date.isoformat():
        return None
    if payload.get("success") is True or (
        payload.get("failure_kind") == "data_incomplete" and payload.get("retryable") is False
    ):
        return payload
    return None


def run_exclusively(args: argparse.Namespace) -> None:
    """Serialize per-date sealing and reuse terminal outcomes after a parent crash."""
    settings = Settings()
    trade_date = args.date or datetime.now(ZoneInfo(settings.timezone)).date()
    lock_path = (
        settings.data_dir / "reports" / "runtime" / trade_date.isoformat() / "minute-sealer.lock"
    )
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        result = (
            _terminal_result(args.result_file, trade_date) if args.result_file is not None else None
        )
        if result is not None:
            print(
                json.dumps(
                    {
                        "event": "market.minute_sealer.result_reused",
                        "trade_date": trade_date.isoformat(),
                        "success": result["success"],
                        "failure_kind": result.get("failure_kind"),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            if not result["success"]:
                raise SystemExit(2)
            return
        asyncio.run(run(args))
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def main() -> None:
    run_exclusively(parse_args())


if __name__ == "__main__":
    main()
