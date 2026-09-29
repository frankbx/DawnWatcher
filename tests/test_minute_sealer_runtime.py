"""Structured minute-sealer outcomes used by the unattended supervisor."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import os
import threading
from datetime import date
from pathlib import Path

import pytest
from scripts import watch_minute_sealer

from regimebeacon.config import Settings


@pytest.mark.parametrize(
    ("failures", "expected_code", "failure_kind", "retryable"),
    [
        ((OSError("temporary disk error"),), 75, "transient_failure", True),
        ((ValueError("no minute bars found for morning"),), 2, "data_incomplete", False),
        (
            (ValueError("no minute bars found for morning"), OSError("temporary disk error")),
            75,
            "transient_failure",
            True,
        ),
    ],
)
def test_sealer_writes_retry_classification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failures: tuple[Exception, ...],
    expected_code: int,
    failure_kind: str,
    retryable: bool,
) -> None:
    failure_iter = iter(failures)

    def fail_seal(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise next(failure_iter)

    monkeypatch.setattr(watch_minute_sealer, "finalize_and_seal", fail_seal)
    result_file = tmp_path / "sealer-result.json"
    args = argparse.Namespace(
        date=date(2026, 9, 28),
        sessions=["morning", "afternoon"] if len(failures) == 2 else ["morning"],
        pool_file=tmp_path / "pool.json",
        industry_map=tmp_path / "industry.json",
        market_benchmark="510300.SH",
        output_root=tmp_path / "lake",
        result_file=result_file,
    )

    with pytest.raises(SystemExit) as exit_info:
        asyncio.run(watch_minute_sealer.run(args))

    assert exit_info.value.code == expected_code
    payload = json.loads(result_file.read_text(encoding="utf-8"))
    assert payload["success"] is False
    assert payload["failure_kind"] == failure_kind
    assert payload["retryable"] is retryable
    assert len(payload["events"]) == len(failures)


@pytest.mark.parametrize(
    ("success", "failure_kind", "expected_code"),
    [(True, None, None), (False, "data_incomplete", 2)],
)
def test_exclusive_sealer_reuses_terminal_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    success: bool,
    failure_kind: str | None,
    expected_code: int | None,
) -> None:
    trade_date = date(2026, 9, 28)
    result_file = tmp_path / "outcome.json"
    result_file.write_text(
        json.dumps(
            {
                "trade_date": trade_date.isoformat(),
                "success": success,
                "failure_kind": failure_kind,
                "retryable": False,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        watch_minute_sealer,
        "Settings",
        lambda: Settings(data_dir=tmp_path / "data", _env_file=None),
    )

    async def unexpected_run(args: argparse.Namespace) -> None:
        raise AssertionError(f"sealer unexpectedly ran: {args}")

    monkeypatch.setattr(watch_minute_sealer, "run", unexpected_run)
    args = argparse.Namespace(date=trade_date, result_file=result_file)

    if expected_code is None:
        watch_minute_sealer.run_exclusively(args)
    else:
        with pytest.raises(SystemExit) as exit_info:
            watch_minute_sealer.run_exclusively(args)
        assert exit_info.value.code == expected_code

    assert (
        tmp_path / "data" / "reports" / "runtime" / trade_date.isoformat() / "minute-sealer.lock"
    ).is_file()


def test_exclusive_sealer_waits_for_existing_writer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trade_date = date(2026, 9, 28)
    settings = Settings(data_dir=tmp_path / "data", _env_file=None)
    monkeypatch.setattr(watch_minute_sealer, "Settings", lambda: settings)
    lock_path = (
        settings.data_dir / "reports" / "runtime" / trade_date.isoformat() / "minute-sealer.lock"
    )
    lock_path.parent.mkdir(parents=True)
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    entered = threading.Event()
    finished = threading.Event()

    async def record_run(args: argparse.Namespace) -> None:
        del args
        entered.set()

    monkeypatch.setattr(watch_minute_sealer, "run", record_run)

    def invoke() -> None:
        try:
            watch_minute_sealer.run_exclusively(
                argparse.Namespace(date=trade_date, result_file=None)
            )
        finally:
            finished.set()

    worker = threading.Thread(target=invoke, daemon=True)
    try:
        worker.start()
        assert not entered.wait(timeout=0.1)
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)
    worker.join(timeout=3)
    assert finished.is_set()
    assert entered.is_set()
