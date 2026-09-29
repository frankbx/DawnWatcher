"""Long-running supervisor that starts and stops trading-day services automatically."""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import signal
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta, tzinfo
from pathlib import Path
from types import TracebackType
from typing import Any, BinaryIO, Literal, Self
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from regimebeacon.config import Settings
from regimebeacon.domain import QuoteSymbol
from regimebeacon.market import MarketSessionStatus
from regimebeacon.market.gate import TushareTradingSessionGate
from regimebeacon.notifications.outbox import enqueue_notification
from regimebeacon.storage.models import NotificationOutbox

RestartPolicy = Literal["always", "on_failure", "never"]

_OPERATIONS_START = time(8, 50)
_DAILY_START = time(9, 14, 30)
_ANALYSIS_START = time(9, 15)
_MARKET_STOP = time(15, 0, 30)
_ANALYSIS_STOP = time(15, 1)
_OPERATIONS_STOP = time(15, 15)
_SEAL_CATCHUP_STOP = time(23, 50)
_ACCEPTANCE_CATCHUP_STOP = time(23, 58)
_RESTART_DELAY = timedelta(seconds=10)
_MAX_DAILY_ATTEMPTS = 3
_NOTIFICATION_DRAIN = timedelta(minutes=2)


class RuntimeAlreadyRunningError(RuntimeError):
    """Raised when a second runtime supervisor tries to use the same data directory."""


@dataclass(frozen=True, slots=True)
class RuntimeServicePlan:
    """One desired child process for the current trading day."""

    name: str
    trade_date: date
    command: tuple[str, ...]
    log_path: Path
    restart_policy: RestartPolicy
    stop_at: datetime
    result_marker: Path | None = None


@dataclass(slots=True)
class _ManagedProcess:
    plan: RuntimeServicePlan
    process: asyncio.subprocess.Process
    log_handle: BinaryIO
    started_at: datetime


class _RuntimeInstanceLock:
    """Advisory single-instance lock held for the supervisor lifetime."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._descriptor: int | None = None

    def __enter__(self) -> Self:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(descriptor)
            raise RuntimeAlreadyRunningError(
                f"another runtime supervisor holds {self.path}"
            ) from exc
        os.ftruncate(descriptor, 0)
        os.write(descriptor, f"{os.getpid()}\n".encode())
        os.fsync(descriptor)
        self._descriptor = descriptor
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback
        if self._descriptor is None:
            return
        fcntl.flock(self._descriptor, fcntl.LOCK_UN)
        os.close(self._descriptor)
        self._descriptor = None


class TradingDayRuntimeSupervisor:
    """Supervise isolated data, analysis, alert, and sealing processes by market date."""

    def __init__(
        self,
        settings: Settings,
        factory: sessionmaker[Session],
        gate: TushareTradingSessionGate,
        *,
        project_root: Path,
        pool_file: Path,
        industry_map_file: Path,
        symbols_file: Path,
        market_benchmark: str = "510300.SH",
        analysis_window_minutes: int = 15,
        status_interval_minutes: int = 15,
        poll_interval_seconds: float = 5.0,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        if poll_interval_seconds < 1:
            raise ValueError("runtime poll interval must be at least one second")
        if analysis_window_minutes < 1 or 60 % analysis_window_minutes:
            raise ValueError("analysis window must be a positive divisor of 60")
        if status_interval_minutes < 1 or 60 % status_interval_minutes:
            raise ValueError("status interval must be a positive divisor of 60")
        self.settings = settings
        self.factory = factory
        self.gate = gate
        self.project_root = project_root.resolve()
        self.pool_file = _resolve_from_root(self.project_root, pool_file)
        self.industry_map_file = _resolve_from_root(self.project_root, industry_map_file)
        self.symbols_file = _resolve_from_root(self.project_root, symbols_file)
        self.market_benchmark = QuoteSymbol.parse(market_benchmark).ts_code
        self.analysis_window_minutes = analysis_window_minutes
        self.status_interval_minutes = status_interval_minutes
        self.poll_interval_seconds = poll_interval_seconds
        self.zone = ZoneInfo(settings.timezone)
        self._now_provider = now_provider or (lambda: datetime.now(self.zone))
        self._managed: dict[str, _ManagedProcess] = {}
        self._attempts: dict[tuple[date, str], int] = {}
        self._next_restart_at: dict[tuple[date, str], datetime] = {}
        self._completed: set[tuple[date, str]] = set()
        self._stop_event = asyncio.Event()
        self._last_phase: tuple[date, str] | None = None
        self._calendar_notice_dates: set[tuple[date, bool]] = set()
        self._symbols = _load_symbols(self.symbols_file)
        _require_file(self.pool_file)
        _require_file(self.industry_map_file)

    async def run(self, *, max_cycles: int | None = None) -> None:
        """Run until signalled, optionally bounding scheduler cycles for diagnostics."""
        if max_cycles is not None and max_cycles < 1:
            raise ValueError("max_cycles must be at least one")
        loop = asyncio.get_running_loop()
        registered_signals: list[signal.Signals] = []
        for watched_signal in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(watched_signal, self._stop_event.set)
            except (NotImplementedError, RuntimeError):
                continue
            registered_signals.append(watched_signal)

        lock_path = self.settings.data_dir / "reports" / "runtime-supervisor.lock"
        try:
            with _RuntimeInstanceLock(lock_path):
                print(
                    json.dumps(
                        {
                            "event": "runtime.supervisor.started",
                            "observed_at": self._now().isoformat(),
                            "project_root": str(self.project_root),
                            "symbol_count": len(self._symbols),
                            "poll_interval_seconds": self.poll_interval_seconds,
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
                cycles = 0
                while not self._stop_event.is_set():
                    await self.run_once()
                    cycles += 1
                    if max_cycles is not None and cycles >= max_cycles:
                        break
                    try:
                        await asyncio.wait_for(
                            self._stop_event.wait(), timeout=self.poll_interval_seconds
                        )
                    except TimeoutError:
                        pass
        finally:
            await self._stop_all()
            for registered_signal in registered_signals:
                loop.remove_signal_handler(registered_signal)
            print(
                json.dumps(
                    {
                        "event": "runtime.supervisor.stopped",
                        "observed_at": self._now().isoformat(),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )

    async def run_once(self) -> None:
        """Reconcile actual child processes with the current trading-day schedule."""
        local_now = self._now()
        status = await self.gate.status_at(local_now.astimezone(UTC))
        phase_key = (status.trade_date, status.phase.value)
        if phase_key != self._last_phase:
            print(
                json.dumps(
                    {"event": "runtime.market_phase.changed", **status.to_dict()},
                    ensure_ascii=False,
                ),
                flush=True,
            )
            self._last_phase = phase_key

        notice_key = (status.trade_date, status.calendar_date_known)
        operations_clock = _OPERATIONS_START <= local_now.time() < _OPERATIONS_STOP
        if not status.calendar_date_known:
            self._calendar_notice_dates.discard((status.trade_date, True))
        if notice_key not in self._calendar_notice_dates and (
            status.calendar_date_known or operations_clock
        ):
            self._sync_calendar_notice(status, local_now)
            self._calendar_notice_dates.add(notice_key)

        await self._collect_exited(local_now)
        plans = build_runtime_service_plans(
            settings=self.settings,
            status=status,
            local_now=local_now,
            project_root=self.project_root,
            pool_file=self.pool_file,
            industry_map_file=self.industry_map_file,
            symbols=self._symbols,
            market_benchmark=self.market_benchmark,
            analysis_window_minutes=self.analysis_window_minutes,
            status_interval_minutes=self.status_interval_minutes,
        )
        desired = {plan.name: plan for plan in plans}
        for name in tuple(self._managed):
            managed = self._managed[name]
            plan = desired.get(name)
            if plan is None or local_now >= plan.stop_at:
                await self._stop_process(name, reason="outside scheduled runtime window")
            else:
                managed.plan = plan

        for plan in plans:
            if plan.name in self._managed or self._is_terminal(plan):
                continue
            key = (plan.trade_date, plan.name)
            if local_now < self._next_restart_at.get(key, local_now):
                continue
            attempts = self._attempt_count(plan)
            if plan.restart_policy != "always" and attempts >= _MAX_DAILY_ATTEMPTS:
                continue
            await self._start_process(plan, local_now)

    def request_stop(self) -> None:
        """Request graceful shutdown from tests or embedding code."""
        self._stop_event.set()

    def _now(self) -> datetime:
        value = self._now_provider()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("runtime now_provider must return a timezone-aware datetime")
        return value.astimezone(self.zone)

    def _is_terminal(self, plan: RuntimeServicePlan) -> bool:
        key = (plan.trade_date, plan.name)
        return key in self._completed or (
            plan.result_marker is not None and _marker_is_terminal(plan.result_marker)
        )

    def _attempt_count(self, plan: RuntimeServicePlan) -> int:
        key = (plan.trade_date, plan.name)
        if key not in self._attempts and plan.result_marker is not None:
            marker = _read_marker(plan.result_marker)
            attempts = _nonnegative_int(marker.get("attempt_count"))
            if (
                marker.get("return_code") is None
                and marker.get("failure_kind") is None
                and marker.get("terminal") is False
            ):
                # The previous supervisor disappeared while the child was
                # active. A fresh child will serialize behind its date lock,
                # so this interrupted start must not exhaust the retry budget.
                attempts = max(0, attempts - 1)
            self._attempts[key] = attempts
        return self._attempts.get(key, 0)

    async def _start_process(self, plan: RuntimeServicePlan, local_now: datetime) -> None:
        key = (plan.trade_date, plan.name)
        self._attempts[key] = self._attempt_count(plan) + 1
        attempt = self._attempts[key]
        if plan.result_marker is not None:
            _write_result_marker(
                plan.result_marker,
                service=plan.name,
                trade_date=plan.trade_date,
                return_code=None,
                started_at=local_now,
                finished_at=None,
                log_path=plan.log_path,
                attempt_count=attempt,
                terminal=False,
                failure_kind=None,
                retryable=True,
            )
        plan.log_path.parent.mkdir(parents=True, exist_ok=True)
        handle = plan.log_path.open("ab", buffering=0)
        try:
            command = plan.command
            if plan.name == "minute_sealer" and plan.result_marker is not None:
                outcome_path = _sealer_outcome_path(plan.result_marker, attempt)
                command = (*command, "--result-file", str(outcome_path))
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=str(self.project_root),
                stdout=handle,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
            )
        except Exception as exc:
            handle.close()
            self._next_restart_at[key] = local_now + _RESTART_DELAY
            print(
                json.dumps(
                    {
                        "event": "runtime.service.start_failed",
                        "observed_at": local_now.isoformat(),
                        "service": plan.name,
                        "trade_date": plan.trade_date.isoformat(),
                        "attempt": attempt,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            if attempt >= _MAX_DAILY_ATTEMPTS:
                self._enqueue_failure(plan, return_code=126, observed_at=local_now)
            if plan.result_marker is not None:
                _write_result_marker(
                    plan.result_marker,
                    service=plan.name,
                    trade_date=plan.trade_date,
                    return_code=126,
                    started_at=local_now,
                    finished_at=local_now,
                    log_path=plan.log_path,
                    attempt_count=attempt,
                    terminal=attempt >= _MAX_DAILY_ATTEMPTS,
                    failure_kind="transient_failure",
                    retryable=attempt < _MAX_DAILY_ATTEMPTS,
                )
            return
        self._managed[plan.name] = _ManagedProcess(
            plan=plan,
            process=process,
            log_handle=handle,
            started_at=local_now,
        )
        print(
            json.dumps(
                {
                    "event": "runtime.service.started",
                    "observed_at": local_now.isoformat(),
                    "service": plan.name,
                    "trade_date": plan.trade_date.isoformat(),
                    "pid": process.pid,
                    "attempt": attempt,
                    "log_path": str(plan.log_path),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

    async def _collect_exited(self, local_now: datetime) -> None:
        for name in tuple(self._managed):
            managed = self._managed[name]
            return_code = managed.process.returncode
            if return_code is None:
                continue
            await managed.process.wait()
            managed.log_handle.close()
            del self._managed[name]
            plan = managed.plan
            key = (plan.trade_date, plan.name)
            attempts = self._attempts.get(key, 0)
            if plan.name == "minute_sealer" and plan.result_marker is not None:
                result = _read_marker(_sealer_outcome_path(plan.result_marker, attempts))
            else:
                result = _read_marker(plan.result_marker)
            failure_kind = result.get("failure_kind")
            retryable = bool(result.get("retryable", return_code not in (0, 2)))
            terminal = (
                (
                    plan.name == "minute_sealer"
                    and (return_code == 0 or not retryable or attempts >= _MAX_DAILY_ATTEMPTS)
                )
                or (plan.name != "minute_sealer" and plan.restart_policy == "never")
                or (
                    plan.restart_policy == "on_failure"
                    and (return_code == 0 or attempts >= _MAX_DAILY_ATTEMPTS)
                )
            )
            if terminal:
                self._completed.add(key)
            else:
                self._next_restart_at[key] = local_now + _RESTART_DELAY
            if plan.result_marker is not None:
                _write_result_marker(
                    plan.result_marker,
                    service=plan.name,
                    trade_date=plan.trade_date,
                    return_code=return_code,
                    started_at=managed.started_at,
                    finished_at=local_now,
                    log_path=plan.log_path,
                    attempt_count=attempts,
                    terminal=terminal,
                    failure_kind=(str(failure_kind) if failure_kind else None),
                    retryable=retryable and not terminal,
                )
            print(
                json.dumps(
                    {
                        "event": "runtime.service.exited",
                        "observed_at": local_now.isoformat(),
                        "service": plan.name,
                        "trade_date": plan.trade_date.isoformat(),
                        "return_code": return_code,
                        "terminal": terminal,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            if return_code != 0 and (terminal or attempts >= _MAX_DAILY_ATTEMPTS):
                self._enqueue_failure(plan, return_code=return_code, observed_at=local_now)

    async def _stop_process(self, name: str, *, reason: str) -> None:
        managed = self._managed.pop(name)
        process = managed.process
        was_running = process.returncode is None
        if was_running:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=15)
            except TimeoutError:
                process.kill()
                await process.wait()
        managed.log_handle.close()
        plan = managed.plan
        if was_running and plan.result_marker is not None:
            key = (plan.trade_date, plan.name)
            stopped_at = self._now()
            if reason == "runtime supervisor shutdown":
                # An intentional restart is not a failed sealing/acceptance attempt.
                attempts = max(0, self._attempt_count(plan) - 1)
                self._attempts[key] = attempts
                terminal = False
                return_code = None
                failure_kind = "interrupted"
            else:
                attempts = self._attempt_count(plan)
                terminal = True
                return_code = 124
                failure_kind = "deadline_exceeded"
                self._completed.add(key)
            _write_result_marker(
                plan.result_marker,
                service=plan.name,
                trade_date=plan.trade_date,
                return_code=return_code,
                started_at=managed.started_at,
                finished_at=stopped_at,
                log_path=plan.log_path,
                attempt_count=attempts,
                terminal=terminal,
                failure_kind=failure_kind,
                retryable=not terminal,
            )
            if terminal:
                self._enqueue_failure(plan, return_code=124, observed_at=stopped_at)
        print(
            json.dumps(
                {
                    "event": "runtime.service.stopped",
                    "observed_at": self._now().isoformat(),
                    "service": name,
                    "trade_date": managed.plan.trade_date.isoformat(),
                    "reason": reason,
                    "return_code": process.returncode,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

    async def _stop_all(self) -> None:
        await self._collect_exited(self._now())
        for name in sorted(
            self._managed,
            key=lambda item: self._managed[item].plan.result_marker is None,
        ):
            await self._stop_process(name, reason="runtime supervisor shutdown")

    def _enqueue_failure(
        self,
        plan: RuntimeServicePlan,
        *,
        return_code: int,
        observed_at: datetime,
    ) -> None:
        key = (plan.trade_date, plan.name)
        attempts = self._attempts.get(key, 0)
        with self.factory.begin() as session:
            enqueue_notification(
                session,
                idempotency_key=(
                    f"runtime-service-failed:{plan.trade_date.isoformat()}:{plan.name}:v1"
                ),
                event_type="runtime.daily_service.failed",
                channel=self.settings.alert_channel,
                recipient=self.settings.alert_recipient,
                payload={
                    "transition": "triggered",
                    "severity": "critical",
                    "alert_key": f"runtime.daily_service.{plan.name}",
                    "summary": f"daily runtime service failed: {plan.name}",
                    "observed_at": observed_at.isoformat(),
                    "details": {
                        "trade_date": plan.trade_date.isoformat(),
                        "return_code": return_code,
                        "attempt_count": attempts,
                        "log_path": str(plan.log_path),
                    },
                },
            )

    def _sync_calendar_notice(self, status: MarketSessionStatus, local_now: datetime) -> None:
        day = status.trade_date.isoformat()
        key = f"runtime-calendar-unknown:{day}:v1"
        with self.factory.begin() as session:
            existing = session.scalar(
                select(NotificationOutbox.id).where(NotificationOutbox.idempotency_key == key)
            )
            if (
                not status.calendar_date_known
                and _OPERATIONS_START <= local_now.time() < _OPERATIONS_STOP
            ):
                enqueue_notification(
                    session,
                    idempotency_key=key,
                    event_type="runtime.calendar.unknown",
                    channel=self.settings.alert_channel,
                    recipient=self.settings.alert_recipient,
                    payload={
                        "transition": "triggered",
                        "severity": "critical",
                        "alert_key": "runtime.calendar.unknown",
                        "summary": f"trading calendar is unknown for {day}; collection is paused",
                        "observed_at": local_now.isoformat(),
                    },
                )
            elif status.calendar_date_known and existing is not None:
                enqueue_notification(
                    session,
                    idempotency_key=f"runtime-calendar-recovered:{day}:v1",
                    event_type="runtime.calendar.recovered",
                    channel=self.settings.alert_channel,
                    recipient=self.settings.alert_recipient,
                    payload={
                        "transition": "resolved",
                        "severity": "warning",
                        "alert_key": "runtime.calendar.unknown",
                        "summary": f"trading calendar status recovered for {day}",
                        "observed_at": local_now.isoformat(),
                    },
                )


def build_runtime_service_plans(
    *,
    settings: Settings,
    status: MarketSessionStatus,
    local_now: datetime,
    project_root: Path,
    pool_file: Path,
    industry_map_file: Path,
    symbols: tuple[str, ...],
    market_benchmark: str,
    analysis_window_minutes: int,
    status_interval_minutes: int,
) -> tuple[RuntimeServicePlan, ...]:
    """Return processes that should exist at one local wall-clock instant."""
    if local_now.tzinfo is None or local_now.utcoffset() is None:
        raise ValueError("local_now must be timezone-aware")
    trade_date = status.trade_date
    zone = local_now.tzinfo
    operations_start = datetime.combine(trade_date, _OPERATIONS_START, tzinfo=zone)
    daily_start = datetime.combine(trade_date, _DAILY_START, tzinfo=zone)
    analysis_start = datetime.combine(trade_date, _ANALYSIS_START, tzinfo=zone)
    market_stop = datetime.combine(trade_date, _MARKET_STOP, tzinfo=zone)
    analysis_stop = datetime.combine(trade_date, _ANALYSIS_STOP, tzinfo=zone)
    operations_stop = datetime.combine(trade_date, _OPERATIONS_STOP, tzinfo=zone)
    seal_catchup_stop = datetime.combine(trade_date, _SEAL_CATCHUP_STOP, tzinfo=zone)
    acceptance_catchup_stop = datetime.combine(trade_date, _ACCEPTANCE_CATCHUP_STOP, tzinfo=zone)
    day_end = datetime.combine(trade_date, time(23, 59, 59), tzinfo=zone)
    day_directory = settings.data_dir / "reports" / "runtime" / trade_date.isoformat()
    result_marker = day_directory / "minute-sealer-result.json"
    acceptance_marker = day_directory / "daily-acceptance-result.json"
    python = sys.executable
    module = (python, "-m", "regimebeacon")
    plans: list[RuntimeServicePlan] = []

    if not status.calendar_date_known:
        if operations_start <= local_now < operations_stop:
            return (
                RuntimeServicePlan(
                    name="notification_worker",
                    trade_date=trade_date,
                    command=(*module, "notifications", "watch"),
                    log_path=day_directory / "notifications.log",
                    restart_policy="always",
                    stop_at=operations_stop,
                ),
            )
        return ()
    if not status.is_trading_day:
        return ()

    if daily_start <= local_now < market_stop:
        plans.append(
            RuntimeServicePlan(
                name="quote_watcher",
                trade_date=trade_date,
                command=(
                    *module,
                    "quotes",
                    "watch",
                    "--expected-date",
                    trade_date.isoformat(),
                    "--interval",
                    str(settings.market_poll_interval_seconds),
                    *symbols,
                ),
                log_path=day_directory / "quote-watcher.log",
                restart_policy="always",
                stop_at=market_stop,
            )
        )

    needs_sealer = daily_start <= local_now < seal_catchup_stop and not _marker_is_terminal(
        result_marker
    )
    sealer_finished = _marker_is_terminal(result_marker)
    needs_acceptance = (
        daily_start <= local_now < acceptance_catchup_stop
        and sealer_finished
        and not _marker_is_terminal(acceptance_marker)
    )
    operations_active = operations_start <= local_now < operations_stop
    if operations_active or needs_sealer or needs_acceptance:
        plans.append(
            RuntimeServicePlan(
                name="monitor",
                trade_date=trade_date,
                command=(*module, "monitor", "watch"),
                log_path=day_directory / "monitor.log",
                restart_policy="always",
                stop_at=(
                    acceptance_catchup_stop
                    if needs_acceptance
                    else seal_catchup_stop
                    if needs_sealer
                    else operations_stop
                ),
            )
        )
    marker_last_modified = max(
        (
            value
            for path in (result_marker, acceptance_marker)
            if (value := _marker_modified_at(path, zone)) is not None
        ),
        default=None,
    )
    drain_active = marker_last_modified is not None and local_now < min(
        marker_last_modified + _NOTIFICATION_DRAIN, day_end
    )
    if operations_active or needs_sealer or needs_acceptance or drain_active:
        plans.append(
            RuntimeServicePlan(
                name="notification_worker",
                trade_date=trade_date,
                command=(*module, "notifications", "watch"),
                log_path=day_directory / "notifications.log",
                restart_policy="always",
                stop_at=(
                    acceptance_catchup_stop
                    if needs_acceptance
                    else seal_catchup_stop
                    if needs_sealer
                    else max(operations_stop, marker_last_modified + _NOTIFICATION_DRAIN)
                    if marker_last_modified is not None
                    else operations_stop
                ),
            )
        )

    if daily_start <= local_now < analysis_stop:
        deadline = datetime.combine(trade_date, time(15, 0), tzinfo=zone)
        plans.append(
            RuntimeServicePlan(
                name="market_analysis",
                trade_date=trade_date,
                command=(
                    python,
                    str(project_root / "scripts" / "watch_market_analysis.py"),
                    "--pool-file",
                    str(pool_file),
                    "--industry-map",
                    str(industry_map_file),
                    "--market-benchmark",
                    market_benchmark,
                    "--window-minutes",
                    str(analysis_window_minutes),
                    "--until",
                    deadline.isoformat(),
                    "--no-initial-report",
                ),
                log_path=day_directory / "market-analysis.log",
                restart_policy="on_failure",
                stop_at=analysis_stop,
            )
        )

    if analysis_start <= local_now < analysis_stop:
        start_at = datetime.combine(trade_date, _ANALYSIS_START, tzinfo=zone)
        deadline = datetime.combine(trade_date, time(15, 0), tzinfo=zone)
        plans.append(
            RuntimeServicePlan(
                name="market_status",
                trade_date=trade_date,
                command=(
                    python,
                    str(project_root / "scripts" / "send_market_status_report.py"),
                    "--start-at",
                    start_at.isoformat(),
                    "--interval-minutes",
                    str(status_interval_minutes),
                    "--until",
                    deadline.isoformat(),
                    "--offset-seconds",
                    "30",
                    "--pool-file",
                    str(pool_file),
                ),
                log_path=day_directory / "market-status.log",
                restart_policy="on_failure",
                stop_at=analysis_stop,
            )
        )

    if needs_sealer:
        plans.append(
            RuntimeServicePlan(
                name="minute_sealer",
                trade_date=trade_date,
                command=(
                    python,
                    str(project_root / "scripts" / "watch_minute_sealer.py"),
                    "--date",
                    trade_date.isoformat(),
                    "--sessions",
                    "morning",
                    "afternoon",
                    "--pool-file",
                    str(pool_file),
                    "--industry-map",
                    str(industry_map_file),
                    "--market-benchmark",
                    market_benchmark,
                ),
                log_path=day_directory / "minute-sealer.log",
                restart_policy="on_failure",
                stop_at=seal_catchup_stop,
                result_marker=result_marker,
            )
        )
    if needs_acceptance:
        plans.append(
            RuntimeServicePlan(
                name="daily_acceptance",
                trade_date=trade_date,
                command=(
                    *module,
                    "acceptance",
                    "run",
                    "--date",
                    trade_date.isoformat(),
                    "--pool-file",
                    str(pool_file),
                ),
                log_path=day_directory / "daily-acceptance.log",
                restart_policy="on_failure",
                stop_at=acceptance_catchup_stop,
                result_marker=acceptance_marker,
            )
        )
    return tuple(plans)


def _load_symbols(path: Path) -> tuple[str, ...]:
    _require_file(path)
    values = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
    symbols = tuple(QuoteSymbol.parse(value).ts_code for value in values if value)
    if not symbols:
        raise ValueError(f"symbol file cannot be empty: {path}")
    if len(set(symbols)) != len(symbols):
        raise ValueError(f"symbol file contains duplicates: {path}")
    return symbols


def _write_result_marker(
    path: Path,
    *,
    service: str,
    trade_date: date,
    return_code: int | None,
    started_at: datetime,
    finished_at: datetime | None,
    log_path: Path,
    attempt_count: int,
    terminal: bool,
    failure_kind: str | None,
    retryable: bool,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".partial")
    payload: dict[str, Any] = {
        "service": service,
        "trade_date": trade_date.isoformat(),
        "success": return_code == 0,
        "return_code": return_code,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat() if finished_at is not None else None,
        "log_path": str(log_path),
        "attempt_count": attempt_count,
        "terminal": terminal,
        "failure_kind": failure_kind,
        "retryable": retryable,
    }
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_marker(path: Path | None) -> dict[str, Any]:
    if path is None or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _sealer_outcome_path(marker_path: Path, attempt: int) -> Path:
    return marker_path.with_name(f"minute-sealer-outcome-{attempt}.json")


def _marker_is_terminal(path: Path) -> bool:
    if not path.is_file():
        return False
    marker = _read_marker(path)
    return bool(marker) and bool(marker.get("terminal", not marker.get("retryable", False)))


def _marker_modified_at(path: Path, zone: tzinfo) -> datetime | None:
    if not _marker_is_terminal(path):
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=zone)


def _nonnegative_int(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


def _resolve_from_root(root: Path, path: Path) -> Path:
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def _require_file(path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(path)
