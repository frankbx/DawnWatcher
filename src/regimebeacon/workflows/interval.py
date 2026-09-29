"""Non-overlapping fixed-interval scheduling for long-running workflows."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from typing import Any

ScheduledJob = Callable[[int], Awaitable[bool | None]]

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class IntervalScheduleResult:
    """Execution counters returned when an interval schedule stops."""

    interval_seconds: float
    attempted_runs: int
    completed_runs: int
    skipped_runs: int
    failed_runs: int
    skipped_intervals: int
    stop_requested: bool

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return asdict(self)


class FixedIntervalScheduler:
    """Run one async job on a fixed cadence without overlapping executions."""

    def __init__(self, interval_seconds: float) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be greater than zero")
        self.interval_seconds = interval_seconds

    async def run(
        self,
        job: ScheduledJob,
        *,
        stop_event: asyncio.Event | None = None,
        max_runs: int | None = None,
    ) -> IntervalScheduleResult:
        """Run immediately, then on cadence until stopped or max_runs is reached."""
        if max_runs is not None and max_runs < 1:
            raise ValueError("max_runs must be at least one")

        active_stop_event = stop_event or asyncio.Event()
        loop = asyncio.get_running_loop()
        next_due = loop.time()
        attempted_runs = 0
        completed_runs = 0
        skipped_runs = 0
        failed_runs = 0
        skipped_intervals = 0

        while not active_stop_event.is_set():
            delay = next_due - loop.time()
            if delay > 0 and await _wait_for_stop(active_stop_event, delay):
                break

            attempted_runs += 1
            try:
                executed = await job(attempted_runs)
            except asyncio.CancelledError:
                raise
            except Exception:
                failed_runs += 1
                logger.exception(
                    "scheduled job failed",
                    extra={"run_number": attempted_runs},
                )
            else:
                if executed is False:
                    skipped_runs += 1
                else:
                    completed_runs += 1

            if max_runs is not None and attempted_runs >= max_runs:
                break

            next_due += self.interval_seconds
            now = loop.time()
            if now > next_due:
                missed = int((now - next_due) // self.interval_seconds) + 1
                skipped_intervals += missed
                next_due += missed * self.interval_seconds

        return IntervalScheduleResult(
            interval_seconds=self.interval_seconds,
            attempted_runs=attempted_runs,
            completed_runs=completed_runs,
            skipped_runs=skipped_runs,
            failed_runs=failed_runs,
            skipped_intervals=skipped_intervals,
            stop_requested=active_stop_event.is_set(),
        )


async def _wait_for_stop(stop_event: asyncio.Event, timeout: float) -> bool:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=timeout)
    except TimeoutError:
        return False
    return True
