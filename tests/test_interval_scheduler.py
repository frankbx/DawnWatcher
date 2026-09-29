"""Tests for non-overlapping fixed-interval scheduling."""

from __future__ import annotations

import asyncio

import pytest

from regimebeacon.workflows.interval import FixedIntervalScheduler


def test_scheduler_runs_jobs_without_overlap_and_skips_missed_slots() -> None:
    async def exercise():
        active_jobs = 0
        maximum_active_jobs = 0

        async def job(run_number: int) -> None:
            nonlocal active_jobs, maximum_active_jobs
            assert run_number in {1, 2, 3}
            active_jobs += 1
            maximum_active_jobs = max(maximum_active_jobs, active_jobs)
            await asyncio.sleep(0.015)
            active_jobs -= 1

        summary = await FixedIntervalScheduler(0.005).run(job, max_runs=3)
        return summary, maximum_active_jobs

    summary, maximum_active_jobs = asyncio.run(exercise())

    assert summary.attempted_runs == 3
    assert summary.completed_runs == 3
    assert summary.skipped_runs == 0
    assert summary.failed_runs == 0
    assert summary.skipped_intervals >= 2
    assert maximum_active_jobs == 1


def test_scheduler_stops_during_cadence_wait() -> None:
    async def exercise():
        stop_event = asyncio.Event()

        async def job(run_number: int) -> None:
            assert run_number == 1
            stop_event.set()

        return await FixedIntervalScheduler(10).run(job, stop_event=stop_event)

    summary = asyncio.run(exercise())

    assert summary.attempted_runs == 1
    assert summary.completed_runs == 1
    assert summary.skipped_runs == 0
    assert summary.stop_requested is True


def test_scheduler_records_failure_and_continues() -> None:
    async def exercise():
        async def job(run_number: int) -> None:
            if run_number == 1:
                raise RuntimeError("transient failure")

        return await FixedIntervalScheduler(0.001).run(job, max_runs=2)

    summary = asyncio.run(exercise())

    assert summary.attempted_runs == 2
    assert summary.completed_runs == 1
    assert summary.failed_runs == 1


def test_scheduler_counts_gate_skips_separately() -> None:
    async def exercise():
        async def job(run_number: int) -> bool:
            del run_number
            return False

        return await FixedIntervalScheduler(0.001).run(job, max_runs=2)

    summary = asyncio.run(exercise())

    assert summary.attempted_runs == 2
    assert summary.completed_runs == 0
    assert summary.skipped_runs == 2
    assert summary.failed_runs == 0


def test_scheduler_rejects_invalid_limits() -> None:
    with pytest.raises(ValueError, match="greater than zero"):
        FixedIntervalScheduler(0)

    async def exercise() -> None:
        async def job(run_number: int) -> None:
            del run_number

        await FixedIntervalScheduler(1).run(job, max_runs=0)

    with pytest.raises(ValueError, match="at least one"):
        asyncio.run(exercise())
