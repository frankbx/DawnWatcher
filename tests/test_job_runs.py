"""Durable job state-machine and recovery tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from regimebeacon.domain import JobStatus
from regimebeacon.ops.recovery import recover_interrupted_jobs
from regimebeacon.storage.models import AuditEvent, JobRun
from regimebeacon.workflows.job_runs import (
    InvalidJobTransition,
    create_job_run,
    transition_job,
)


def test_job_creation_is_idempotent_across_transactions(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    scheduled_for = datetime(2026, 9, 28, 1, 20, tzinfo=UTC)
    with session_factory_fixture.begin() as session:
        first = create_job_run(
            session,
            idempotency_key="intraday:2026-09-28:opening-v1",
            job_type="intraday_opening",
            scheduled_for=scheduled_for,
        )
        first_id = first.id

    with session_factory_fixture.begin() as session:
        second = create_job_run(
            session,
            idempotency_key="intraday:2026-09-28:opening-v1",
            job_type="intraday_opening",
            scheduled_for=scheduled_for,
        )
        count = session.scalar(select(func.count()).select_from(JobRun))

    assert second.id == first_id
    assert count == 1


def test_job_transitions_are_validated_and_audited(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    now = datetime(2026, 9, 28, 1, 20, tzinfo=UTC)
    with session_factory_fixture.begin() as session:
        job = create_job_run(
            session,
            idempotency_key="job-transition-test",
            job_type="test",
            scheduled_for=now,
        )
        transition_job(session, job, JobStatus.PREFLIGHT, now=now)
        transition_job(session, job, JobStatus.RUNNING, now=now)

        assert job.attempt_count == 1
        assert job.lease_expires_at == now + timedelta(seconds=60)
        with pytest.raises(InvalidJobTransition):
            transition_job(session, job, JobStatus.COMPLETE, now=now)

        audit_count = session.scalar(select(func.count()).select_from(AuditEvent))

    assert audit_count == 3


def test_startup_recovery_retries_misses_and_fails_expired_jobs(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    now = datetime(2026, 9, 28, 2, 0, tzinfo=UTC)
    expired = now - timedelta(minutes=5)

    with session_factory_fixture.begin() as session:
        retry_job = _running_job(
            session,
            key="recover-retry",
            now=expired,
            not_after=now + timedelta(minutes=5),
            max_attempts=3,
        )
        missed_job = _running_job(
            session,
            key="recover-missed",
            now=expired,
            not_after=now - timedelta(seconds=1),
            max_attempts=3,
        )
        failed_job = _running_job(
            session,
            key="recover-failed",
            now=expired,
            not_after=now + timedelta(minutes=5),
            max_attempts=1,
        )

        counts = recover_interrupted_jobs(session, now=now)

        assert counts == (1, 1, 1)
        assert retry_job.status is JobStatus.RETRYING
        assert missed_job.status is JobStatus.MISSED
        assert failed_job.status is JobStatus.FAILED
        assert retry_job.lease_expires_at is None


def _running_job(
    session: Session,
    *,
    key: str,
    now: datetime,
    not_after: datetime,
    max_attempts: int,
) -> JobRun:
    job = create_job_run(
        session,
        idempotency_key=key,
        job_type="recovery-test",
        scheduled_for=now,
        not_after=not_after,
        max_attempts=max_attempts,
    )
    transition_job(session, job, JobStatus.PREFLIGHT, now=now, lease_seconds=1)
    transition_job(session, job, JobStatus.RUNNING, now=now, lease_seconds=1)
    return job
