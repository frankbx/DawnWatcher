"""Recovery of interrupted jobs and notification deliveries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, sessionmaker

from dawnwatcher.domain.jobs import ACTIVE_JOB_STATUSES, JobStatus
from dawnwatcher.notifications.outbox import recover_stale_notifications
from dawnwatcher.storage.audit import append_audit_event
from dawnwatcher.storage.models import JobRun, utc_now


@dataclass(frozen=True, slots=True)
class RecoveryReport:
    """Counts of records recovered during application startup."""

    jobs_retried: int = 0
    jobs_missed: int = 0
    jobs_failed: int = 0
    notifications_recovered: int = 0


def recover_interrupted_jobs(
    session: Session, *, now: datetime | None = None
) -> tuple[int, int, int]:
    """Move jobs with expired leases into a safe restart state."""
    timestamp = now or utc_now()
    interrupted = session.scalars(
        select(JobRun).where(
            JobRun.status.in_(ACTIVE_JOB_STATUSES),
            or_(JobRun.lease_expires_at.is_(None), JobRun.lease_expires_at <= timestamp),
        )
    ).all()

    retried = missed = failed = 0
    for job in interrupted:
        previous_status = job.status
        if job.not_after is not None and job.not_after < timestamp:
            job.status = JobStatus.MISSED
            job.finished_at = timestamp
            missed += 1
        elif job.attempt_count < job.max_attempts:
            job.status = JobStatus.RETRYING
            retried += 1
        else:
            job.status = JobStatus.FAILED
            job.finished_at = timestamp
            failed += 1

        job.lease_expires_at = None
        job.error_code = "interrupted"
        job.error_message = "job lease expired before completion"
        append_audit_event(
            session,
            event_type="job.recovered",
            entity_type="job_run",
            entity_id=job.id,
            correlation_id=job.id,
            payload={
                "from_status": previous_status.value,
                "to_status": job.status.value,
                "attempt_count": job.attempt_count,
            },
            occurred_at=timestamp,
        )
    session.flush()
    return retried, missed, failed


def run_startup_recovery(
    factory: sessionmaker[Session], *, now: datetime | None = None
) -> RecoveryReport:
    """Recover all durable work in one atomic startup transaction."""
    timestamp = now or utc_now()
    with factory.begin() as session:
        retried, missed, failed = recover_interrupted_jobs(session, now=timestamp)
        notifications = recover_stale_notifications(session, now=timestamp)
    return RecoveryReport(
        jobs_retried=retried,
        jobs_missed=missed,
        jobs_failed=failed,
        notifications_recovered=notifications,
    )
