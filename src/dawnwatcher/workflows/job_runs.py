"""Durable job creation, transitions, leases, and heartbeats."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dawnwatcher.domain.jobs import ACTIVE_JOB_STATUSES, JobStatus, can_transition
from dawnwatcher.storage.audit import append_audit_event
from dawnwatcher.storage.models import JobRun, utc_now


class InvalidJobTransition(ValueError):
    """Raised when code attempts an illegal workflow transition."""


def create_job_run(
    session: Session,
    *,
    idempotency_key: str,
    job_type: str,
    scheduled_for: datetime,
    trade_date: date | None = None,
    not_after: datetime | None = None,
    max_attempts: int = 3,
    payload: dict[str, Any] | None = None,
) -> JobRun:
    """Create a job exactly once for a caller-defined idempotency key."""
    existing = session.scalar(select(JobRun).where(JobRun.idempotency_key == idempotency_key))
    if existing is not None:
        return existing
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least one")

    job = JobRun(
        idempotency_key=idempotency_key,
        job_type=job_type,
        trade_date=trade_date,
        scheduled_for=scheduled_for,
        not_after=not_after,
        max_attempts=max_attempts,
        payload=payload or {},
    )
    session.add(job)
    session.flush()
    append_audit_event(
        session,
        event_type="job.created",
        entity_type="job_run",
        entity_id=job.id,
        correlation_id=job.id,
        payload={"job_type": job_type, "idempotency_key": idempotency_key},
        idempotency_key=f"job-created:{idempotency_key}",
    )
    return job


def transition_job(
    session: Session,
    job: JobRun,
    to_status: JobStatus,
    *,
    now: datetime | None = None,
    lease_seconds: int = 60,
    error_code: str | None = None,
    error_message: str | None = None,
    actor_type: str = "system",
    actor_id: str | None = None,
) -> JobRun:
    """Apply a validated job state transition and append an audit event."""
    timestamp = now or utc_now()
    from_status = job.status
    if not can_transition(from_status, to_status):
        raise InvalidJobTransition(f"cannot transition job from {from_status} to {to_status}")
    if lease_seconds < 1:
        raise ValueError("lease_seconds must be positive")

    job.status = to_status
    job.error_code = error_code
    job.error_message = error_message

    if to_status is JobStatus.RUNNING:
        job.attempt_count += 1
        job.started_at = job.started_at or timestamp
    if to_status in ACTIVE_JOB_STATUSES:
        job.heartbeat_at = timestamp
        job.lease_expires_at = timestamp + timedelta(seconds=lease_seconds)
    else:
        job.lease_expires_at = None
    if to_status in {
        JobStatus.COMPLETE,
        JobStatus.BLOCKED,
        JobStatus.MISSED,
        JobStatus.FAILED,
    }:
        job.finished_at = timestamp

    append_audit_event(
        session,
        event_type="job.transitioned",
        entity_type="job_run",
        entity_id=job.id,
        actor_type=actor_type,
        actor_id=actor_id,
        correlation_id=job.id,
        payload={
            "from_status": from_status.value,
            "to_status": to_status.value,
            "attempt_count": job.attempt_count,
            "error_code": error_code,
        },
        occurred_at=timestamp,
    )
    session.flush()
    return job


def heartbeat_job(
    session: Session,
    job: JobRun,
    *,
    now: datetime | None = None,
    lease_seconds: int = 60,
) -> JobRun:
    """Extend the lease of an active job."""
    if job.status not in ACTIVE_JOB_STATUSES:
        raise InvalidJobTransition(f"cannot heartbeat job in {job.status} state")
    if lease_seconds < 1:
        raise ValueError("lease_seconds must be positive")

    timestamp = now or utc_now()
    job.heartbeat_at = timestamp
    job.lease_expires_at = timestamp + timedelta(seconds=lease_seconds)
    session.flush()
    return job
