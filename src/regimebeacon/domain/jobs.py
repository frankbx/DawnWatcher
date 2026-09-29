"""Durable job states and valid transitions."""

from __future__ import annotations

from enum import StrEnum


class JobStatus(StrEnum):
    """Lifecycle states for a durable workflow run."""

    SCHEDULED = "scheduled"
    PREFLIGHT = "preflight"
    RUNNING = "running"
    VALIDATING = "validating"
    PUBLISHED = "published"
    NOTIFIED = "notified"
    COMPLETE = "complete"
    RETRYING = "retrying"
    DEGRADED = "degraded"
    BLOCKED = "blocked"
    MISSED = "missed"
    FAILED = "failed"


ACTIVE_JOB_STATUSES = frozenset({JobStatus.PREFLIGHT, JobStatus.RUNNING, JobStatus.VALIDATING})
TERMINAL_JOB_STATUSES = frozenset(
    {JobStatus.COMPLETE, JobStatus.BLOCKED, JobStatus.MISSED, JobStatus.FAILED}
)

ALLOWED_JOB_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.SCHEDULED: frozenset(
        {JobStatus.PREFLIGHT, JobStatus.BLOCKED, JobStatus.MISSED, JobStatus.FAILED}
    ),
    JobStatus.PREFLIGHT: frozenset(
        {
            JobStatus.RUNNING,
            JobStatus.RETRYING,
            JobStatus.BLOCKED,
            JobStatus.MISSED,
            JobStatus.FAILED,
        }
    ),
    JobStatus.RUNNING: frozenset(
        {
            JobStatus.VALIDATING,
            JobStatus.RETRYING,
            JobStatus.DEGRADED,
            JobStatus.BLOCKED,
            JobStatus.MISSED,
            JobStatus.FAILED,
        }
    ),
    JobStatus.VALIDATING: frozenset(
        {
            JobStatus.PUBLISHED,
            JobStatus.RETRYING,
            JobStatus.DEGRADED,
            JobStatus.BLOCKED,
            JobStatus.MISSED,
            JobStatus.FAILED,
        }
    ),
    JobStatus.PUBLISHED: frozenset(
        {JobStatus.NOTIFIED, JobStatus.COMPLETE, JobStatus.RETRYING, JobStatus.FAILED}
    ),
    JobStatus.NOTIFIED: frozenset({JobStatus.COMPLETE, JobStatus.FAILED}),
    JobStatus.RETRYING: frozenset(
        {
            JobStatus.PREFLIGHT,
            JobStatus.RUNNING,
            JobStatus.BLOCKED,
            JobStatus.MISSED,
            JobStatus.FAILED,
        }
    ),
    JobStatus.DEGRADED: frozenset(
        {JobStatus.PUBLISHED, JobStatus.NOTIFIED, JobStatus.COMPLETE, JobStatus.BLOCKED}
    ),
    JobStatus.COMPLETE: frozenset(),
    JobStatus.BLOCKED: frozenset(),
    JobStatus.MISSED: frozenset(),
    JobStatus.FAILED: frozenset(),
}


def can_transition(from_status: JobStatus, to_status: JobStatus) -> bool:
    """Return whether a job state transition is allowed."""
    return to_status in ALLOWED_JOB_TRANSITIONS[from_status]
