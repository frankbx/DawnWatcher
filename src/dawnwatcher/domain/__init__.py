"""Domain models and state machines."""

from dawnwatcher.domain.jobs import JobStatus
from dawnwatcher.domain.notifications import NotificationStatus

__all__ = ["JobStatus", "NotificationStatus"]
