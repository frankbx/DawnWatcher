"""Notification delivery states."""

from enum import StrEnum


class NotificationStatus(StrEnum):
    """Lifecycle states for a transactional outbox item."""

    PENDING = "pending"
    SENDING = "sending"
    RETRYING = "retrying"
    SENT = "sent"
    DEAD = "dead"


DELIVERABLE_NOTIFICATION_STATUSES = frozenset(
    {NotificationStatus.PENDING, NotificationStatus.RETRYING}
)
TERMINAL_NOTIFICATION_STATUSES = frozenset({NotificationStatus.SENT, NotificationStatus.DEAD})
