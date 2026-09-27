"""Transactional notification outbox operations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from sqlalchemy import and_, or_, select, update
from sqlalchemy.orm import Session

from dawnwatcher.domain.notifications import (
    DELIVERABLE_NOTIFICATION_STATUSES,
    NotificationStatus,
)
from dawnwatcher.storage.audit import append_audit_event
from dawnwatcher.storage.models import NotificationAttempt, NotificationOutbox, utc_now


class NotificationClaimError(RuntimeError):
    """Raised when a worker attempts to complete a claim it does not own."""


@dataclass(frozen=True, slots=True)
class NotificationClaim:
    """A leased notification and the token required to complete it."""

    notification: NotificationOutbox
    lock_token: str


def enqueue_notification(
    session: Session,
    *,
    idempotency_key: str,
    event_type: str,
    channel: str,
    recipient: str,
    payload: dict[str, Any],
    max_attempts: int = 5,
    available_at: datetime | None = None,
) -> NotificationOutbox:
    """Enqueue a notification exactly once for the supplied key."""
    existing = session.scalar(
        select(NotificationOutbox).where(NotificationOutbox.idempotency_key == idempotency_key)
    )
    if existing is not None:
        return existing
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least one")

    notification = NotificationOutbox(
        idempotency_key=idempotency_key,
        event_type=event_type,
        channel=channel,
        recipient=recipient,
        payload=payload,
        max_attempts=max_attempts,
        next_attempt_at=available_at or utc_now(),
    )
    session.add(notification)
    session.flush()
    append_audit_event(
        session,
        event_type="notification.enqueued",
        entity_type="notification_outbox",
        entity_id=notification.id,
        correlation_id=notification.id,
        payload={"event_type": event_type, "channel": channel},
        idempotency_key=f"notification-enqueued:{idempotency_key}",
    )
    return notification


def claim_next_notification(
    session: Session,
    *,
    now: datetime | None = None,
    lease_seconds: int = 30,
) -> NotificationClaim | None:
    """Atomically lease the oldest deliverable notification."""
    if lease_seconds < 1:
        raise ValueError("lease_seconds must be positive")
    timestamp = now or utc_now()

    candidate = session.scalar(
        select(NotificationOutbox)
        .where(
            NotificationOutbox.status.in_(DELIVERABLE_NOTIFICATION_STATUSES),
            NotificationOutbox.next_attempt_at <= timestamp,
        )
        .order_by(NotificationOutbox.next_attempt_at, NotificationOutbox.created_at)
        .limit(1)
    )
    if candidate is None:
        return None

    lock_token = str(uuid4())
    previous_status = candidate.status
    result = session.connection().execute(
        update(NotificationOutbox)
        .where(
            NotificationOutbox.id == candidate.id,
            NotificationOutbox.status == previous_status,
        )
        .values(
            status=NotificationStatus.SENDING,
            attempt_count=NotificationOutbox.attempt_count + 1,
            lock_token=lock_token,
            locked_at=timestamp,
            lock_expires_at=timestamp + timedelta(seconds=lease_seconds),
        )
    )
    if result.rowcount != 1:
        session.expire(candidate)
        return None
    session.flush()
    session.refresh(candidate)
    return NotificationClaim(notification=candidate, lock_token=lock_token)


def mark_notification_sent(
    session: Session,
    *,
    notification_id: str,
    lock_token: str,
    provider_message_id: str | None = None,
    now: datetime | None = None,
) -> NotificationOutbox:
    """Finish a claimed delivery successfully."""
    timestamp = now or utc_now()
    notification = _get_owned_claim(session, notification_id, lock_token)
    attempt_started_at = notification.locked_at or timestamp

    notification.status = NotificationStatus.SENT
    notification.sent_at = timestamp
    notification.provider_message_id = provider_message_id
    notification.last_error = None
    _clear_lock(notification)
    session.add(
        NotificationAttempt(
            notification_id=notification.id,
            attempt_number=notification.attempt_count,
            started_at=attempt_started_at,
            finished_at=timestamp,
            success=True,
            provider_message_id=provider_message_id,
        )
    )
    append_audit_event(
        session,
        event_type="notification.sent",
        entity_type="notification_outbox",
        entity_id=notification.id,
        correlation_id=notification.id,
        payload={"provider_message_id": provider_message_id},
        occurred_at=timestamp,
    )
    session.flush()
    return notification


def mark_notification_failed(
    session: Session,
    *,
    notification_id: str,
    lock_token: str,
    error_message: str,
    retry_delay_seconds: int = 30,
    now: datetime | None = None,
) -> NotificationOutbox:
    """Record a failed attempt and either retry later or dead-letter the item."""
    if retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds cannot be negative")
    timestamp = now or utc_now()
    notification = _get_owned_claim(session, notification_id, lock_token)
    attempt_started_at = notification.locked_at or timestamp

    notification.last_error = error_message
    if notification.attempt_count >= notification.max_attempts:
        notification.status = NotificationStatus.DEAD
    else:
        notification.status = NotificationStatus.RETRYING
        notification.next_attempt_at = timestamp + timedelta(seconds=retry_delay_seconds)
    _clear_lock(notification)
    session.add(
        NotificationAttempt(
            notification_id=notification.id,
            attempt_number=notification.attempt_count,
            started_at=attempt_started_at,
            finished_at=timestamp,
            success=False,
            error_message=error_message,
        )
    )
    append_audit_event(
        session,
        event_type="notification.failed",
        entity_type="notification_outbox",
        entity_id=notification.id,
        correlation_id=notification.id,
        payload={
            "attempt_count": notification.attempt_count,
            "next_status": notification.status.value,
            "error_message": error_message,
        },
        occurred_at=timestamp,
    )
    session.flush()
    return notification


def recover_stale_notifications(
    session: Session,
    *,
    now: datetime | None = None,
) -> int:
    """Release expired delivery leases after a worker crash."""
    timestamp = now or utc_now()
    stale = session.scalars(
        select(NotificationOutbox).where(
            NotificationOutbox.status == NotificationStatus.SENDING,
            or_(
                NotificationOutbox.lock_expires_at.is_(None),
                NotificationOutbox.lock_expires_at <= timestamp,
            ),
        )
    ).all()

    for notification in stale:
        notification.status = (
            NotificationStatus.DEAD
            if notification.attempt_count >= notification.max_attempts
            else NotificationStatus.RETRYING
        )
        notification.next_attempt_at = timestamp
        notification.last_error = "delivery lease expired during worker interruption"
        _clear_lock(notification)
        append_audit_event(
            session,
            event_type="notification.recovered",
            entity_type="notification_outbox",
            entity_id=notification.id,
            correlation_id=notification.id,
            payload={"next_status": notification.status.value},
            occurred_at=timestamp,
        )
    session.flush()
    return len(stale)


def _get_owned_claim(session: Session, notification_id: str, lock_token: str) -> NotificationOutbox:
    notification = session.scalar(
        select(NotificationOutbox).where(
            and_(
                NotificationOutbox.id == notification_id,
                NotificationOutbox.status == NotificationStatus.SENDING,
                NotificationOutbox.lock_token == lock_token,
            )
        )
    )
    if notification is None:
        raise NotificationClaimError(
            "notification claim is missing, stale, or owned by another worker"
        )
    return notification


def _clear_lock(notification: NotificationOutbox) -> None:
    notification.lock_token = None
    notification.locked_at = None
    notification.lock_expires_at = None
