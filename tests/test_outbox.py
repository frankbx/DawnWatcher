"""Transactional outbox and delivery recovery tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from regimebeacon.domain import NotificationStatus
from regimebeacon.notifications.outbox import (
    NotificationClaimError,
    claim_next_notification,
    enqueue_notification,
    mark_notification_failed,
    mark_notification_sent,
    recover_stale_notifications,
)
from regimebeacon.storage.models import NotificationAttempt, NotificationOutbox


def test_enqueue_is_idempotent_and_success_is_auditable(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    now = datetime(2026, 9, 28, 1, 45, tzinfo=UTC)
    with session_factory_fixture.begin() as session:
        first = enqueue_notification(
            session,
            idempotency_key="decision-1:feishu",
            event_type="decision.published",
            channel="feishu",
            recipient="user-1",
            payload={"decision_id": "decision-1"},
            available_at=now,
        )
        duplicate = enqueue_notification(
            session,
            idempotency_key="decision-1:feishu",
            event_type="decision.published",
            channel="feishu",
            recipient="user-1",
            payload={"decision_id": "different-payload-is-ignored"},
            available_at=now,
        )
        assert duplicate.id == first.id

        claim = claim_next_notification(session, now=now)
        assert claim is not None
        sent = mark_notification_sent(
            session,
            notification_id=claim.notification.id,
            lock_token=claim.lock_token,
            provider_message_id="feishu-message-1",
            now=now + timedelta(seconds=1),
        )
        count = session.scalar(select(func.count()).select_from(NotificationOutbox))
        attempts = session.scalar(select(func.count()).select_from(NotificationAttempt))

    assert sent.status is NotificationStatus.SENT
    assert count == 1
    assert attempts == 1


def test_failed_delivery_retries_then_dead_letters(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    now = datetime(2026, 9, 28, 1, 45, tzinfo=UTC)
    with session_factory_fixture.begin() as session:
        notification = enqueue_notification(
            session,
            idempotency_key="decision-2:feishu",
            event_type="decision.published",
            channel="feishu",
            recipient="user-1",
            payload={},
            max_attempts=2,
            available_at=now,
        )
        first_claim = claim_next_notification(session, now=now)
        assert first_claim is not None
        mark_notification_failed(
            session,
            notification_id=notification.id,
            lock_token=first_claim.lock_token,
            error_message="temporary failure",
            retry_delay_seconds=10,
            now=now,
        )
        assert notification.status is NotificationStatus.RETRYING
        assert claim_next_notification(session, now=now + timedelta(seconds=9)) is None

        second_claim = claim_next_notification(session, now=now + timedelta(seconds=10))
        assert second_claim is not None
        mark_notification_failed(
            session,
            notification_id=notification.id,
            lock_token=second_claim.lock_token,
            error_message="permanent failure",
            now=now + timedelta(seconds=11),
        )

    assert notification.status is NotificationStatus.DEAD
    assert notification.attempt_count == 2


def test_claim_token_is_required(session_factory_fixture: sessionmaker[Session]) -> None:
    now = datetime(2026, 9, 28, 1, 45, tzinfo=UTC)
    with session_factory_fixture.begin() as session:
        enqueue_notification(
            session,
            idempotency_key="decision-3:feishu",
            event_type="decision.published",
            channel="feishu",
            recipient="user-1",
            payload={},
            available_at=now,
        )
        claim = claim_next_notification(session, now=now)
        assert claim is not None
        with pytest.raises(NotificationClaimError):
            mark_notification_sent(
                session,
                notification_id=claim.notification.id,
                lock_token="wrong-token",
                now=now,
            )


def test_expired_delivery_lease_is_recovered(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    now = datetime(2026, 9, 28, 1, 45, tzinfo=UTC)
    with session_factory_fixture.begin() as session:
        notification = enqueue_notification(
            session,
            idempotency_key="decision-4:feishu",
            event_type="decision.published",
            channel="feishu",
            recipient="user-1",
            payload={},
            available_at=now,
        )
        claim = claim_next_notification(session, now=now, lease_seconds=1)
        assert claim is not None

        recovered = recover_stale_notifications(session, now=now + timedelta(seconds=2))

    assert recovered == 1
    assert notification.status is NotificationStatus.RETRYING
    assert notification.lock_token is None
