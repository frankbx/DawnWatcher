"""Durable notification delivery worker."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy.orm import Session, sessionmaker

from regimebeacon.notifications.feishu import FeishuDeliveryError, FeishuDeliveryReceipt
from regimebeacon.notifications.outbox import (
    claim_next_notification,
    mark_notification_expired,
    mark_notification_failed,
    mark_notification_sent,
    recover_stale_notifications,
)
from regimebeacon.storage.models import NotificationOutbox


class NotificationSender(Protocol):
    """External delivery operation used by the outbox worker."""

    async def send(self, notification: NotificationOutbox) -> FeishuDeliveryReceipt: ...


@dataclass(frozen=True, slots=True)
class DeliveryBatchResult:
    """Counters from one bounded outbox drain."""

    claimed: int = 0
    sent: int = 0
    failed: int = 0
    dead: int = 0
    recovered: int = 0

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


class NotificationDeliveryWorker:
    """Claim, send, and transactionally complete Feishu notifications."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        sender: NotificationSender,
        *,
        channel: str = "feishu",
        lease_seconds: int = 30,
        retry_base_seconds: int = 30,
        retry_max_seconds: int = 1_800,
    ) -> None:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        if retry_base_seconds < 1:
            raise ValueError("retry_base_seconds must be positive")
        if retry_max_seconds < retry_base_seconds:
            raise ValueError("retry_max_seconds cannot be less than retry_base_seconds")
        self.session_factory = session_factory
        self.sender = sender
        self.channel = channel
        self.lease_seconds = lease_seconds
        self.retry_base_seconds = retry_base_seconds
        self.retry_max_seconds = retry_max_seconds

    async def deliver_batch(self, *, max_items: int) -> DeliveryBatchResult:
        """Recover stale leases and deliver at most ``max_items`` notifications."""
        if max_items < 1:
            raise ValueError("max_items must be at least one")
        with self.session_factory.begin() as session:
            recovered = recover_stale_notifications(session)

        claimed = sent = failed = dead = 0
        for _ in range(max_items):
            with self.session_factory.begin() as session:
                claim = claim_next_notification(
                    session,
                    lease_seconds=self.lease_seconds,
                    channel=self.channel,
                )
            if claim is None:
                break
            claimed += 1
            if _is_expired(claim.notification):
                with self.session_factory.begin() as session:
                    mark_notification_expired(
                        session,
                        notification_id=claim.notification.id,
                        lock_token=claim.lock_token,
                    )
                dead += 1
                continue
            try:
                receipt = await self.sender.send(claim.notification)
            except FeishuDeliveryError as exc:
                error_message = str(exc)
                failed += 1
                became_dead = self._record_failure(
                    claim.notification, claim.lock_token, error_message
                )
                dead += int(became_dead)
            except Exception as exc:
                # Never persist an arbitrary exception string: it may embed the secret webhook URL.
                failed += 1
                error_message = f"unexpected delivery error: {type(exc).__name__}"
                became_dead = self._record_failure(
                    claim.notification, claim.lock_token, error_message
                )
                dead += int(became_dead)
            else:
                with self.session_factory.begin() as session:
                    mark_notification_sent(
                        session,
                        notification_id=claim.notification.id,
                        lock_token=claim.lock_token,
                        provider_message_id=receipt.provider_message_id,
                    )
                sent += 1
        return DeliveryBatchResult(
            claimed=claimed,
            sent=sent,
            failed=failed,
            dead=dead,
            recovered=recovered,
        )

    def _record_failure(
        self,
        notification: NotificationOutbox,
        lock_token: str,
        error_message: str,
    ) -> bool:
        exponent = max(0, notification.attempt_count - 1)
        retry_delay = min(self.retry_max_seconds, self.retry_base_seconds * 2**exponent)
        with self.session_factory.begin() as session:
            failed_notification = mark_notification_failed(
                session,
                notification_id=notification.id,
                lock_token=lock_token,
                error_message=error_message,
                retry_delay_seconds=retry_delay,
            )
            return failed_notification.status.value == "dead"


def _is_expired(notification: NotificationOutbox) -> bool:
    value = notification.payload.get("expires_at")
    if value is None:
        return False
    if not isinstance(value, str):
        return True
    try:
        expires_at = datetime.fromisoformat(value)
    except ValueError:
        return True
    if expires_at.tzinfo is None or expires_at.utcoffset() is None:
        return True
    return datetime.now(UTC) >= expires_at.astimezone(UTC)
