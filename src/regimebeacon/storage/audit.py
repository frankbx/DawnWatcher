"""Append-only audit helpers."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from regimebeacon.storage.models import AuditEvent, utc_now


def append_audit_event(
    session: Session,
    *,
    event_type: str,
    entity_type: str,
    entity_id: str,
    actor_type: str = "system",
    actor_id: str | None = None,
    correlation_id: str | None = None,
    payload: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
    occurred_at: datetime | None = None,
) -> AuditEvent:
    """Append an audit event, returning an existing event for a repeated key."""
    if idempotency_key is not None:
        existing = session.scalar(
            select(AuditEvent).where(AuditEvent.idempotency_key == idempotency_key)
        )
        if existing is not None:
            return existing

    event = AuditEvent(
        idempotency_key=idempotency_key,
        event_type=event_type,
        entity_type=entity_type,
        entity_id=entity_id,
        actor_type=actor_type,
        actor_id=actor_id,
        correlation_id=correlation_id,
        payload=payload or {},
        occurred_at=occurred_at or utc_now(),
    )
    session.add(event)
    session.flush()
    return event
