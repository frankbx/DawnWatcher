"""SQLAlchemy models for the durable Phase 1 foundation."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy import (
    Enum as SAEnum,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from dawnwatcher.domain import JobStatus, NotificationStatus
from dawnwatcher.storage.types import UTCDateTime

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


def utc_now() -> datetime:
    """Return the current timezone-aware UTC timestamp."""
    return datetime.now(UTC)


def new_id() -> str:
    """Return a portable UUID identifier."""
    return str(uuid4())


class Base(DeclarativeBase):
    """Declarative model base with deterministic constraint names."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class TimestampMixin:
    """Creation and update timestamps for mutable records."""

    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, onupdate=utc_now, nullable=False
    )


class JobRun(TimestampMixin, Base):
    """One idempotent execution of a scheduled workflow."""

    __tablename__ = "job_run"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_job_run_idempotency_key"),
        Index("ix_job_run_status_scheduled_for", "status", "scheduled_for"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    job_type: Mapped[str] = mapped_column(String(100), nullable=False)
    trade_date: Mapped[date | None] = mapped_column(Date(), nullable=True)
    status: Mapped[JobStatus] = mapped_column(
        SAEnum(
            JobStatus,
            native_enum=False,
            values_callable=lambda enum_type: [member.value for member in enum_type],
            length=32,
        ),
        default=JobStatus.SCHEDULED,
        nullable=False,
    )
    attempt_count: Mapped[int] = mapped_column(Integer(), default=0, nullable=False)
    max_attempts: Mapped[int] = mapped_column(Integer(), default=3, nullable=False)
    scheduled_for: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    not_after: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text(), nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON(), default=dict, nullable=False)


class NotificationOutbox(TimestampMixin, Base):
    """A notification committed atomically with the event that produced it."""

    __tablename__ = "notification_outbox"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_notification_outbox_idempotency_key"),
        Index(
            "ix_notification_outbox_delivery",
            "status",
            "next_attempt_at",
            "created_at",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    channel: Mapped[str] = mapped_column(String(50), nullable=False)
    recipient: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[NotificationStatus] = mapped_column(
        SAEnum(
            NotificationStatus,
            native_enum=False,
            values_callable=lambda enum_type: [member.value for member in enum_type],
            length=32,
        ),
        default=NotificationStatus.PENDING,
        nullable=False,
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer(), default=0, nullable=False)
    max_attempts: Mapped[int] = mapped_column(Integer(), default=5, nullable=False)
    next_attempt_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False
    )
    lock_token: Mapped[str | None] = mapped_column(String(36), nullable=True)
    locked_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    lock_expires_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    provider_message_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text(), nullable=True)

    attempts: Mapped[list[NotificationAttempt]] = relationship(
        back_populates="notification", cascade="all, delete-orphan"
    )


class NotificationAttempt(Base):
    """Immutable result of one external notification delivery attempt."""

    __tablename__ = "notification_attempt"
    __table_args__ = (
        UniqueConstraint(
            "notification_id",
            "attempt_number",
            name="uq_notification_attempt_number",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    notification_id: Mapped[str] = mapped_column(
        ForeignKey("notification_outbox.id", ondelete="CASCADE"), nullable=False
    )
    attempt_number: Mapped[int] = mapped_column(Integer(), nullable=False)
    started_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    finished_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    success: Mapped[bool] = mapped_column(Boolean(), nullable=False)
    provider_message_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text(), nullable=True)

    notification: Mapped[NotificationOutbox] = relationship(back_populates="attempts")


class AuditEvent(Base):
    """Append-only audit record for state-changing operations."""

    __tablename__ = "audit_event"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_audit_event_idempotency_key"),
        Index("ix_audit_event_entity", "entity_type", "entity_id", "occurred_at"),
        Index("ix_audit_event_correlation", "correlation_id", "occurred_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(100), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(100), nullable=False)
    actor_type: Mapped[str] = mapped_column(String(50), default="system", nullable=False)
    actor_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    correlation_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON(), default=dict, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)
