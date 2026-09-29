"""SQLAlchemy models for the durable Phase 1 foundation."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Date,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy import (
    Enum as SAEnum,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from dawnwatcher.domain import (
    DataQualityState,
    Exchange,
    JobStatus,
    NotificationStatus,
    QuoteProvider,
)
from dawnwatcher.market import MarketPhase
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


class MarketCollectionRun(Base):
    """Metadata for one idempotent Tencent collection cycle."""

    __tablename__ = "market_collection_run"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_market_collection_run_idempotency_key"),
        Index("ix_market_collection_run_started_at", "started_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    expected_trade_date: Mapped[date | None] = mapped_column(Date(), nullable=True)
    market_phase: Mapped[MarketPhase | None] = mapped_column(
        SAEnum(
            MarketPhase,
            native_enum=False,
            values_callable=lambda enum_type: [member.value for member in enum_type],
            length=32,
        ),
        nullable=True,
    )
    requested_symbols: Mapped[list[str]] = mapped_column(JSON(), nullable=False)
    started_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    finished_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    provider_summaries: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    quality_counts: Mapped[dict[str, int]] = mapped_column(JSON(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)


class TradingCalendarDay(Base):
    """Locally cached Tushare trade_cal row used by the runtime gate."""

    __tablename__ = "trading_calendar_day"
    __table_args__ = (
        UniqueConstraint("exchange", "cal_date", name="uq_trading_calendar_day_exchange_date"),
        Index("ix_trading_calendar_day_date_open", "cal_date", "is_open"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    exchange: Mapped[str] = mapped_column(String(10), nullable=False)
    cal_date: Mapped[date] = mapped_column(Date(), nullable=False)
    is_open: Mapped[bool] = mapped_column(Boolean(), nullable=False)
    pretrade_date: Mapped[date | None] = mapped_column(Date(), nullable=True)
    source: Mapped[str] = mapped_column(String(20), default="tushare", nullable=False)
    source_fetched_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, onupdate=utc_now, nullable=False
    )


class RuntimeHeartbeat(TimestampMixin, Base):
    """Durable liveness record for one long-running process instance."""

    __tablename__ = "runtime_heartbeat"
    __table_args__ = (
        UniqueConstraint("instance_id", name="uq_runtime_heartbeat_instance_id"),
        Index(
            "ix_runtime_heartbeat_service_status_time",
            "service_name",
            "status",
            "heartbeat_at",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    service_name: Mapped[str] = mapped_column(String(100), nullable=False)
    instance_id: Mapped[str] = mapped_column(String(36), nullable=False)
    process_id: Mapped[int] = mapped_column(Integer(), nullable=False)
    hostname: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    interval_seconds: Mapped[float] = mapped_column(Float(), nullable=False)
    started_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    heartbeat_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    stopped_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    details: Mapped[dict[str, Any]] = mapped_column(JSON(), default=dict, nullable=False)


class OperationalAlert(TimestampMixin, Base):
    """Stateful operational alert used to suppress duplicate notifications."""

    __tablename__ = "operational_alert"
    __table_args__ = (
        UniqueConstraint("alert_key", name="uq_operational_alert_alert_key"),
        Index("ix_operational_alert_status_severity", "status", "severity"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    alert_key: Mapped[str] = mapped_column(String(150), nullable=False)
    category: Mapped[str] = mapped_column(String(50), nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    summary: Mapped[str] = mapped_column(Text(), nullable=False)
    details: Mapped[dict[str, Any]] = mapped_column(JSON(), default=dict, nullable=False)
    first_triggered_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    last_observed_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    occurrence_count: Mapped[int] = mapped_column(Integer(), default=1, nullable=False)


class ProviderQuoteSnapshot(Base):
    """Normalized but provider-isolated quote snapshot."""

    __tablename__ = "provider_quote_snapshot"
    __table_args__ = (
        UniqueConstraint(
            "collection_id",
            "provider",
            "symbol",
            name="uq_provider_quote_snapshot_collection_provider_symbol",
        ),
        Index(
            "ix_provider_quote_snapshot_symbol_time",
            "symbol",
            "quote_at",
            "provider",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    collection_id: Mapped[str] = mapped_column(
        ForeignKey("market_collection_run.id", ondelete="CASCADE"), nullable=False
    )
    provider: Mapped[QuoteProvider] = mapped_column(
        SAEnum(
            QuoteProvider,
            native_enum=False,
            values_callable=lambda enum_type: [member.value for member in enum_type],
            length=20,
        ),
        nullable=False,
    )
    symbol: Mapped[str] = mapped_column(String(9), nullable=False)
    exchange: Mapped[Exchange] = mapped_column(
        SAEnum(
            Exchange,
            native_enum=False,
            values_callable=lambda enum_type: [member.value for member in enum_type],
            length=10,
        ),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    quote_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    open: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    previous_close: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    latest: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    high: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    low: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    volume_shares: Mapped[int] = mapped_column(BigInteger(), nullable=False)
    amount_cny: Mapped[Decimal] = mapped_column(Numeric(24, 4), nullable=False)
    bid1_price: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    bid1_volume_shares: Mapped[int] = mapped_column(BigInteger(), nullable=False)
    ask1_price: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    ask1_volume_shares: Mapped[int] = mapped_column(BigInteger(), nullable=False)
    volume_precision_shares: Mapped[int] = mapped_column(Integer(), nullable=False)
    raw_field_count: Mapped[int] = mapped_column(Integer(), nullable=False)
    validation_issues: Mapped[list[dict[str, Any]]] = mapped_column(JSON(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)


class ReconciledQuoteSnapshot(Base):
    """Decision-facing data quality for one symbol and collection cycle."""

    __tablename__ = "reconciled_quote_snapshot"
    __table_args__ = (
        UniqueConstraint(
            "collection_id",
            "symbol",
            name="uq_reconciled_quote_snapshot_collection_symbol",
        ),
        Index(
            "ix_reconciled_quote_snapshot_symbol_state",
            "symbol",
            "quality_state",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    collection_id: Mapped[str] = mapped_column(
        ForeignKey("market_collection_run.id", ondelete="CASCADE"), nullable=False
    )
    symbol: Mapped[str] = mapped_column(String(9), nullable=False)
    exchange: Mapped[Exchange] = mapped_column(
        SAEnum(
            Exchange,
            native_enum=False,
            values_callable=lambda enum_type: [member.value for member in enum_type],
            length=10,
        ),
        nullable=False,
    )
    quality_state: Mapped[DataQualityState] = mapped_column(
        SAEnum(
            DataQualityState,
            native_enum=False,
            values_callable=lambda enum_type: [member.value for member in enum_type],
            length=20,
        ),
        nullable=False,
    )
    selected_provider: Mapped[QuoteProvider | None] = mapped_column(
        SAEnum(
            QuoteProvider,
            native_enum=False,
            values_callable=lambda enum_type: [member.value for member in enum_type],
            length=20,
        ),
        nullable=True,
    )
    comparisons: Mapped[list[dict[str, Any]]] = mapped_column(JSON(), nullable=False)
    reasons: Mapped[list[str]] = mapped_column(JSON(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)


class MinuteBar(TimestampMixin, Base):
    """One auditable minute bar derived from validated quote snapshots."""

    __tablename__ = "minute_bar"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "symbol",
            "minute_start",
            name="uq_minute_bar_provider_symbol_start",
        ),
        Index("ix_minute_bar_symbol_trade_time", "symbol", "trade_date", "minute_start"),
        Index("ix_minute_bar_trade_time", "trade_date", "minute_start"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    provider: Mapped[QuoteProvider] = mapped_column(
        SAEnum(
            QuoteProvider,
            native_enum=False,
            values_callable=lambda enum_type: [member.value for member in enum_type],
            length=20,
        ),
        nullable=False,
    )
    symbol: Mapped[str] = mapped_column(String(9), nullable=False)
    exchange: Mapped[Exchange] = mapped_column(
        SAEnum(
            Exchange,
            native_enum=False,
            values_callable=lambda enum_type: [member.value for member in enum_type],
            length=10,
        ),
        nullable=False,
    )
    trade_date: Mapped[date] = mapped_column(Date(), nullable=False)
    minute_start: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    minute_end: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    open: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    high: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    low: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    close: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    cumulative_volume_start: Mapped[int | None] = mapped_column(BigInteger(), nullable=True)
    cumulative_volume_end: Mapped[int] = mapped_column(BigInteger(), nullable=False)
    volume_shares: Mapped[int | None] = mapped_column(BigInteger(), nullable=True)
    cumulative_amount_start: Mapped[Decimal | None] = mapped_column(Numeric(24, 4), nullable=True)
    cumulative_amount_end: Mapped[Decimal] = mapped_column(Numeric(24, 4), nullable=False)
    amount_cny: Mapped[Decimal | None] = mapped_column(Numeric(24, 4), nullable=True)
    vwap: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    sample_count: Mapped[int] = mapped_column(Integer(), nullable=False)
    expected_sample_count: Mapped[int] = mapped_column(Integer(), nullable=False)
    coverage_ratio: Mapped[Decimal] = mapped_column(Numeric(10, 6), nullable=False)
    first_quote_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    last_quote_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    quality_flags: Mapped[list[str]] = mapped_column(JSON(), default=list, nullable=False)


class MinuteFeature(TimestampMixin, Base):
    """Decision-facing features calculated from one persisted minute bar."""

    __tablename__ = "minute_feature"
    __table_args__ = (
        UniqueConstraint("minute_bar_id", name="uq_minute_feature_minute_bar_id"),
        Index("ix_minute_feature_market_benchmark", "market_benchmark_symbol"),
        Index("ix_minute_feature_industry_benchmark", "industry_benchmark_symbol"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    minute_bar_id: Mapped[str] = mapped_column(
        ForeignKey("minute_bar.id", ondelete="CASCADE"), nullable=False
    )
    price_trend_bps: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    vwap_deviation_bps: Mapped[Decimal | None] = mapped_column(Numeric(20, 6), nullable=True)
    relative_volume_ratio: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    relative_volume_history_days: Mapped[int] = mapped_column(Integer(), nullable=False)
    market_benchmark_symbol: Mapped[str | None] = mapped_column(String(9), nullable=True)
    market_relative_strength_bps: Mapped[Decimal | None] = mapped_column(
        Numeric(20, 6), nullable=True
    )
    industry_benchmark_symbol: Mapped[str | None] = mapped_column(String(9), nullable=True)
    industry_relative_strength_bps: Mapped[Decimal | None] = mapped_column(
        Numeric(20, 6), nullable=True
    )
    quality_flags: Mapped[list[str]] = mapped_column(JSON(), default=list, nullable=False)
