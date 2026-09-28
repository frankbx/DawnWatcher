"""Add runtime heartbeats and stateful operational alerts.

Revision ID: 0005_monitoring
Revises: 0004_market_sessions
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005_monitoring"
down_revision: str | None = "0004_market_sessions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create heartbeat and alert-state tables."""
    op.create_table(
        "runtime_heartbeat",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("service_name", sa.String(length=100), nullable=False),
        sa.Column("instance_id", sa.String(length=36), nullable=False),
        sa.Column("process_id", sa.Integer(), nullable=False),
        sa.Column("hostname", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("interval_seconds", sa.Float(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(), nullable=False),
        sa.Column("stopped_at", sa.DateTime(), nullable=True),
        sa.Column("details", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_runtime_heartbeat")),
        sa.UniqueConstraint("instance_id", name="uq_runtime_heartbeat_instance_id"),
    )
    op.create_index(
        "ix_runtime_heartbeat_service_status_time",
        "runtime_heartbeat",
        ["service_name", "status", "heartbeat_at"],
        unique=False,
    )

    op.create_table(
        "operational_alert",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("alert_key", sa.String(length=150), nullable=False),
        sa.Column("category", sa.String(length=50), nullable=False),
        sa.Column("severity", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("details", sa.JSON(), nullable=False),
        sa.Column("first_triggered_at", sa.DateTime(), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(), nullable=False),
        sa.Column("resolved_at", sa.DateTime(), nullable=True),
        sa.Column("occurrence_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_operational_alert")),
        sa.UniqueConstraint("alert_key", name="uq_operational_alert_alert_key"),
    )
    op.create_index(
        "ix_operational_alert_status_severity",
        "operational_alert",
        ["status", "severity"],
        unique=False,
    )


def downgrade() -> None:
    """Remove operational monitoring state."""
    op.drop_index("ix_operational_alert_status_severity", table_name="operational_alert")
    op.drop_table("operational_alert")
    op.drop_index(
        "ix_runtime_heartbeat_service_status_time",
        table_name="runtime_heartbeat",
    )
    op.drop_table("runtime_heartbeat")
