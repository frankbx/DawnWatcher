"""Index the time ranges used by incremental minute-feature builds.

Revision ID: 0007_minute_query_indexes
Revises: 0006_minute_features
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0007_minute_query_indexes"
down_revision: str | None = "0006_minute_features"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Avoid full snapshot scans and repeated historical minute scans."""
    op.create_index(
        "ix_provider_quote_snapshot_provider_fetched",
        "provider_quote_snapshot",
        ["provider", "fetched_at"],
    )
    op.create_index(
        "ix_minute_bar_provider_start",
        "minute_bar",
        ["provider", "minute_start"],
    )


def downgrade() -> None:
    """Remove incremental-analysis indexes."""
    op.drop_index("ix_minute_bar_provider_start", table_name="minute_bar")
    op.drop_index(
        "ix_provider_quote_snapshot_provider_fetched", table_name="provider_quote_snapshot"
    )
