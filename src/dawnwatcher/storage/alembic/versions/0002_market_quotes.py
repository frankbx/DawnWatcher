"""Add provider-isolated and reconciled market quote snapshots.

Revision ID: 0002_market_quotes
Revises: 0001_phase1
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_market_quotes"
down_revision: str | None = "0001_phase1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create collection, provider snapshot, and reconciliation tables."""
    op.create_table(
        "market_collection_run",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("expected_trade_date", sa.Date(), nullable=True),
        sa.Column("requested_symbols", sa.JSON(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=False),
        sa.Column("provider_summaries", sa.JSON(), nullable=False),
        sa.Column("quality_counts", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_market_collection_run")),
        sa.UniqueConstraint("idempotency_key", name="uq_market_collection_run_idempotency_key"),
    )
    op.create_index(
        "ix_market_collection_run_started_at",
        "market_collection_run",
        ["started_at"],
        unique=False,
    )

    op.create_table(
        "provider_quote_snapshot",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("collection_id", sa.String(length=36), nullable=False),
        sa.Column("provider", sa.String(length=20), nullable=False),
        sa.Column("symbol", sa.String(length=6), nullable=False),
        sa.Column("exchange", sa.String(length=10), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("quote_at", sa.DateTime(), nullable=False),
        sa.Column("fetched_at", sa.DateTime(), nullable=False),
        sa.Column("open", sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column("previous_close", sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column("latest", sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column("high", sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column("low", sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column("volume_shares", sa.BigInteger(), nullable=False),
        sa.Column("amount_cny", sa.Numeric(precision=24, scale=4), nullable=False),
        sa.Column("bid1_price", sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column("bid1_volume_shares", sa.BigInteger(), nullable=False),
        sa.Column("ask1_price", sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column("ask1_volume_shares", sa.BigInteger(), nullable=False),
        sa.Column("volume_precision_shares", sa.Integer(), nullable=False),
        sa.Column("raw_field_count", sa.Integer(), nullable=False),
        sa.Column("validation_issues", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["collection_id"],
            ["market_collection_run.id"],
            name=op.f("fk_provider_quote_snapshot_collection_id_market_collection_run"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_provider_quote_snapshot")),
        sa.UniqueConstraint(
            "collection_id",
            "provider",
            "symbol",
            name="uq_provider_quote_snapshot_collection_provider_symbol",
        ),
    )
    op.create_index(
        "ix_provider_quote_snapshot_symbol_time",
        "provider_quote_snapshot",
        ["symbol", "quote_at", "provider"],
        unique=False,
    )

    op.create_table(
        "reconciled_quote_snapshot",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("collection_id", sa.String(length=36), nullable=False),
        sa.Column("symbol", sa.String(length=6), nullable=False),
        sa.Column("exchange", sa.String(length=10), nullable=False),
        sa.Column("quality_state", sa.String(length=20), nullable=False),
        sa.Column("selected_provider", sa.String(length=20), nullable=True),
        sa.Column("comparisons", sa.JSON(), nullable=False),
        sa.Column("reasons", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["collection_id"],
            ["market_collection_run.id"],
            name=op.f("fk_reconciled_quote_snapshot_collection_id_market_collection_run"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_reconciled_quote_snapshot")),
        sa.UniqueConstraint(
            "collection_id",
            "symbol",
            name="uq_reconciled_quote_snapshot_collection_symbol",
        ),
    )
    op.create_index(
        "ix_reconciled_quote_snapshot_symbol_state",
        "reconciled_quote_snapshot",
        ["symbol", "quality_state"],
        unique=False,
    )


def downgrade() -> None:
    """Remove Phase 2 quote storage."""
    op.drop_index(
        "ix_reconciled_quote_snapshot_symbol_state",
        table_name="reconciled_quote_snapshot",
    )
    op.drop_table("reconciled_quote_snapshot")
    op.drop_index("ix_provider_quote_snapshot_symbol_time", table_name="provider_quote_snapshot")
    op.drop_table("provider_quote_snapshot")
    op.drop_index("ix_market_collection_run_started_at", table_name="market_collection_run")
    op.drop_table("market_collection_run")
