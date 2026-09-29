"""Add auditable minute bars and derived intraday features.

Revision ID: 0006_minute_features
Revises: 0005_monitoring
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006_minute_features"
down_revision: str | None = "0005_monitoring"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the minute data and feature tables."""
    op.create_table(
        "minute_bar",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("provider", sa.String(length=20), nullable=False),
        sa.Column("symbol", sa.String(length=9), nullable=False),
        sa.Column("exchange", sa.String(length=10), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("minute_start", sa.DateTime(), nullable=False),
        sa.Column("minute_end", sa.DateTime(), nullable=False),
        sa.Column("open", sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column("high", sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column("low", sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column("close", sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column("cumulative_volume_start", sa.BigInteger(), nullable=True),
        sa.Column("cumulative_volume_end", sa.BigInteger(), nullable=False),
        sa.Column("volume_shares", sa.BigInteger(), nullable=True),
        sa.Column("cumulative_amount_start", sa.Numeric(precision=24, scale=4), nullable=True),
        sa.Column("cumulative_amount_end", sa.Numeric(precision=24, scale=4), nullable=False),
        sa.Column("amount_cny", sa.Numeric(precision=24, scale=4), nullable=True),
        sa.Column("vwap", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("sample_count", sa.Integer(), nullable=False),
        sa.Column("expected_sample_count", sa.Integer(), nullable=False),
        sa.Column("coverage_ratio", sa.Numeric(precision=10, scale=6), nullable=False),
        sa.Column("first_quote_at", sa.DateTime(), nullable=False),
        sa.Column("last_quote_at", sa.DateTime(), nullable=False),
        sa.Column("quality_flags", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_minute_bar")),
        sa.UniqueConstraint(
            "provider",
            "symbol",
            "minute_start",
            name="uq_minute_bar_provider_symbol_start",
        ),
    )
    op.create_index(
        "ix_minute_bar_symbol_trade_time",
        "minute_bar",
        ["symbol", "trade_date", "minute_start"],
        unique=False,
    )
    op.create_index(
        "ix_minute_bar_trade_time",
        "minute_bar",
        ["trade_date", "minute_start"],
        unique=False,
    )

    op.create_table(
        "minute_feature",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("minute_bar_id", sa.String(length=36), nullable=False),
        sa.Column("price_trend_bps", sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column("vwap_deviation_bps", sa.Numeric(precision=20, scale=6), nullable=True),
        sa.Column("relative_volume_ratio", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("relative_volume_history_days", sa.Integer(), nullable=False),
        sa.Column("market_benchmark_symbol", sa.String(length=9), nullable=True),
        sa.Column("market_relative_strength_bps", sa.Numeric(precision=20, scale=6), nullable=True),
        sa.Column("industry_benchmark_symbol", sa.String(length=9), nullable=True),
        sa.Column(
            "industry_relative_strength_bps", sa.Numeric(precision=20, scale=6), nullable=True
        ),
        sa.Column("quality_flags", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["minute_bar_id"],
            ["minute_bar.id"],
            name=op.f("fk_minute_feature_minute_bar_id_minute_bar"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_minute_feature")),
        sa.UniqueConstraint("minute_bar_id", name="uq_minute_feature_minute_bar_id"),
    )
    op.create_index(
        "ix_minute_feature_market_benchmark",
        "minute_feature",
        ["market_benchmark_symbol"],
        unique=False,
    )
    op.create_index(
        "ix_minute_feature_industry_benchmark",
        "minute_feature",
        ["industry_benchmark_symbol"],
        unique=False,
    )


def downgrade() -> None:
    """Remove minute features and bars."""
    op.drop_index("ix_minute_feature_industry_benchmark", table_name="minute_feature")
    op.drop_index("ix_minute_feature_market_benchmark", table_name="minute_feature")
    op.drop_table("minute_feature")
    op.drop_index("ix_minute_bar_trade_time", table_name="minute_bar")
    op.drop_index("ix_minute_bar_symbol_trade_time", table_name="minute_bar")
    op.drop_table("minute_bar")
