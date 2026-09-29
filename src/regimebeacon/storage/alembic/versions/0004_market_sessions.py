"""Cache Tushare trade_cal and persist collection market phases.

Revision ID: 0004_market_sessions
Revises: 0003_tushare_symbols
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_market_sessions"
down_revision: str | None = "0003_tushare_symbols"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the local Tushare calendar and collection phase metadata."""
    op.add_column(
        "market_collection_run",
        sa.Column("market_phase", sa.String(length=32), nullable=True),
    )
    op.create_table(
        "trading_calendar_day",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("exchange", sa.String(length=10), nullable=False),
        sa.Column("cal_date", sa.Date(), nullable=False),
        sa.Column("is_open", sa.Boolean(), nullable=False),
        sa.Column("pretrade_date", sa.Date(), nullable=True),
        sa.Column("source", sa.String(length=20), nullable=False),
        sa.Column("source_fetched_at", sa.DateTime(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_trading_calendar_day")),
        sa.UniqueConstraint(
            "exchange",
            "cal_date",
            name="uq_trading_calendar_day_exchange_date",
        ),
    )
    op.create_index(
        "ix_trading_calendar_day_date_open",
        "trading_calendar_day",
        ["cal_date", "is_open"],
        unique=False,
    )


def downgrade() -> None:
    """Remove collection phase metadata and cached calendar data."""
    op.drop_index("ix_trading_calendar_day_date_open", table_name="trading_calendar_day")
    op.drop_table("trading_calendar_day")
    op.drop_column("market_collection_run", "market_phase")
