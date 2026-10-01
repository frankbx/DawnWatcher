"""Track active Tushare daily Parquet partitions in SQLite.

Revision ID: 0008_daily_lake_control
Revises: 0007_minute_query_indexes
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0008_daily_lake_control"
down_revision: str | None = "0007_minute_query_indexes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "daily_lake_partition",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("dataset", sa.String(20), nullable=False),
        sa.Column("source", sa.String(20), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("parquet_path", sa.Text(), nullable=True),
        sa.Column("sha256", sa.String(64), nullable=True),
        sa.Column("row_count", sa.Integer(), nullable=False),
        sa.Column("expected_count", sa.Integer(), nullable=False),
        sa.Column("member_sha256", sa.String(64), nullable=False),
        sa.Column("missing_symbols", sa.JSON(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("dataset", "trade_date", name="uq_daily_lake_partition_dataset_date"),
    )
    op.create_index(
        "ix_daily_lake_partition_status_date",
        "daily_lake_partition",
        ["status", "trade_date"],
    )


def downgrade() -> None:
    op.drop_index("ix_daily_lake_partition_status_date", table_name="daily_lake_partition")
    op.drop_table("daily_lake_partition")
