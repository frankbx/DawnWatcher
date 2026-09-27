"""Normalize persisted security identifiers to Tushare ts_code values.

Revision ID: 0003_tushare_symbols
Revises: 0002_market_quotes
"""

from collections.abc import Callable, Sequence
from typing import Any

import sqlalchemy as sa
from alembic import op

revision: str = "0003_tushare_symbols"
down_revision: str | None = "0002_market_quotes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Expand symbol columns and rewrite existing values to Tushare format."""
    with op.batch_alter_table("provider_quote_snapshot") as batch_op:
        batch_op.alter_column(
            "symbol",
            existing_type=sa.String(length=6),
            type_=sa.String(length=9),
            existing_nullable=False,
        )
    with op.batch_alter_table("reconciled_quote_snapshot") as batch_op:
        batch_op.alter_column(
            "symbol",
            existing_type=sa.String(length=6),
            type_=sa.String(length=9),
            existing_nullable=False,
        )

    connection = op.get_bind()
    connection.execute(
        sa.text(
            "UPDATE provider_quote_snapshot "
            "SET symbol = symbol || CASE exchange "
            "WHEN 'sse' THEN '.SH' WHEN 'szse' THEN '.SZ' WHEN 'bse' THEN '.BJ' END "
            "WHERE instr(symbol, '.') = 0"
        )
    )
    connection.execute(
        sa.text(
            "UPDATE reconciled_quote_snapshot "
            "SET symbol = symbol || CASE exchange "
            "WHEN 'sse' THEN '.SH' WHEN 'szse' THEN '.SZ' WHEN 'bse' THEN '.BJ' END "
            "WHERE instr(symbol, '.') = 0"
        )
    )
    _rewrite_requested_symbols(_to_ts_code)


def downgrade() -> None:
    """Restore legacy six-digit persisted identifiers."""
    connection = op.get_bind()
    connection.execute(
        sa.text(
            "UPDATE provider_quote_snapshot SET symbol = substr(symbol, 1, 6) "
            "WHERE instr(symbol, '.') = 7"
        )
    )
    connection.execute(
        sa.text(
            "UPDATE reconciled_quote_snapshot SET symbol = substr(symbol, 1, 6) "
            "WHERE instr(symbol, '.') = 7"
        )
    )
    _rewrite_requested_symbols(lambda value: _to_ts_code(value)[:6])

    with op.batch_alter_table("reconciled_quote_snapshot") as batch_op:
        batch_op.alter_column(
            "symbol",
            existing_type=sa.String(length=9),
            type_=sa.String(length=6),
            existing_nullable=False,
        )
    with op.batch_alter_table("provider_quote_snapshot") as batch_op:
        batch_op.alter_column(
            "symbol",
            existing_type=sa.String(length=9),
            type_=sa.String(length=6),
            existing_nullable=False,
        )


def _rewrite_requested_symbols(converter: Callable[[str], str]) -> None:
    collection = sa.table(
        "market_collection_run",
        sa.column("id", sa.String(length=36)),
        sa.column("requested_symbols", sa.JSON()),
    )
    connection = op.get_bind()
    rows = (
        connection.execute(sa.select(collection.c.id, collection.c.requested_symbols))
        .mappings()
        .all()
    )
    for row in rows:
        symbols: Any = row["requested_symbols"]
        if not isinstance(symbols, list):
            raise ValueError("market_collection_run.requested_symbols must be a JSON array")
        normalized = [converter(str(value)) for value in symbols]
        connection.execute(
            collection.update()
            .where(collection.c.id == row["id"])
            .values(requested_symbols=normalized)
        )


def _to_ts_code(value: str) -> str:
    normalized = value.strip().upper()
    if len(normalized) == 9 and normalized[6] == ".":
        return normalized
    if len(normalized) == 8 and normalized[:2] in {"SH", "SZ", "BJ"}:
        suffix = normalized[:2]
        normalized = normalized[2:]
        return f"{normalized}.{suffix}"
    if len(normalized) != 6 or not normalized.isdigit():
        raise ValueError(f"cannot migrate invalid security code: {value}")
    if normalized.startswith(("600", "601", "603", "605", "688", "689")):
        suffix = "SH"
    elif normalized.startswith(("000", "001", "002", "003", "300", "301")):
        suffix = "SZ"
    elif normalized.startswith(("4", "8", "920")):
        suffix = "BJ"
    else:
        raise ValueError(f"cannot infer exchange for security code: {value}")
    return f"{normalized}.{suffix}"
