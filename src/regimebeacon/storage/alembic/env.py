"""Alembic runtime configuration."""

from __future__ import annotations

from alembic import context
from sqlalchemy import engine_from_config, pool

from regimebeacon.storage.models import Base

config = context.config
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations without creating an Engine."""
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=True,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations through a short-lived SQLAlchemy Engine."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section) or {},
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.begin() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys = ON")
        connection.exec_driver_sql("PRAGMA journal_mode = WAL")
        connection.exec_driver_sql("PRAGMA synchronous = FULL")
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=True,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()
    connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
