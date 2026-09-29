"""Schema migration and SQLite durability tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from sqlalchemy import Engine, MetaData, select

from regimebeacon.config import Settings
from regimebeacon.storage.database import create_database_engine
from regimebeacon.storage.schema import inspect_schema, upgrade_database


def test_migration_applies_expected_schema(
    database_settings: Settings,
    database_engine: Engine,
) -> None:
    status = inspect_schema(database_settings, database_engine)

    assert status.ok is True
    assert status.revision == "0006_minute_features"
    assert status.integrity == "ok"
    assert status.journal_mode == "wal"
    assert status.foreign_keys is True
    assert status.synchronous == 2
    assert Path(status.database_path).is_file()


def test_tushare_migration_rewrites_existing_symbols(tmp_path: Path) -> None:
    settings = Settings(data_dir=tmp_path, _env_file=None)
    upgrade_database(settings, "0002_market_quotes")
    engine = create_database_engine(settings)
    metadata = MetaData()
    metadata.reflect(engine)
    collection = metadata.tables["market_collection_run"]
    provider = metadata.tables["provider_quote_snapshot"]
    reconciled = metadata.tables["reconciled_quote_snapshot"]
    now = datetime(2026, 9, 24, 2, 0, tzinfo=UTC)
    prices = {
        "open": Decimal("9.00"),
        "previous_close": Decimal("8.98"),
        "latest": Decimal("9.01"),
        "high": Decimal("9.05"),
        "low": Decimal("8.97"),
    }
    with engine.begin() as connection:
        connection.execute(
            collection.insert(),
            {
                "id": "collection-1",
                "idempotency_key": "legacy-symbols",
                "expected_trade_date": now.date(),
                "requested_symbols": ["600000", "000001"],
                "started_at": now,
                "finished_at": now,
                "provider_summaries": {},
                "quality_counts": {"complete": 1},
                "created_at": now,
            },
        )
        connection.execute(
            provider.insert(),
            {
                "id": "provider-1",
                "collection_id": "collection-1",
                "provider": "tencent",
                "symbol": "600000",
                "exchange": "sse",
                "name": "浦发银行",
                "quote_at": now,
                "fetched_at": now,
                **prices,
                "volume_shares": 100,
                "amount_cny": Decimal("901.00"),
                "bid1_price": Decimal("9.00"),
                "bid1_volume_shares": 100,
                "ask1_price": Decimal("9.01"),
                "ask1_volume_shares": 100,
                "volume_precision_shares": 1,
                "raw_field_count": 34,
                "validation_issues": [],
                "created_at": now,
            },
        )
        connection.execute(
            reconciled.insert(),
            {
                "id": "reconciled-1",
                "collection_id": "collection-1",
                "symbol": "600000",
                "exchange": "sse",
                "quality_state": "complete",
                "selected_provider": "tencent",
                "comparisons": [],
                "reasons": [],
                "created_at": now,
            },
        )
    engine.dispose()

    upgrade_database(settings)

    upgraded_engine = create_database_engine(settings)
    upgraded_metadata = MetaData()
    upgraded_metadata.reflect(upgraded_engine)
    with upgraded_engine.connect() as connection:
        migrated_requested = connection.scalar(
            select(upgraded_metadata.tables["market_collection_run"].c.requested_symbols)
        )
        migrated_provider = connection.scalar(
            select(upgraded_metadata.tables["provider_quote_snapshot"].c.symbol)
        )
        migrated_reconciled = connection.scalar(
            select(upgraded_metadata.tables["reconciled_quote_snapshot"].c.symbol)
        )
    upgraded_engine.dispose()

    assert migrated_requested == ["600000.SH", "000001.SZ"]
    assert migrated_provider == "600000.SH"
    assert migrated_reconciled == "600000.SH"
