"""Runtime heartbeat, gap, disk, and alert deduplication tests."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from dawnwatcher.config import Settings
from dawnwatcher.market import ChinaAStockCalendar, MarketPhase
from dawnwatcher.ops.monitoring import (
    DiskUsage,
    run_operational_checks,
    start_runtime_heartbeat,
    stop_runtime_heartbeat,
    touch_runtime_heartbeat,
)
from dawnwatcher.storage.models import (
    MarketCollectionRun,
    NotificationOutbox,
    OperationalAlert,
)


def test_runtime_heartbeat_lifecycle(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    now = datetime(2026, 9, 28, 1, 0, tzinfo=UTC)
    with session_factory_fixture.begin() as session:
        start_runtime_heartbeat(
            session,
            service_name="quote_watcher",
            instance_id="instance-1",
            interval_seconds=15,
            now=now,
            details={"attempted_runs": 0},
        )
        touch_runtime_heartbeat(
            session,
            instance_id="instance-1",
            now=now + timedelta(seconds=15),
            details={"attempted_runs": 1},
        )
        stopped = stop_runtime_heartbeat(
            session,
            instance_id="instance-1",
            now=now + timedelta(seconds=30),
            details={"attempted_runs": 2},
        )

    assert stopped.status == "stopped"
    assert stopped.stopped_at == now + timedelta(seconds=30)
    assert stopped.details == {"attempted_runs": 2}


def test_heartbeat_alert_is_deduplicated_and_resolved(
    database_settings: Settings,
    session_factory_fixture: sessionmaker[Session],
) -> None:
    now = datetime(2026, 9, 28, 2, 0, tzinfo=UTC)
    market_status = ChinaAStockCalendar({date(2026, 9, 28): True}).status_at(now)
    disk = lambda path: DiskUsage(10_000, 1_000, 9_000)  # noqa: E731
    settings = database_settings.model_copy(
        update={"disk_critical_free_bytes": 100, "disk_warning_free_bytes": 200}
    )

    with session_factory_fixture.begin() as session:
        session.add(
            MarketCollectionRun(
                id="heartbeat-test-collection",
                idempotency_key="heartbeat-test-collection",
                expected_trade_date=market_status.trade_date,
                market_phase=market_status.phase,
                requested_symbols=["600000.SH"],
                started_at=now - timedelta(seconds=2),
                finished_at=now - timedelta(seconds=1),
                provider_summaries={},
                quality_counts={"complete": 1},
            )
        )
        first = run_operational_checks(
            session,
            settings=settings,
            market_status=market_status,
            observed_at=now,
            disk_usage=disk,
        )
    with session_factory_fixture.begin() as session:
        second = run_operational_checks(
            session,
            settings=settings,
            market_status=market_status,
            observed_at=now + timedelta(seconds=10),
            disk_usage=disk,
        )
        start_runtime_heartbeat(
            session,
            service_name="quote_watcher",
            instance_id="healthy-instance",
            interval_seconds=15,
            now=now + timedelta(seconds=19),
        )
    with session_factory_fixture.begin() as session:
        recovered = run_operational_checks(
            session,
            settings=settings,
            market_status=market_status,
            observed_at=now + timedelta(seconds=20),
            disk_usage=disk,
        )
        notification_count = session.scalar(select(func.count()).select_from(NotificationOutbox))
        alert = session.scalar(
            select(OperationalAlert).where(
                OperationalAlert.alert_key == "runtime.quote_watcher.heartbeat"
            )
        )

    assert first["alert_transitions"] == [
        {"alert_key": "runtime.quote_watcher.heartbeat", "transition": "triggered"}
    ]
    assert second["alert_transitions"] == []
    assert recovered["alert_transitions"] == [
        {"alert_key": "runtime.quote_watcher.heartbeat", "transition": "resolved"}
    ]
    assert notification_count == 2
    assert alert is not None and alert.status == "resolved"


def test_heartbeat_is_not_required_outside_trading_day_runtime(
    database_settings: Settings,
    session_factory_fixture: sessionmaker[Session],
) -> None:
    observed = datetime(2026, 9, 27, 8, 0, tzinfo=UTC)
    market_status = ChinaAStockCalendar({date(2026, 9, 27): False}).status_at(observed)
    settings = database_settings.model_copy(
        update={"disk_critical_free_bytes": 100, "disk_warning_free_bytes": 200}
    )

    with session_factory_fixture.begin() as session:
        report = run_operational_checks(
            session,
            settings=settings,
            market_status=market_status,
            observed_at=observed,
            disk_usage=lambda path: DiskUsage(10_000, 1_000, 9_000),
        )

    assert report["checks"]["heartbeat"]["ok"] is True
    assert report["checks"]["heartbeat"]["applicable"] is False
    assert not any(
        item["alert_key"] == "runtime.quote_watcher.heartbeat" for item in report["active_issues"]
    )


def test_active_session_collection_gap_triggers_and_recovers(
    database_settings: Settings,
    session_factory_fixture: sessionmaker[Session],
) -> None:
    observed = datetime(2026, 9, 28, 2, 0, tzinfo=UTC)
    market_status = ChinaAStockCalendar({date(2026, 9, 28): True}).status_at(observed)
    assert market_status.phase is MarketPhase.MORNING_CONTINUOUS
    settings = database_settings.model_copy(
        update={
            "heartbeat_stale_seconds": 600,
            "collection_gap_seconds": 60,
            "disk_critical_free_bytes": 100,
            "disk_warning_free_bytes": 200,
        }
    )
    disk = lambda path: DiskUsage(10_000, 1_000, 9_000)  # noqa: E731
    with session_factory_fixture.begin() as session:
        start_runtime_heartbeat(
            session,
            service_name="quote_watcher",
            instance_id="gap-instance",
            interval_seconds=15,
            now=observed - timedelta(minutes=5),
        )
        first = run_operational_checks(
            session,
            settings=settings,
            market_status=market_status,
            observed_at=observed,
            disk_usage=disk,
        )
        session.add(
            MarketCollectionRun(
                id="recent-collection",
                idempotency_key="recent-collection",
                expected_trade_date=market_status.trade_date,
                market_phase=market_status.phase,
                requested_symbols=["600000.SH"],
                started_at=observed - timedelta(seconds=31),
                finished_at=observed - timedelta(seconds=30),
                provider_summaries={},
                quality_counts={"complete": 1},
            )
        )
    with session_factory_fixture.begin() as session:
        second = run_operational_checks(
            session,
            settings=settings,
            market_status=market_status,
            observed_at=observed + timedelta(seconds=1),
            disk_usage=disk,
        )

    assert any(item["alert_key"] == "market.collection.gap" for item in first["active_issues"])
    assert second["checks"]["collection_gap"]["ok"] is True
    assert {item["transition"] for item in second["alert_transitions"]} == {"resolved"}


def test_disk_alert_escalates_and_resolves(
    database_settings: Settings,
    session_factory_fixture: sessionmaker[Session],
) -> None:
    now = datetime(2026, 9, 27, 2, 0, tzinfo=UTC)
    market_status = ChinaAStockCalendar({date(2026, 9, 27): False}).status_at(now)
    settings = database_settings.model_copy(
        update={"disk_critical_free_bytes": 100, "disk_warning_free_bytes": 200}
    )
    with session_factory_fixture.begin() as session:
        start_runtime_heartbeat(
            session,
            service_name="quote_watcher",
            instance_id="disk-instance",
            interval_seconds=15,
            now=now,
        )
        warning = run_operational_checks(
            session,
            settings=settings,
            market_status=market_status,
            observed_at=now,
            disk_usage=lambda path: DiskUsage(1_000, 850, 150),
        )
    with session_factory_fixture.begin() as session:
        critical = run_operational_checks(
            session,
            settings=settings,
            market_status=market_status,
            observed_at=now + timedelta(seconds=1),
            disk_usage=lambda path: DiskUsage(1_000, 950, 50),
        )
    with session_factory_fixture.begin() as session:
        recovered = run_operational_checks(
            session,
            settings=settings,
            market_status=market_status,
            observed_at=now + timedelta(seconds=2),
            disk_usage=lambda path: DiskUsage(1_000, 500, 500),
        )

    assert {item["transition"] for item in warning["alert_transitions"]} == {"triggered"}
    assert {item["transition"] for item in critical["alert_transitions"]} == {"escalated"}
    assert {item["transition"] for item in recovered["alert_transitions"]} == {"resolved"}
