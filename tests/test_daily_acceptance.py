"""End-to-end daily verdict and missing-session detection."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from regimebeacon.config import Settings
from regimebeacon.market import ChinaAStockCalendar, MarketPhase
from regimebeacon.notifications.feishu import build_alert_card
from regimebeacon.ops.daily_acceptance import _assess_seals, run_daily_acceptance
from regimebeacon.storage.minute_parquet import (
    MinuteTradingSession,
    merge_minute_day,
    seal_minute_session,
)
from regimebeacon.storage.models import JobRun, MarketCollectionRun, NotificationOutbox
from tests.test_minute_parquet import _METADATA, _seed_afternoon, _seed_morning

_ZONE = ZoneInfo("Asia/Shanghai")
_DATE = date(2026, 9, 28)


def test_daily_acceptance_passes_complete_day_and_is_idempotent(
    database_settings: Settings,
    session_factory_fixture: sessionmaker[Session],
    tmp_path: Path,
) -> None:
    pool = _pool(tmp_path)
    output_root = database_settings.data_dir / "lake" / "minute_market"
    with session_factory_fixture.begin() as session:
        _seed_morning(session)
        _seed_afternoon(session)
        session.add_all(_full_day_collections())
    with session_factory_fixture() as session:
        for trading_session in MinuteTradingSession:
            seal_minute_session(
                session,
                trade_date=_DATE,
                trading_session=trading_session,
                timezone="Asia/Shanghai",
                output_root=output_root,
                instrument_metadata=_METADATA,
                observed_at=datetime(2026, 9, 28, 15, 2, tzinfo=_ZONE),
            )
    merge_minute_day(
        trade_date=_DATE,
        timezone="Asia/Shanghai",
        output_root=output_root,
        observed_at=datetime(2026, 9, 28, 15, 2, tzinfo=_ZONE),
    )

    first = run_daily_acceptance(
        database_settings,
        session_factory_fixture,
        trade_date=_DATE,
        pool_file=pool,
        observed_at=datetime(2026, 9, 28, 15, 10, tzinfo=_ZONE),
    )
    second = run_daily_acceptance(
        database_settings,
        session_factory_fixture,
        trade_date=_DATE,
        pool_file=pool,
        observed_at=datetime(2026, 9, 28, 15, 11, tzinfo=_ZONE),
    )

    assert first == second
    assert first["verdict"] == "passed"
    assert first["collection"]["expected_slot_count"] == 948
    assert first["collection"]["missing_slot_count"] == 0
    assert first["collection"]["excluded_non_continuous_collection_count"] == 52
    assert first["collection"]["legacy_dual_collection_count"] == 0
    assert first["seals"]["day"]["row_count"] == 480
    assert first["seals"]["day"]["valid"] is True
    with session_factory_fixture() as session:
        assert session.scalar(select(func.count()).select_from(JobRun)) == 1
        assert session.scalar(select(func.count()).select_from(NotificationOutbox)) == 1
        notification = session.scalar(select(NotificationOutbox))
    assert notification is not None
    card = build_alert_card(notification)
    assert card["schema"] == "2.0"
    assert card["header"]["template"] == "green"
    assert "480/480" in card["body"]["elements"][0]["content"]

    day_dir = output_root / f"trade_date={_DATE.isoformat()}"
    wrong_pool = _assess_seals(
        day_dir,
        trade_date=_DATE,
        expected_symbols={"600000.SH", "000001.SZ"},
    )
    assert all(
        "symbol_set_mismatch" in wrong_pool[name]["problems"]
        for name in ("morning", "afternoon", "day")
    )
    with (day_dir / "day.parquet").open("ab") as handle:
        handle.write(b"corruption")
    damaged = _assess_seals(day_dir, trade_date=_DATE, expected_symbols=set(_METADATA))
    assert damaged["day"]["valid"] is False
    assert "整日分钟文件缺失、损坏或不完整" in damaged["issues"]


def test_daily_acceptance_fails_when_entire_morning_is_missing(
    database_settings: Settings,
    session_factory_fixture: sessionmaker[Session],
    tmp_path: Path,
) -> None:
    pool = _pool(tmp_path)
    start = datetime(2026, 9, 28, 13, 0, tzinfo=_ZONE)
    with session_factory_fixture.begin() as session:
        session.add(_collection(1, start))

    report = run_daily_acceptance(
        database_settings,
        session_factory_fixture,
        trade_date=_DATE,
        pool_file=pool,
        observed_at=datetime(2026, 9, 28, 15, 10, tzinfo=_ZONE),
    )

    assert report["verdict"] == "failed"
    assert report["collection"]["max_missing_gap_seconds"] >= 7_200
    assert report["collection"]["phases"][0]["missing_slot_count"] == 480
    assert report["seals"]["day"]["valid"] is False


def _pool(tmp_path: Path) -> Path:
    path = tmp_path / "pool.json"
    path.write_text(
        json.dumps(
            {
                "members": [
                    {
                        "ts_code": symbol,
                        "name": metadata.name,
                        "instrument_type": metadata.instrument_type,
                        "role": metadata.role,
                    }
                    for symbol, metadata in _METADATA.items()
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


def _full_day_collections() -> list[MarketCollectionRun]:
    spans = (
        (time(9, 15), time(9, 25)),
        (time(9, 30), time(11, 30)),
        (time(13, 0), time(14, 57)),
        (time(14, 57), time(15, 0)),
    )
    calendar = ChinaAStockCalendar({_DATE: True})
    rows: list[MarketCollectionRun] = []
    for start_clock, end_clock in spans:
        current = datetime.combine(_DATE, start_clock, tzinfo=_ZONE)
        end = datetime.combine(_DATE, end_clock, tzinfo=_ZONE)
        while current < end:
            rows.append(_collection(len(rows), current, phase=calendar.status_at(current).phase))
            current += timedelta(seconds=15)
    return rows


def _collection(
    number: int, local_started: datetime, *, phase: MarketPhase | None = None
) -> MarketCollectionRun:
    started = local_started.astimezone(UTC)
    return MarketCollectionRun(
        id=f"acceptance-{number}",
        idempotency_key=f"acceptance-{number}",
        expected_trade_date=_DATE,
        market_phase=phase,
        requested_symbols=list(_METADATA),
        started_at=started,
        finished_at=started + timedelta(milliseconds=800),
        provider_summaries={
            "tencent": {"valid_quote_count": 2, "elapsed_ms": 800, "batch_issues": []}
        },
        quality_counts={"complete": 2},
    )
