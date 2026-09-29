"""Atomic minute-session Parquet sealing tests."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy.orm import Session, sessionmaker

from regimebeacon.domain import Exchange, QuoteProvider
from regimebeacon.storage.minute_parquet import (
    InstrumentMetadata,
    MinuteTradingSession,
    merge_minute_day,
    seal_minute_session,
)
from regimebeacon.storage.models import MinuteBar, MinuteFeature

pq = pytest.importorskip("pyarrow.parquet")

_ZONE = ZoneInfo("Asia/Shanghai")
_DATE = date(2026, 9, 28)
_SYMBOLS = ("600000.SH", "510300.SH")
_METADATA = {
    "600000.SH": InstrumentMetadata(
        name="浦发银行",
        instrument_type="stock",
        role="fixed_representative",
        industry_l1="银行",
        market="主板",
        selection_bucket="large_cap",
        proxy_quality=None,
    ),
    "510300.SH": InstrumentMetadata(
        name="沪深300ETF",
        instrument_type="etf",
        role="broad_benchmark",
        industry_l1=None,
        market="场内基金",
        selection_bucket="沪深300",
        proxy_quality="direct",
    ),
}


def test_seals_complete_afternoon_partition_atomically(
    session_factory_fixture: sessionmaker[Session], tmp_path: Path
) -> None:
    with session_factory_fixture.begin() as session:
        _seed_afternoon(session)

    with session_factory_fixture() as session:
        report = seal_minute_session(
            session,
            trade_date=_DATE,
            trading_session=MinuteTradingSession.AFTERNOON,
            timezone="Asia/Shanghai",
            output_root=tmp_path / "minute_market",
            instrument_metadata=_METADATA,
            observed_at=datetime(2026, 9, 28, 15, 1, tzinfo=_ZONE),
        )

    assert report.complete is True
    assert report.row_count == 240
    assert report.symbol_count == 2
    assert report.minute_count == 120
    assert report.missing_minutes == ()
    parquet_path = Path(report.parquet_path)
    manifest_path = Path(report.manifest_path)
    assert parquet_path.exists()
    assert manifest_path.exists()
    assert not list(parquet_path.parent.glob("*.partial"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["complete"] is True
    assert manifest["unique_key"] == ["provider", "symbol", "minute_start"]
    assert manifest["parquet_sha256"] == _sha256(parquet_path)
    table = pq.ParquetFile(parquet_path).read()
    assert table.num_rows == 240
    assert table.schema.metadata[b"regimebeacon.schema_version"] == b"1"
    assert table.schema.metadata[b"dawnwatcher.schema_version"] == b"1"
    assert "industry_l1" in table.column_names


def test_seals_partial_data_but_marks_manifest_incomplete(
    session_factory_fixture: sessionmaker[Session], tmp_path: Path
) -> None:
    with session_factory_fixture.begin() as session:
        _seed_afternoon(session, minutes=1)

    with session_factory_fixture() as session:
        report = seal_minute_session(
            session,
            trade_date=_DATE,
            trading_session=MinuteTradingSession.AFTERNOON,
            timezone="Asia/Shanghai",
            output_root=tmp_path / "minute_market",
            instrument_metadata=_METADATA,
            observed_at=datetime(2026, 9, 28, 15, 1, tzinfo=_ZONE),
        )

    assert report.complete is False
    assert report.row_count == 2
    assert len(report.missing_minutes) == 119
    assert Path(report.parquet_path).exists()


def test_rejects_sealing_an_active_session(
    session_factory_fixture: sessionmaker[Session], tmp_path: Path
) -> None:
    with session_factory_fixture.begin() as session:
        _seed_afternoon(session, minutes=1)

    with session_factory_fixture() as session:
        with pytest.raises(ValueError, match="not sealable"):
            seal_minute_session(
                session,
                trade_date=_DATE,
                trading_session=MinuteTradingSession.AFTERNOON,
                timezone="Asia/Shanghai",
                output_root=tmp_path / "minute_market",
                instrument_metadata=_METADATA,
                observed_at=datetime(2026, 9, 28, 14, 30, tzinfo=_ZONE),
            )


def test_merges_sealed_sessions_into_one_validated_daily_file(
    session_factory_fixture: sessionmaker[Session], tmp_path: Path
) -> None:
    output_root = tmp_path / "minute_market"
    with session_factory_fixture.begin() as session:
        _seed_morning(session)
        _seed_afternoon(session)
    with session_factory_fixture() as session:
        morning = seal_minute_session(
            session,
            trade_date=_DATE,
            trading_session=MinuteTradingSession.MORNING,
            timezone="Asia/Shanghai",
            output_root=output_root,
            instrument_metadata=_METADATA,
            observed_at=datetime(2026, 9, 28, 15, 2, tzinfo=_ZONE),
        )
        afternoon = seal_minute_session(
            session,
            trade_date=_DATE,
            trading_session=MinuteTradingSession.AFTERNOON,
            timezone="Asia/Shanghai",
            output_root=output_root,
            instrument_metadata=_METADATA,
            observed_at=datetime(2026, 9, 28, 15, 2, tzinfo=_ZONE),
        )

    report = merge_minute_day(
        trade_date=_DATE,
        timezone="Asia/Shanghai",
        output_root=output_root,
        observed_at=datetime(2026, 9, 28, 15, 2, tzinfo=_ZONE),
    )

    assert morning.complete is True
    assert afternoon.complete is True
    assert report.complete is True
    assert report.row_count == 500
    assert report.minute_count == 250
    assert report.symbol_count == 2
    parquet_path = Path(report.parquet_path)
    manifest = json.loads(Path(report.manifest_path).read_text(encoding="utf-8"))
    assert parquet_path.name == "day.parquet"
    assert manifest["dataset"] == "minute_market_day"
    assert manifest["parquet_sha256"] == _sha256(parquet_path)
    assert [item["session"] for item in manifest["source_sessions"]] == [
        "morning",
        "afternoon",
    ]
    table = pq.read_table(parquet_path)
    assert table.num_rows == 500
    assert set(table.column("session").to_pylist()) == {"morning", "afternoon"}


def test_daily_merge_propagates_incomplete_session_status(
    session_factory_fixture: sessionmaker[Session], tmp_path: Path
) -> None:
    output_root = tmp_path / "minute_market"
    with session_factory_fixture.begin() as session:
        _seed_morning(session)
        _seed_afternoon(session, minutes=1)
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

    report = merge_minute_day(
        trade_date=_DATE,
        timezone="Asia/Shanghai",
        output_root=output_root,
        observed_at=datetime(2026, 9, 28, 15, 2, tzinfo=_ZONE),
    )

    assert report.complete is False
    assert report.row_count == 262
    assert report.minute_count == 131
    assert len(report.missing_minutes) == 119
    assert Path(report.parquet_path).is_file()


def test_daily_merge_rejects_corrupted_source_partition(
    session_factory_fixture: sessionmaker[Session], tmp_path: Path
) -> None:
    output_root = tmp_path / "minute_market"
    with session_factory_fixture.begin() as session:
        _seed_morning(session)
        _seed_afternoon(session)
    with session_factory_fixture() as session:
        reports = [
            seal_minute_session(
                session,
                trade_date=_DATE,
                trading_session=trading_session,
                timezone="Asia/Shanghai",
                output_root=output_root,
                instrument_metadata=_METADATA,
                observed_at=datetime(2026, 9, 28, 15, 2, tzinfo=_ZONE),
            )
            for trading_session in MinuteTradingSession
        ]
    with Path(reports[0].parquet_path).open("ab") as handle:
        handle.write(b"corruption")

    with pytest.raises(ValueError, match="checksum mismatch"):
        merge_minute_day(
            trade_date=_DATE,
            timezone="Asia/Shanghai",
            output_root=output_root,
            observed_at=datetime(2026, 9, 28, 15, 2, tzinfo=_ZONE),
        )


def test_daily_merge_requires_market_close_and_both_sessions(tmp_path: Path) -> None:
    output_root = tmp_path / "minute_market"
    with pytest.raises(ValueError, match="not mergeable until"):
        merge_minute_day(
            trade_date=_DATE,
            timezone="Asia/Shanghai",
            output_root=output_root,
            observed_at=datetime(2026, 9, 28, 14, 59, tzinfo=_ZONE),
        )
    with pytest.raises(FileNotFoundError, match="morning partition is missing"):
        merge_minute_day(
            trade_date=_DATE,
            timezone="Asia/Shanghai",
            output_root=output_root,
            observed_at=datetime(2026, 9, 28, 15, 2, tzinfo=_ZONE),
        )


def _seed_morning(session: Session) -> None:
    call_auction = [
        datetime(2026, 9, 28, 9, 15, tzinfo=_ZONE) + timedelta(minutes=index) for index in range(10)
    ]
    continuous = [
        datetime(2026, 9, 28, 9, 30, tzinfo=_ZONE) + timedelta(minutes=index)
        for index in range(120)
    ]
    _seed_minutes(session, call_auction + continuous)


def _seed_afternoon(session: Session, *, minutes: int = 120) -> None:
    local_start = datetime(2026, 9, 28, 13, 0, tzinfo=_ZONE)
    _seed_minutes(
        session,
        [local_start + timedelta(minutes=minute_index) for minute_index in range(minutes)],
    )


def _seed_minutes(session: Session, local_minutes: list[datetime]) -> None:
    bars: list[MinuteBar] = []
    for minute_index, local_minute in enumerate(local_minutes):
        start = local_minute.astimezone(UTC)
        for symbol in _SYMBOLS:
            price = Decimal("10") + Decimal(minute_index) / Decimal("100")
            bar = MinuteBar(
                provider=QuoteProvider.TENCENT,
                symbol=symbol,
                exchange=Exchange.SSE,
                trade_date=_DATE,
                minute_start=start,
                minute_end=start + timedelta(minutes=1),
                open=price,
                high=price,
                low=price,
                close=price,
                cumulative_volume_start=minute_index * 100,
                cumulative_volume_end=(minute_index + 1) * 100,
                volume_shares=100,
                cumulative_amount_start=Decimal(minute_index * 1000),
                cumulative_amount_end=Decimal((minute_index + 1) * 1000),
                amount_cny=Decimal("1000"),
                vwap=price,
                sample_count=4,
                expected_sample_count=4,
                coverage_ratio=Decimal("1"),
                first_quote_at=start,
                last_quote_at=start + timedelta(seconds=45),
                quality_flags=[],
            )
            bars.append(bar)
            session.add(bar)
    session.flush()
    for bar in bars:
        session.add(
            MinuteFeature(
                minute_bar_id=bar.id,
                price_trend_bps=Decimal("0"),
                vwap_deviation_bps=Decimal("0"),
                relative_volume_ratio=None,
                relative_volume_history_days=0,
                market_benchmark_symbol="510300.SH",
                market_relative_strength_bps=Decimal("0"),
                industry_benchmark_symbol=("512800.SH" if bar.symbol == "600000.SH" else None),
                industry_relative_strength_bps=(
                    Decimal("0") if bar.symbol == "600000.SH" else None
                ),
                quality_flags=["relative_volume_history_insufficient"],
            )
        )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
