"""Atomic Parquet sealing for finalized intraday minute data."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from dawnwatcher.domain import QuoteProvider
from dawnwatcher.storage.models import MinuteBar, MinuteFeature

PARQUET_SCHEMA_VERSION = 1


class MinuteTradingSession(StrEnum):
    """Immutable minute-data partitions produced after each trading session."""

    MORNING = "morning"
    AFTERNOON = "afternoon"


@dataclass(frozen=True, slots=True)
class InstrumentMetadata:
    """Stable pool attributes embedded in analytical Parquet rows."""

    name: str
    instrument_type: str
    role: str
    industry_l1: str | None
    market: str | None
    selection_bucket: str | None
    proxy_quality: str | None


@dataclass(frozen=True, slots=True)
class MinuteParquetSealReport:
    """Validated output paths and completeness of one sealed session."""

    trade_date: date
    trading_session: str
    schema_version: int
    complete: bool
    row_count: int
    symbol_count: int
    expected_symbol_count: int
    minute_count: int
    expected_minute_count: int
    missing_feature_count: int
    missing_minutes: tuple[str, ...]
    incomplete_minutes: tuple[dict[str, int | str], ...]
    first_minute: datetime
    last_minute: datetime
    parquet_path: str
    manifest_path: str
    parquet_size_bytes: int
    parquet_sha256: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["trade_date"] = self.trade_date.isoformat()
        payload["first_minute"] = self.first_minute.isoformat()
        payload["last_minute"] = self.last_minute.isoformat()
        return payload


@dataclass(frozen=True, slots=True)
class MinuteParquetDayReport:
    """Validated whole-day file assembled from both sealed sessions."""

    trade_date: date
    schema_version: int
    complete: bool
    row_count: int
    symbol_count: int
    expected_symbol_count: int
    minute_count: int
    expected_minute_count: int
    missing_feature_count: int
    missing_minutes: tuple[str, ...]
    incomplete_minutes: tuple[dict[str, int | str], ...]
    first_minute: datetime
    last_minute: datetime
    parquet_path: str
    manifest_path: str
    parquet_size_bytes: int
    parquet_sha256: str
    source_sessions: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["trade_date"] = self.trade_date.isoformat()
        payload["first_minute"] = self.first_minute.isoformat()
        payload["last_minute"] = self.last_minute.isoformat()
        return payload


def load_instrument_metadata(path: Path) -> dict[str, InstrumentMetadata]:
    """Load the generated stock-pool metadata used to label Parquet rows."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw_members = payload.get("members")
    if not isinstance(raw_members, list):
        raise ValueError("pool document must contain a members list")
    result: dict[str, InstrumentMetadata] = {}
    for raw in raw_members:
        if not isinstance(raw, dict):
            raise ValueError("pool members must be JSON objects")
        symbol = raw.get("ts_code")
        name = raw.get("name")
        instrument_type = raw.get("instrument_type")
        role = raw.get("role")
        if (
            not isinstance(symbol, str)
            or not symbol
            or not isinstance(name, str)
            or not name
            or not isinstance(instrument_type, str)
            or not instrument_type
            or not isinstance(role, str)
            or not role
        ):
            raise ValueError("pool members require ts_code, name, instrument_type, and role")
        if symbol in result:
            raise ValueError(f"duplicate pool symbol: {symbol}")
        result[symbol] = InstrumentMetadata(
            name=name,
            instrument_type=instrument_type,
            role=role,
            industry_l1=_optional_string(raw.get("industry_l1")),
            market=_optional_string(raw.get("market")),
            selection_bucket=_optional_string(raw.get("selection_bucket")),
            proxy_quality=_optional_string(raw.get("proxy_quality")),
        )
    if not result:
        raise ValueError("pool metadata cannot be empty")
    return result


def seal_minute_session(
    session: Session,
    *,
    trade_date: date,
    trading_session: MinuteTradingSession,
    timezone: str,
    output_root: Path,
    instrument_metadata: dict[str, InstrumentMetadata],
    observed_at: datetime,
    provider: QuoteProvider = QuoteProvider.TENCENT,
) -> MinuteParquetSealReport:
    """Seal one finished session to a validated, atomically replaced Parquet partition."""
    zone = ZoneInfo(timezone)
    local_observed = _require_aware(observed_at).astimezone(zone)
    local_start, local_end = _session_bounds(trade_date, trading_session, zone)
    if local_observed < local_end + timedelta(minutes=1):
        raise ValueError(
            f"{trading_session.value} session is not sealable until "
            f"{(local_end + timedelta(minutes=1)).isoformat()}"
        )
    expected_minutes = _expected_minutes(trade_date, trading_session, zone)
    expected_minute_set = set(expected_minutes)
    symbols = set(instrument_metadata)
    rows = list(
        session.execute(
            select(MinuteBar, MinuteFeature)
            .outerjoin(MinuteFeature, MinuteFeature.minute_bar_id == MinuteBar.id)
            .where(
                MinuteBar.trade_date == trade_date,
                MinuteBar.provider == provider,
                MinuteBar.symbol.in_(symbols),
                MinuteBar.minute_start >= local_start.astimezone(UTC),
                MinuteBar.minute_start < local_end.astimezone(UTC),
            )
            .order_by(MinuteBar.minute_start, MinuteBar.symbol)
        )
    )
    if not rows:
        raise ValueError(
            f"no minute bars found for {trade_date.isoformat()} {trading_session.value}"
        )

    records: list[dict[str, Any]] = []
    keys: set[tuple[str, datetime]] = set()
    minute_counts: Counter[datetime] = Counter()
    missing_features = 0
    for bar, feature in rows:
        optional_feature: MinuteFeature | None = feature
        local_minute = bar.minute_start.astimezone(zone)
        # Boundary snapshots such as 09:25 belong to the raw audit trail but
        # do not represent a complete [minute, minute + 1) trading interval.
        if local_minute not in expected_minute_set:
            continue
        key = (bar.symbol, bar.minute_start)
        if key in keys:
            raise ValueError(f"duplicate minute key encountered: {key}")
        keys.add(key)
        minute_counts[local_minute] += 1
        metadata = instrument_metadata[bar.symbol]
        if optional_feature is None:
            missing_features += 1
        records.append(
            _record(
                bar,
                optional_feature,
                metadata=metadata,
                local_minute=local_minute,
                trading_session=trading_session,
            )
        )

    if not records:
        raise ValueError(
            f"no sealable minute bars found for {trade_date.isoformat()} {trading_session.value}"
        )
    missing_minutes = tuple(
        minute.strftime("%H:%M") for minute in expected_minutes if minute not in minute_counts
    )
    expected_symbols = len(instrument_metadata)
    incomplete_values: list[dict[str, int | str]] = []
    for minute in sorted(minute_counts):
        if minute_counts[minute] != expected_symbols:
            incomplete_values.append(
                {
                    "minute": minute.strftime("%H:%M"),
                    "actual_symbols": minute_counts[minute],
                    "expected_symbols": expected_symbols,
                }
            )
    incomplete_minutes = tuple(incomplete_values)
    complete = not missing_minutes and not incomplete_minutes and missing_features == 0
    partition = (
        output_root / f"trade_date={trade_date.isoformat()}" / f"session={trading_session.value}"
    )
    partition.mkdir(parents=True, exist_ok=True)
    parquet_path = partition / "part-000.parquet"
    manifest_path = partition / "manifest.json"
    schema = _arrow_schema()
    table = _arrow_module().Table.from_pylist(records, schema=schema)
    parquet_sha256, parquet_size = _write_parquet_atomic(table, parquet_path)
    metadata = _read_parquet_metadata(parquet_path)
    if metadata.num_rows != len(records):
        raise RuntimeError(
            f"Parquet row-count verification failed: {metadata.num_rows} != {len(records)}"
        )

    first_minute = min(minute_counts)
    last_minute = max(minute_counts)
    report = MinuteParquetSealReport(
        trade_date=trade_date,
        trading_session=trading_session.value,
        schema_version=PARQUET_SCHEMA_VERSION,
        complete=complete,
        row_count=len(records),
        symbol_count=len({record["symbol"] for record in records}),
        expected_symbol_count=expected_symbols,
        minute_count=len(minute_counts),
        expected_minute_count=len(expected_minutes),
        missing_feature_count=missing_features,
        missing_minutes=missing_minutes,
        incomplete_minutes=incomplete_minutes,
        first_minute=first_minute,
        last_minute=last_minute,
        parquet_path=str(parquet_path.resolve()),
        manifest_path=str(manifest_path.resolve()),
        parquet_size_bytes=parquet_size,
        parquet_sha256=parquet_sha256,
    )
    manifest = {
        **report.to_dict(),
        "dataset": "minute_market",
        "provider": provider.value,
        "timezone": timezone,
        "compression": "zstd",
        "row_group_size": 65_536,
        "unique_key": ["provider", "symbol", "minute_start"],
        "generated_at": local_observed.isoformat(),
        "columns": schema.names,
    }
    _write_json_atomic(manifest_path, manifest)
    return report


def merge_minute_day(
    *,
    trade_date: date,
    timezone: str,
    output_root: Path,
    observed_at: datetime,
) -> MinuteParquetDayReport:
    """Atomically assemble morning and afternoon partitions into one daily Parquet file."""
    zone = ZoneInfo(timezone)
    local_observed = _require_aware(observed_at).astimezone(zone)
    market_close = datetime.combine(trade_date, time(15, 0), tzinfo=zone)
    if local_observed < market_close + timedelta(minutes=1):
        raise ValueError(
            f"day is not mergeable until {(market_close + timedelta(minutes=1)).isoformat()}"
        )

    day_partition = output_root / f"trade_date={trade_date.isoformat()}"
    tables: list[Any] = []
    source_sessions: list[dict[str, Any]] = []
    manifests: list[dict[str, Any]] = []
    for trading_session in MinuteTradingSession:
        session_partition = day_partition / f"session={trading_session.value}"
        parquet_path = session_partition / "part-000.parquet"
        manifest_path = session_partition / "manifest.json"
        table, manifest = _load_validated_session_partition(
            trade_date=trade_date,
            trading_session=trading_session,
            parquet_path=parquet_path,
            manifest_path=manifest_path,
        )
        tables.append(table)
        manifests.append(manifest)
        source_sessions.append(
            {
                "session": trading_session.value,
                "complete": manifest["complete"],
                "row_count": table.num_rows,
                "parquet_path": str(parquet_path.resolve()),
                "manifest_path": str(manifest_path.resolve()),
                "parquet_sha256": manifest["parquet_sha256"],
            }
        )

    expected_symbol_counts = {_manifest_int(value, "expected_symbol_count") for value in manifests}
    if len(expected_symbol_counts) != 1:
        raise ValueError("session manifests disagree on expected_symbol_count")
    expected_symbol_count = expected_symbol_counts.pop()
    pa = _arrow_module()
    table = pa.concat_tables(tables)
    _validate_unique_day_keys(table)

    minute_values = table.column("minute_start").to_pylist()
    symbol_values = table.column("symbol").to_pylist()
    first_minute = min(minute_values)
    last_minute = max(minute_values)
    minute_count = len(set(minute_values))
    symbol_count = len(set(symbol_values))
    expected_minute_count = sum(
        _manifest_int(value, "expected_minute_count") for value in manifests
    )
    missing_feature_count = sum(
        _manifest_int(value, "missing_feature_count") for value in manifests
    )
    missing_minutes = tuple(
        str(minute) for value in manifests for minute in _manifest_list(value, "missing_minutes")
    )
    incomplete_minutes = tuple(
        item for value in manifests for item in _manifest_dict_list(value, "incomplete_minutes")
    )
    expected_row_count = expected_symbol_count * expected_minute_count
    complete = (
        all(value["complete"] is True for value in manifests)
        and table.num_rows == expected_row_count
        and minute_count == expected_minute_count
        and symbol_count == expected_symbol_count
        and missing_feature_count == 0
        and not missing_minutes
        and not incomplete_minutes
    )

    parquet_path = day_partition / "day.parquet"
    manifest_path = day_partition / "day-manifest.json"
    parquet_sha256, parquet_size = _write_parquet_atomic(table, parquet_path)
    output_metadata = _read_parquet_metadata(parquet_path)
    if output_metadata.num_rows != table.num_rows:
        raise RuntimeError(
            f"daily Parquet row-count verification failed: "
            f"{output_metadata.num_rows} != {table.num_rows}"
        )
    report = MinuteParquetDayReport(
        trade_date=trade_date,
        schema_version=PARQUET_SCHEMA_VERSION,
        complete=complete,
        row_count=table.num_rows,
        symbol_count=symbol_count,
        expected_symbol_count=expected_symbol_count,
        minute_count=minute_count,
        expected_minute_count=expected_minute_count,
        missing_feature_count=missing_feature_count,
        missing_minutes=missing_minutes,
        incomplete_minutes=incomplete_minutes,
        first_minute=first_minute,
        last_minute=last_minute,
        parquet_path=str(parquet_path.resolve()),
        manifest_path=str(manifest_path.resolve()),
        parquet_size_bytes=parquet_size,
        parquet_sha256=parquet_sha256,
        source_sessions=tuple(source_sessions),
    )
    manifest = {
        **report.to_dict(),
        "dataset": "minute_market_day",
        "provider": QuoteProvider.TENCENT.value,
        "timezone": timezone,
        "compression": "zstd",
        "row_group_size": 65_536,
        "unique_key": ["provider", "symbol", "minute_start"],
        "generated_at": local_observed.isoformat(),
        "columns": table.schema.names,
    }
    _write_json_atomic(manifest_path, manifest)
    return report


def _load_validated_session_partition(
    *,
    trade_date: date,
    trading_session: MinuteTradingSession,
    parquet_path: Path,
    manifest_path: Path,
) -> tuple[Any, dict[str, Any]]:
    if not parquet_path.is_file() or not manifest_path.is_file():
        raise FileNotFoundError(
            f"sealed {trading_session.value} partition is missing for {trade_date.isoformat()}"
        )
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"invalid session manifest: {manifest_path}")
    if payload.get("trade_date") != trade_date.isoformat():
        raise ValueError(f"session manifest trade_date mismatch: {manifest_path}")
    if payload.get("trading_session") != trading_session.value:
        raise ValueError(f"session manifest name mismatch: {manifest_path}")
    if payload.get("schema_version") != PARQUET_SCHEMA_VERSION:
        raise ValueError(f"session manifest schema version mismatch: {manifest_path}")
    if not isinstance(payload.get("complete"), bool):
        raise ValueError(f"session manifest complete flag is invalid: {manifest_path}")
    expected_checksum = payload.get("parquet_sha256")
    if not isinstance(expected_checksum, str) or _sha256(parquet_path) != expected_checksum:
        raise ValueError(f"session Parquet checksum mismatch: {parquet_path}")

    # ParquetFile avoids Hive partition inference from the surrounding
    # trade_date=/session= directories because those columns already exist.
    table = _parquet_module().ParquetFile(parquet_path).read()
    if not table.schema.equals(_arrow_schema(), check_metadata=True):
        raise ValueError(f"session Parquet schema mismatch: {parquet_path}")
    if table.num_rows != _manifest_int(payload, "row_count"):
        raise ValueError(f"session Parquet row count mismatch: {parquet_path}")
    if set(table.column("trade_date").to_pylist()) != {trade_date}:
        raise ValueError(f"session Parquet contains another trade date: {parquet_path}")
    if set(table.column("session").to_pylist()) != {trading_session.value}:
        raise ValueError(f"session Parquet contains another session: {parquet_path}")
    return table, payload


def _validate_unique_day_keys(table: Any) -> None:
    providers = table.column("provider").to_pylist()
    symbols = table.column("symbol").to_pylist()
    minutes = table.column("minute_start").to_pylist()
    keys = set(zip(providers, symbols, minutes, strict=True))
    if len(keys) != table.num_rows:
        raise ValueError("daily Parquet sources contain duplicate provider/symbol/minute keys")


def _manifest_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"session manifest {key} is invalid")
    return value


def _manifest_list(payload: dict[str, Any], key: str) -> list[Any]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise ValueError(f"session manifest {key} is invalid")
    return value


def _manifest_dict_list(payload: dict[str, Any], key: str) -> list[dict[str, int | str]]:
    values = _manifest_list(payload, key)
    if not all(isinstance(value, dict) for value in values):
        raise ValueError(f"session manifest {key} is invalid")
    return values


def _record(
    bar: MinuteBar,
    feature: MinuteFeature | None,
    *,
    metadata: InstrumentMetadata,
    local_minute: datetime,
    trading_session: MinuteTradingSession,
) -> dict[str, Any]:
    return {
        "schema_version": PARQUET_SCHEMA_VERSION,
        "provider": bar.provider.value,
        "trade_date": bar.trade_date,
        "session": trading_session.value,
        "market_phase": _market_phase(local_minute),
        "symbol": bar.symbol,
        "exchange": bar.exchange.value,
        "name": metadata.name,
        "instrument_type": metadata.instrument_type,
        "role": metadata.role,
        "industry_l1": metadata.industry_l1,
        "market": metadata.market,
        "selection_bucket": metadata.selection_bucket,
        "proxy_quality": metadata.proxy_quality,
        "minute_start": bar.minute_start,
        "minute_end": bar.minute_end,
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "cumulative_volume_start": bar.cumulative_volume_start,
        "cumulative_volume_end": bar.cumulative_volume_end,
        "incremental_volume_shares": bar.volume_shares,
        "cumulative_amount_start": bar.cumulative_amount_start,
        "cumulative_amount_end": bar.cumulative_amount_end,
        "incremental_amount_cny": bar.amount_cny,
        "vwap": bar.vwap,
        "sample_count": bar.sample_count,
        "expected_sample_count": bar.expected_sample_count,
        "coverage_ratio": bar.coverage_ratio,
        "first_quote_at": bar.first_quote_at,
        "last_quote_at": bar.last_quote_at,
        "bar_quality_flags": bar.quality_flags,
        "price_trend_bps": feature.price_trend_bps if feature is not None else None,
        "vwap_deviation_bps": feature.vwap_deviation_bps if feature is not None else None,
        "relative_volume_ratio": feature.relative_volume_ratio if feature is not None else None,
        "relative_volume_history_days": (
            feature.relative_volume_history_days if feature is not None else None
        ),
        "market_benchmark_symbol": (
            feature.market_benchmark_symbol if feature is not None else None
        ),
        "market_relative_strength_bps": (
            feature.market_relative_strength_bps if feature is not None else None
        ),
        "industry_benchmark_symbol": (
            feature.industry_benchmark_symbol if feature is not None else None
        ),
        "industry_relative_strength_bps": (
            feature.industry_relative_strength_bps if feature is not None else None
        ),
        "feature_quality_flags": feature.quality_flags if feature is not None else ["missing"],
    }


def _arrow_schema() -> Any:
    pa = _arrow_module()
    fields = [
        pa.field("schema_version", pa.int16(), nullable=False),
        pa.field("provider", pa.string(), nullable=False),
        pa.field("trade_date", pa.date32(), nullable=False),
        pa.field("session", pa.string(), nullable=False),
        pa.field("market_phase", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("exchange", pa.string(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
        pa.field("instrument_type", pa.string(), nullable=False),
        pa.field("role", pa.string(), nullable=False),
        pa.field("industry_l1", pa.string()),
        pa.field("market", pa.string()),
        pa.field("selection_bucket", pa.string()),
        pa.field("proxy_quality", pa.string()),
        pa.field("minute_start", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("minute_end", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("open", pa.decimal128(20, 6), nullable=False),
        pa.field("high", pa.decimal128(20, 6), nullable=False),
        pa.field("low", pa.decimal128(20, 6), nullable=False),
        pa.field("close", pa.decimal128(20, 6), nullable=False),
        pa.field("cumulative_volume_start", pa.int64()),
        pa.field("cumulative_volume_end", pa.int64(), nullable=False),
        pa.field("incremental_volume_shares", pa.int64()),
        pa.field("cumulative_amount_start", pa.decimal128(24, 4)),
        pa.field("cumulative_amount_end", pa.decimal128(24, 4), nullable=False),
        pa.field("incremental_amount_cny", pa.decimal128(24, 4)),
        pa.field("vwap", pa.decimal128(20, 8)),
        pa.field("sample_count", pa.int32(), nullable=False),
        pa.field("expected_sample_count", pa.int32(), nullable=False),
        pa.field("coverage_ratio", pa.decimal128(10, 6), nullable=False),
        pa.field("first_quote_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("last_quote_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field(
            "bar_quality_flags",
            pa.list_(pa.field("element", pa.string())),
            nullable=False,
        ),
        pa.field("price_trend_bps", pa.decimal128(20, 6)),
        pa.field("vwap_deviation_bps", pa.decimal128(20, 6)),
        pa.field("relative_volume_ratio", pa.decimal128(20, 8)),
        pa.field("relative_volume_history_days", pa.int32()),
        pa.field("market_benchmark_symbol", pa.string()),
        pa.field("market_relative_strength_bps", pa.decimal128(20, 6)),
        pa.field("industry_benchmark_symbol", pa.string()),
        pa.field("industry_relative_strength_bps", pa.decimal128(20, 6)),
        pa.field(
            "feature_quality_flags",
            pa.list_(pa.field("element", pa.string())),
            nullable=False,
        ),
    ]
    metadata = {
        b"dawnwatcher.dataset": b"minute_market",
        b"dawnwatcher.schema_version": str(PARQUET_SCHEMA_VERSION).encode(),
    }
    return pa.schema(fields, metadata=metadata)


def _arrow_module() -> Any:
    try:
        return importlib.import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Parquet support is not installed; run pip install -e '.[parquet]'"
        ) from exc


def _parquet_module() -> Any:
    try:
        return importlib.import_module("pyarrow.parquet")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Parquet support is not installed; run pip install -e '.[parquet]'"
        ) from exc


def _write_parquet_atomic(table: Any, destination: Path) -> tuple[str, int]:
    temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.partial")
    try:
        _parquet_module().write_table(
            table,
            temporary,
            compression="zstd",
            use_dictionary=[
                "provider",
                "session",
                "market_phase",
                "symbol",
                "exchange",
                "instrument_type",
                "role",
                "industry_l1",
                "market",
                "selection_bucket",
                "proxy_quality",
                "market_benchmark_symbol",
                "industry_benchmark_symbol",
            ],
            write_statistics=True,
            row_group_size=65_536,
        )
        _fsync_file(temporary)
        metadata = _read_parquet_metadata(temporary)
        if metadata.num_rows != table.num_rows:
            raise RuntimeError(
                f"temporary Parquet verification failed: {metadata.num_rows} != {table.num_rows}"
            )
        checksum = _sha256(temporary)
        size = temporary.stat().st_size
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)
        return checksum, size
    finally:
        temporary.unlink(missing_ok=True)


def _write_json_atomic(destination: Path, payload: dict[str, Any]) -> None:
    temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.partial")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _read_parquet_metadata(path: Path) -> Any:
    return _parquet_module().read_metadata(path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _session_bounds(
    trade_date: date, trading_session: MinuteTradingSession, zone: ZoneInfo
) -> tuple[datetime, datetime]:
    if trading_session is MinuteTradingSession.MORNING:
        return (
            datetime.combine(trade_date, time(9, 15), tzinfo=zone),
            datetime.combine(trade_date, time(11, 30), tzinfo=zone),
        )
    return (
        datetime.combine(trade_date, time(13, 0), tzinfo=zone),
        datetime.combine(trade_date, time(15, 0), tzinfo=zone),
    )


def _expected_minutes(
    trade_date: date, trading_session: MinuteTradingSession, zone: ZoneInfo
) -> tuple[datetime, ...]:
    start, end = _session_bounds(trade_date, trading_session, zone)
    result: list[datetime] = []
    current = start
    while current < end:
        if (
            trading_session is MinuteTradingSession.AFTERNOON
            or current.time() < time(9, 25)
            or current.time() >= time(9, 30)
        ):
            result.append(current)
        current += timedelta(minutes=1)
    return tuple(result)


def _market_phase(local_minute: datetime) -> str:
    clock = local_minute.time()
    if time(9, 15) <= clock < time(9, 25):
        return "opening_call_auction"
    if time(9, 30) <= clock < time(11, 30):
        return "morning_continuous"
    if time(13, 0) <= clock < time(14, 57):
        return "afternoon_continuous"
    if time(14, 57) <= clock < time(15, 0):
        return "closing_call_auction"
    return "unknown"


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _require_aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    return value
