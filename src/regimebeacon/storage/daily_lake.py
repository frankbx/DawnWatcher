"""Validated Tushare day partitions: immutable Parquet data, SQLite active pointers."""

from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from regimebeacon.domain.quotes import QuoteSymbol
from regimebeacon.storage.models import DailyLakePartition

PRICE_ENDPOINTS = {"stock": "daily", "etf": "fund_daily"}
FACTOR_ENDPOINTS = {"stock": "adj_factor", "etf": "fund_adj"}
SCHEMA_VERSION = 1


class DailySource(Protocol):
    def fetch(self, endpoint: str, trade_date: date) -> list[dict[str, Any]]: ...


def load_daily_members(pool_file: Path, holdings_file: Path | None) -> dict[str, str]:
    """Read the current pool and optional holdings without persisting personal details."""
    pool = json.loads(pool_file.read_text(encoding="utf-8"))
    if not isinstance(pool, dict) or not isinstance(pool.get("members"), list):
        raise ValueError("pool file requires a members list")
    members: dict[str, str] = {}
    for item in pool["members"]:
        symbol = QuoteSymbol.parse(item["ts_code"]).ts_code
        kind = item["instrument_type"]
        if kind not in PRICE_ENDPOINTS or symbol in members:
            raise ValueError(f"invalid or duplicate pool member: {symbol}")
        members[symbol] = kind
    if holdings_file is not None and holdings_file.exists():
        holdings = json.loads(holdings_file.read_text(encoding="utf-8"))
        for item in holdings["positions"]:
            symbol = QuoteSymbol.parse(item["symbol"]).ts_code
            if members.get(symbol, "stock") != "stock":
                raise ValueError(f"holding conflicts with ETF member: {symbol}")
            members[symbol] = "stock"
    if not members:
        raise ValueError("daily member set is empty")
    return members


def _number(value: object, name: str, *, positive: bool = False) -> float:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {name}: {value}") from exc
    if not math.isfinite(result) or result < 0 or (positive and result == 0):
        raise ValueError(f"invalid {name}: {value}")
    return result


def _normalize(
    raw: list[dict[str, Any]],
    *,
    trade_date: date,
    kind: str,
    dataset: str,
    members: dict[str, str],
) -> list[dict[str, object]]:
    expected_date = trade_date.strftime("%Y%m%d")
    endpoint = (PRICE_ENDPOINTS if dataset == "price" else FACTOR_ENDPOINTS)[kind]
    result: list[dict[str, object]] = []
    seen: set[str] = set()
    for source in raw:
        symbol = source.get("ts_code")
        if symbol not in members or members[symbol] != kind:
            continue
        if symbol in seen:
            raise ValueError(f"{endpoint} returned duplicate target symbol: {symbol}")
        if source.get("trade_date") != expected_date:
            raise ValueError(f"{endpoint} returned wrong trade date for {symbol}")
        seen.add(symbol)
        base: dict[str, object] = {
            "trade_date": trade_date,
            "ts_code": symbol,
            "instrument_type": kind,
            "source_endpoint": endpoint,
        }
        if dataset == "factor":
            base["adj_factor"] = _number(source.get("adj_factor"), "adj_factor", positive=True)
        else:
            prices = {
                key: _number(source.get(key), key, positive=True)
                for key in ("open", "high", "low", "close", "pre_close")
            }
            if prices["high"] < max(prices[key] for key in ("open", "close", "low")) or (
                prices["low"] > min(prices[key] for key in ("open", "close", "high"))
            ):
                raise ValueError(f"{endpoint} returned invalid OHLC for {symbol}")
            base.update(prices)
            # Tushare's native units are preserved exactly: hands and CNY thousands.
            base["change"] = float(source["change"])
            base["pct_chg"] = float(source["pct_chg"])
            base["vol"] = _number(source.get("vol"), "vol")
            base["amount"] = _number(source.get("amount"), "amount")
            if not math.isfinite(base["change"]) or not math.isfinite(base["pct_chg"]):  # type: ignore[arg-type]
                raise ValueError(f"{endpoint} returned invalid change for {symbol}")
        result.append(base)
    return result


def _schema(dataset: str) -> Any:
    import pyarrow as pa  # type: ignore[import-untyped]

    fields = [
        pa.field("trade_date", pa.date32(), nullable=False),
        pa.field("ts_code", pa.string(), nullable=False),
        pa.field("instrument_type", pa.string(), nullable=False),
        pa.field("source_endpoint", pa.string(), nullable=False),
    ]
    if dataset == "factor":
        fields.append(pa.field("adj_factor", pa.float64(), nullable=False))
    else:
        fields.extend(
            pa.field(name, pa.float64(), nullable=False)
            for name in (
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "change",
                "pct_chg",
                "vol",
                "amount",
            )
        )
    return pa.schema(fields, metadata={b"source": b"tushare", b"schema_version": b"1"})


def _write_partition(
    rows: list[dict[str, object]], *, dataset: str, trade_date: date, lake_root: Path
) -> tuple[Path, str]:
    import pyarrow as pa
    import pyarrow.parquet as pq  # type: ignore[import-untyped]

    directory = lake_root / f"dataset={dataset}" / f"trade_date={trade_date.isoformat()}"
    directory.mkdir(parents=True, exist_ok=True)
    temporary = directory / f".{uuid4().hex}.tmp.parquet"
    table = pa.Table.from_pylist(
        sorted(rows, key=lambda row: str(row["ts_code"])), schema=_schema(dataset)
    )
    try:
        pq.write_table(table, temporary, compression="zstd")
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        digest = hashlib.sha256(temporary.read_bytes()).hexdigest()
        destination = directory / f"part-{digest[:16]}.parquet"
        if destination.exists():
            if hashlib.sha256(destination.read_bytes()).hexdigest() != digest:
                raise RuntimeError(f"Parquet filename collision: {destination}")
            temporary.unlink()
        else:
            os.replace(temporary, destination)
        if pq.read_metadata(destination).num_rows != len(rows):
            raise RuntimeError(f"Parquet row count verification failed: {destination}")
        return destination.resolve(), digest
    finally:
        temporary.unlink(missing_ok=True)


def _set_partition(
    session: Session,
    *,
    dataset: str,
    trade_date: date,
    row_count: int,
    expected_count: int,
    member_sha256: str,
    missing: list[str],
    path: Path | None,
    digest: str | None,
    error: str | None = None,
) -> DailyLakePartition:
    partition = session.scalar(
        select(DailyLakePartition).where(
            DailyLakePartition.dataset == dataset, DailyLakePartition.trade_date == trade_date
        )
    )
    if partition is None:
        partition = DailyLakePartition(
            dataset=dataset,
            trade_date=trade_date,
            status="failed",
            expected_count=expected_count,
            member_sha256=member_sha256,
        )
        session.add(partition)
    elif error and partition.status == "complete":
        return partition  # A failed refresh must not erase a previously validated snapshot.
    partition.source = "tushare"
    partition.status = "failed" if error else ("complete" if not missing else "partial")
    partition.parquet_path = str(path) if path else None
    partition.sha256 = digest
    partition.row_count = row_count
    partition.expected_count = expected_count
    partition.member_sha256 = member_sha256
    partition.missing_symbols = missing
    partition.error = error
    partition.schema_version = SCHEMA_VERSION
    partition.updated_at = datetime.now(UTC)
    return partition


def partition_report(partition: DailyLakePartition) -> dict[str, object]:
    return {
        "dataset": partition.dataset,
        "trade_date": partition.trade_date.isoformat(),
        "status": partition.status,
        "row_count": partition.row_count,
        "expected_count": partition.expected_count,
        "member_sha256": partition.member_sha256,
        "missing_symbols": partition.missing_symbols,
        "path": partition.parquet_path,
        "sha256": partition.sha256,
        "error": partition.error,
    }


def sync_daily_date(
    *,
    source: DailySource,
    session_factory: sessionmaker[Session],
    lake_root: Path,
    trade_date: date,
    members: dict[str, str],
    refresh: bool = False,
) -> list[dict[str, object]]:
    """Fetch, validate, stage, then atomically activate both datasets in SQLite."""
    expected = len(members)
    member_sha256 = hashlib.sha256(
        json.dumps(members, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    with session_factory() as session:
        current = {
            row.dataset: row
            for row in session.scalars(
                select(DailyLakePartition).where(DailyLakePartition.trade_date == trade_date)
            )
        }
        if not refresh and all(
            dataset in current
            and current[dataset].status == "complete"
            and current[dataset].expected_count == expected
            and current[dataset].member_sha256 == member_sha256
            and current[dataset].schema_version == SCHEMA_VERSION
            and current[dataset].parquet_path is not None
            and Path(str(current[dataset].parquet_path)).is_file()
            and hashlib.sha256(Path(str(current[dataset].parquet_path)).read_bytes()).hexdigest()
            == current[dataset].sha256
            for dataset in ("price", "factor")
        ):
            return [partition_report(current[dataset]) for dataset in ("price", "factor")]

    staged: dict[str, tuple[list[dict[str, object]], list[str], Path, str]] = {}
    active_dataset = "price"
    try:
        for dataset, endpoints in (("price", PRICE_ENDPOINTS), ("factor", FACTOR_ENDPOINTS)):
            active_dataset = dataset
            rows: list[dict[str, object]] = []
            for kind, endpoint in endpoints.items():
                rows.extend(
                    _normalize(
                        source.fetch(endpoint, trade_date),
                        trade_date=trade_date,
                        kind=kind,
                        dataset=dataset,
                        members=members,
                    )
                )
            present = {str(row["ts_code"]) for row in rows}
            missing = sorted(set(members) - present)
            path, digest = _write_partition(
                rows, dataset=dataset, trade_date=trade_date, lake_root=lake_root
            )
            staged[dataset] = rows, missing, path, digest
    except Exception as exc:
        with session_factory.begin() as session:
            _set_partition(
                session,
                dataset=active_dataset,
                trade_date=trade_date,
                row_count=0,
                expected_count=expected,
                member_sha256=member_sha256,
                missing=sorted(members),
                path=None,
                digest=None,
                error=f"{type(exc).__name__}: {exc}",
            )
        raise

    with session_factory.begin() as session:
        results = [
            _set_partition(
                session,
                dataset=dataset,
                trade_date=trade_date,
                row_count=len(staged[dataset][0]),
                expected_count=expected,
                member_sha256=member_sha256,
                missing=staged[dataset][1],
                path=staged[dataset][2],
                digest=staged[dataset][3],
            )
            for dataset in ("price", "factor")
        ]
        session.flush()
        return [partition_report(row) for row in results]


def list_daily_partitions(
    session: Session, *, trade_date: date | None = None
) -> list[dict[str, object]]:
    statement = select(DailyLakePartition).order_by(
        DailyLakePartition.trade_date, DailyLakePartition.dataset
    )
    if trade_date is not None:
        statement = statement.where(DailyLakePartition.trade_date == trade_date)
    return [partition_report(row) for row in session.scalars(statement)]
