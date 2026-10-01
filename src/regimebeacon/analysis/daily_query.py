"""DuckDB reads only SQLite-approved Tushare Parquet objects."""

from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from regimebeacon.storage.models import DailyLakePartition


def _active_paths(
    session: Session,
    *,
    dataset: str,
    start_date: date,
    end_date: date,
    lake_root: Path,
) -> list[str]:
    rows = session.scalars(
        select(DailyLakePartition)
        .where(
            DailyLakePartition.dataset == dataset,
            DailyLakePartition.status == "complete",
            DailyLakePartition.trade_date >= start_date,
            DailyLakePartition.trade_date <= end_date,
        )
        .order_by(DailyLakePartition.trade_date)
    ).all()
    root = lake_root.resolve()
    paths: list[str] = []
    for row in rows:
        if row.schema_version != 1:
            raise RuntimeError(f"unsupported {dataset} schema version for {row.trade_date}")
        if not row.parquet_path or not row.sha256:
            raise RuntimeError(f"invalid {dataset} control record for {row.trade_date}")
        path = Path(row.parquet_path).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise RuntimeError(f"missing or unsafe active Parquet object: {path}")
        if hashlib.sha256(path.read_bytes()).hexdigest() != row.sha256:
            raise RuntimeError(f"active Parquet checksum mismatch: {path}")
        paths.append(str(path))
    return paths


def query_daily_history(
    session: Session,
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    lake_root: Path,
) -> list[dict[str, Any]]:
    """Return raw OHLCV plus an optional separate factor; never auto-adjust prices."""
    if end_date < start_date:
        raise ValueError("end_date cannot precede start_date")
    import duckdb

    prices = _active_paths(
        session, dataset="price", start_date=start_date, end_date=end_date, lake_root=lake_root
    )
    if not prices:
        return []
    factors = _active_paths(
        session, dataset="factor", start_date=start_date, end_date=end_date, lake_root=lake_root
    )
    with duckdb.connect(":memory:") as connection:
        connection.read_parquet(prices).create_view("prices")
        if factors:
            connection.read_parquet(factors).create_view("factors")
            factor_join = (
                "LEFT JOIN factors f ON p.ts_code = f.ts_code "
                "AND p.trade_date = f.trade_date AND p.instrument_type = f.instrument_type"
            )
            factor_column = "f.adj_factor"
        else:
            factor_join = ""
            factor_column = "CAST(NULL AS DOUBLE)"
        cursor = connection.execute(
            "SELECT p.trade_date, p.ts_code, p.instrument_type, p.open, p.high, p.low, "
            "p.close, p.pre_close, p.change, p.pct_chg, p.vol, p.amount, "
            f"{factor_column} AS adj_factor FROM prices p {factor_join} "
            "WHERE p.ts_code = ? AND p.trade_date BETWEEN ? AND ? ORDER BY p.trade_date",
            [symbol, start_date, end_date],
        )
        names = [column[0] for column in cursor.description]
        return [
            {
                key: value.isoformat() if isinstance(value, date) else value
                for key, value in zip(names, values, strict=True)
            }
            for values in cursor.fetchall()
        ]


def summarize_daily_pool(
    session: Session, *, trade_date: date, lake_root: Path
) -> dict[str, object]:
    """Cross-sectional summary of the configured sample, not the entire A-share market."""
    import duckdb

    paths = _active_paths(
        session, dataset="price", start_date=trade_date, end_date=trade_date, lake_root=lake_root
    )
    if not paths:
        raise ValueError(f"no complete price partition for {trade_date}")
    with duckdb.connect(":memory:") as connection:
        connection.read_parquet(paths).create_view("prices")
        cursor = connection.execute(
            "SELECT instrument_type, COUNT(*) AS instruments, "
            "COUNT(*) FILTER (WHERE pct_chg > 0) AS up, "
            "COUNT(*) FILTER (WHERE pct_chg < 0) AS down, "
            "COUNT(*) FILTER (WHERE pct_chg = 0) AS flat, "
            "ROUND(AVG(pct_chg), 4) AS average_change_pct, "
            "ROUND(SUM(amount) * 1000, 2) AS turnover_cny "
            "FROM prices GROUP BY instrument_type ORDER BY instrument_type"
        )
        names = [column[0] for column in cursor.description]
        groups = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    return {
        "trade_date": trade_date.isoformat(),
        "scope": "configured_pool_and_holdings",
        "groups": groups,
    }
