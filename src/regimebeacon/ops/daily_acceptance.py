"""Post-close evidence and final verdict for one trading day."""

from __future__ import annotations

import fcntl
import hashlib
import importlib
import json
import math
import os
from collections import Counter
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from statistics import fmean
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from regimebeacon.config import Settings
from regimebeacon.domain.jobs import JobStatus
from regimebeacon.notifications.outbox import enqueue_notification
from regimebeacon.storage.minute_parquet import PARQUET_SCHEMA_VERSION, load_instrument_metadata
from regimebeacon.storage.models import JobRun, MarketCollectionRun
from regimebeacon.workflows.job_runs import create_job_run, transition_job

_PHASE_WINDOWS = (
    ("opening_call_auction", time(9, 15), time(9, 25)),
    ("morning_continuous", time(9, 30), time(11, 30)),
    ("afternoon_continuous", time(13, 0), time(14, 57)),
    ("closing_call_auction", time(14, 57), time(15, 0)),
)
_REPORT_SCHEMA = 1


def run_daily_acceptance(
    settings: Settings,
    factory: sessionmaker[Session],
    *,
    trade_date: date,
    pool_file: Path,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    """Write one idempotent daily report and queue its Feishu card."""
    lock_path = settings.data_dir / "reports" / "daily" / trade_date.isoformat() / "acceptance.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("daily acceptance is already running") from exc
        return _run_daily_acceptance_locked(
            settings,
            factory,
            trade_date=trade_date,
            pool_file=pool_file,
            observed_at=observed_at,
        )
    finally:
        os.close(descriptor)


def _run_daily_acceptance_locked(
    settings: Settings,
    factory: sessionmaker[Session],
    *,
    trade_date: date,
    pool_file: Path,
    observed_at: datetime | None,
) -> dict[str, Any]:
    zone = ZoneInfo(settings.timezone)
    now = (observed_at or datetime.now(UTC)).astimezone(zone)
    if now < datetime.combine(trade_date, time(15, 1), tzinfo=zone):
        raise ValueError("daily acceptance requires the completed trading day")
    expected_symbols = set(load_instrument_metadata(pool_file))
    report_path = (
        settings.data_dir / "reports" / "daily" / trade_date.isoformat() / "acceptance.json"
    )
    key = f"daily-acceptance:{trade_date.isoformat()}:v{_REPORT_SCHEMA}"
    with factory.begin() as session:
        job = create_job_run(
            session,
            idempotency_key=key,
            job_type="daily_acceptance",
            trade_date=trade_date,
            scheduled_for=datetime.combine(trade_date, time(15, 2), tzinfo=zone),
            not_after=datetime.combine(trade_date, time(23, 58), tzinfo=zone),
        )
        if job.status is JobStatus.COMPLETE and report_path.is_file():
            return _load_existing_report(report_path, trade_date)
        was_complete = job.status is JobStatus.COMPLETE
        if job.status not in {
            JobStatus.SCHEDULED,
            JobStatus.RETRYING,
            JobStatus.RUNNING,
            JobStatus.COMPLETE,
        }:
            raise RuntimeError(f"daily acceptance job is {job.status.value}")
        if job.status is JobStatus.RUNNING:
            transition_job(session, job, JobStatus.RETRYING, now=now)
        if not was_complete:
            transition_job(session, job, JobStatus.PREFLIGHT, now=now, lease_seconds=1_800)
            transition_job(session, job, JobStatus.RUNNING, now=now, lease_seconds=1_800)

    try:
        with factory() as session:
            collections = list(
                session.scalars(
                    select(MarketCollectionRun)
                    .where(MarketCollectionRun.expected_trade_date == trade_date)
                    .order_by(MarketCollectionRun.started_at)
                )
            )
        collection_report = _assess_collections(
            collections,
            trade_date=trade_date,
            zone=zone,
            expected_symbols=expected_symbols,
            interval_seconds=settings.market_poll_interval_seconds,
        )
        seal_report = _assess_seals(
            settings.data_dir / "lake" / "minute_market" / f"trade_date={trade_date.isoformat()}",
            trade_date=trade_date,
            expected_symbol_count=len(expected_symbols),
        )
        failures: list[str] = []
        warnings: list[str] = []
        metrics = collection_report["metrics"]
        if collection_report["collection_count"] == 0:
            failures.append("当日没有腾讯采集记录")
        if collection_report["pool_symbol_count"] != len(expected_symbols):
            failures.append("采集股票池覆盖不完整")
        valid_rate = metrics["valid_quote_rate_pct"]
        if valid_rate is None or valid_rate < settings.acceptance_min_valid_quote_rate_pct:
            failures.append("有效行情率低于阈值")
        if collection_report["max_missing_gap_seconds"] > settings.acceptance_max_gap_seconds:
            failures.append("采集时间轴存在超阈值缺口")
        if seal_report["issues"]:
            failures.extend(seal_report["issues"])
        if collection_report["missing_slot_count"]:
            warnings.append("部分 15 秒采集槽位缺失")
        successful_rate = metrics["successful_run_rate_pct"]
        if (
            successful_rate is None
            or successful_rate < settings.acceptance_min_successful_run_rate_pct
        ):
            warnings.append("完整轮次率低于阈值")
        p95 = metrics["latency_ms"]["p95"]
        if p95 is None or p95 > settings.acceptance_max_p95_latency_ms:
            warnings.append("采集 P95 延迟超阈值")
        if metrics["circuit_opened_count"]:
            warnings.append("当日曾触发腾讯源熔断")
        verdict = "failed" if failures else "warning" if warnings else "passed"
        report: dict[str, Any] = {
            "schema_version": _REPORT_SCHEMA,
            "trade_date": trade_date.isoformat(),
            "generated_at": now.isoformat(),
            "verdict": verdict,
            "expected_symbol_count": len(expected_symbols),
            "thresholds": {
                "minimum_valid_quote_rate_pct": settings.acceptance_min_valid_quote_rate_pct,
                "minimum_successful_run_rate_pct": settings.acceptance_min_successful_run_rate_pct,
                "maximum_p95_latency_ms": settings.acceptance_max_p95_latency_ms,
                "maximum_gap_seconds": settings.acceptance_max_gap_seconds,
            },
            "failures": failures,
            "warnings": warnings,
            "collection": collection_report,
            "seals": seal_report,
        }
        _write_json_atomic(report_path, report)
        with factory.begin() as session:
            final_job = session.scalar(select(JobRun).where(JobRun.idempotency_key == key))
            if final_job is None:
                raise RuntimeError("daily acceptance job disappeared")
            final_job.payload = {
                "verdict": verdict,
                "report_path": str(report_path.resolve()),
                "failure_count": len(failures),
                "warning_count": len(warnings),
            }
            if final_job.status is not JobStatus.COMPLETE:
                transition_job(session, final_job, JobStatus.VALIDATING, now=now)
                transition_job(session, final_job, JobStatus.PUBLISHED, now=now)
                transition_job(session, final_job, JobStatus.COMPLETE, now=now)
            enqueue_notification(
                session,
                idempotency_key=f"daily-acceptance-report:{trade_date.isoformat()}:v{_REPORT_SCHEMA}",
                event_type="market.daily_acceptance.completed",
                channel=settings.alert_channel,
                recipient=settings.alert_recipient,
                payload={
                    "trade_date": trade_date.isoformat(),
                    "verdict": verdict,
                    "report_path": str(report_path.resolve()),
                    "failure_count": len(failures),
                    "warning_count": len(warnings),
                    "failures": failures[:8],
                    "warnings": warnings[:8],
                    "collection_count": collection_report["collection_count"],
                    "valid_quote_rate_pct": valid_rate,
                    "successful_run_rate_pct": successful_rate,
                    "p95_latency_ms": p95,
                    "missing_slot_count": collection_report["missing_slot_count"],
                    "max_missing_gap_seconds": collection_report["max_missing_gap_seconds"],
                    "day_complete": seal_report["day"]["complete"],
                    "day_row_count": seal_report["day"]["row_count"],
                    "expected_day_row_count": len(expected_symbols) * 250,
                },
            )
        return report
    except Exception:
        with factory.begin() as session:
            failed_job = session.scalar(select(JobRun).where(JobRun.idempotency_key == key))
            if failed_job is not None and failed_job.status is JobStatus.RUNNING:
                next_status = (
                    JobStatus.RETRYING
                    if failed_job.attempt_count < failed_job.max_attempts
                    else JobStatus.FAILED
                )
                transition_job(
                    session,
                    failed_job,
                    next_status,
                    now=now,
                    error_code="acceptance_execution_failed",
                )
        raise


def _assess_collections(
    collections: list[MarketCollectionRun],
    *,
    trade_date: date,
    zone: ZoneInfo,
    expected_symbols: set[str],
    interval_seconds: float,
) -> dict[str, Any]:
    single_source = [item for item in collections if set(item.provider_summaries) == {"tencent"}]
    requested_symbols: set[str] = set()
    successful = valid_quotes = requested_quotes = circuit_opened = 0
    latencies: list[float] = []
    quality_counts: Counter[str] = Counter()
    for item in single_source:
        requested_symbols.update(item.requested_symbols)
        requested = len(item.requested_symbols)
        requested_quotes += requested
        raw_summary = item.provider_summaries.get("tencent", {})
        summary = raw_summary if isinstance(raw_summary, dict) else {}
        valid = _safe_int(summary.get("valid_quote_count"))
        valid_quotes += valid
        issues = summary.get("batch_issues", [])
        issue_rows = issues if isinstance(issues, list) else []
        has_error = any(
            isinstance(issue, dict) and issue.get("severity") == "error" for issue in issue_rows
        )
        successful += int(valid == requested and requested > 0 and not has_error)
        circuit_opened += int(
            any(
                isinstance(issue, dict) and issue.get("code") == "circuit_opened"
                for issue in issue_rows
            )
        )
        elapsed = summary.get("elapsed_ms")
        if isinstance(elapsed, int | float) and not isinstance(elapsed, bool):
            latencies.append(float(elapsed))
        quality_counts.update({key: _safe_int(value) for key, value in item.quality_counts.items()})

    phase_reports: list[dict[str, Any]] = []
    all_missing: list[str] = []
    max_missing_gap = 0.0
    for name, start, end in _PHASE_WINDOWS:
        phase_start = datetime.combine(trade_date, start, tzinfo=zone)
        phase_end = datetime.combine(trade_date, end, tzinfo=zone)
        expected_count = math.ceil((phase_end - phase_start).total_seconds() / interval_seconds)
        occupied: set[int] = set()
        for item in single_source:
            local = item.started_at.astimezone(zone)
            if phase_start <= local < phase_end:
                slot = int((local - phase_start).total_seconds() // interval_seconds)
                if slot < expected_count:
                    occupied.add(slot)
        missing = [index for index in range(expected_count) if index not in occupied]
        longest = _longest_missing_run(missing) * interval_seconds
        max_missing_gap = max(max_missing_gap, longest)
        missing_times = [
            (phase_start + timedelta(seconds=index * interval_seconds)).strftime("%H:%M:%S")
            for index in missing
        ]
        all_missing.extend(missing_times)
        phase_reports.append(
            {
                "phase": name,
                "expected_slots": expected_count,
                "observed_slots": len(occupied),
                "missing_slot_count": len(missing),
                "max_missing_gap_seconds": longest,
                "missing_slots": missing_times,
            }
        )
    ordered_latencies = sorted(latencies)
    p95 = ordered_latencies[math.ceil(len(ordered_latencies) * 0.95) - 1] if latencies else None
    return {
        "collection_count": len(single_source),
        "legacy_dual_collection_count": len(collections) - len(single_source),
        "pool_symbol_count": len(requested_symbols & expected_symbols),
        "unexpected_symbols": sorted(requested_symbols - expected_symbols),
        "expected_slot_count": sum(row["expected_slots"] for row in phase_reports),
        "missing_slot_count": len(all_missing),
        "max_missing_gap_seconds": max_missing_gap,
        "missing_slots": all_missing,
        "phases": phase_reports,
        "quality_counts": dict(quality_counts),
        "metrics": {
            "successful_run_rate_pct": _percent(successful, len(single_source)),
            "valid_quote_rate_pct": _percent(valid_quotes, requested_quotes),
            "requested_quote_count": requested_quotes,
            "valid_quote_count": valid_quotes,
            "latency_ms": {
                "average": round(fmean(latencies), 3) if latencies else None,
                "p95": round(p95, 3) if p95 is not None else None,
                "max": round(ordered_latencies[-1], 3) if latencies else None,
            },
            "circuit_opened_count": circuit_opened,
        },
    }


def _assess_seals(day_dir: Path, *, trade_date: date, expected_symbol_count: int) -> dict[str, Any]:
    issues: list[str] = []
    sessions: dict[str, dict[str, Any]] = {}
    for name in ("morning", "afternoon"):
        sessions[name] = _validate_partition(
            day_dir / f"session={name}" / "manifest.json",
            day_dir / f"session={name}" / "part-000.parquet",
            trade_date=trade_date,
            expected_symbol_count=expected_symbol_count,
            expected_minute_count=130 if name == "morning" else 120,
        )
        if not sessions[name]["valid"]:
            issues.append(f"{name} 分钟分区缺失、损坏或不完整")
    day = _validate_partition(
        day_dir / "day-manifest.json",
        day_dir / "day.parquet",
        trade_date=trade_date,
        expected_symbol_count=expected_symbol_count,
        expected_minute_count=250,
        check_unique_keys=True,
    )
    if not day["valid"]:
        issues.append("整日分钟文件缺失、损坏或不完整")
    if day["valid"]:
        source_rows = day.get("source_sessions", [])
        for name in ("morning", "afternoon"):
            match = next(
                (
                    row
                    for row in source_rows
                    if isinstance(row, dict) and row.get("session") == name
                ),
                None,
            )
            if match is None or match.get("parquet_sha256") != sessions[name].get("parquet_sha256"):
                issues.append(f"整日文件的 {name} 来源校验和不匹配")
    return {
        "morning": sessions["morning"],
        "afternoon": sessions["afternoon"],
        "day": day,
        "issues": issues,
    }


def _validate_partition(
    manifest_path: Path,
    parquet_path: Path,
    *,
    trade_date: date,
    expected_symbol_count: int,
    expected_minute_count: int,
    check_unique_keys: bool = False,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "valid": False,
        "complete": False,
        "manifest_path": str(manifest_path.resolve()),
        "parquet_path": str(parquet_path.resolve()),
        "row_count": None,
        "parquet_sha256": None,
        "problems": [],
    }
    problems: list[str] = result["problems"]
    if not manifest_path.is_file() or not parquet_path.is_file():
        problems.append("missing_file")
        return result
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("manifest must be an object")
        result["complete"] = payload.get("complete") is True
        result["row_count"] = payload.get("row_count")
        result["parquet_sha256"] = payload.get("parquet_sha256")
        result["source_sessions"] = payload.get("source_sessions", [])
        for field, expected in (
            ("trade_date", trade_date.isoformat()),
            ("schema_version", PARQUET_SCHEMA_VERSION),
            ("expected_symbol_count", expected_symbol_count),
            ("expected_minute_count", expected_minute_count),
            ("row_count", expected_symbol_count * expected_minute_count),
            ("minute_count", expected_minute_count),
            ("symbol_count", expected_symbol_count),
        ):
            if payload.get(field) != expected:
                problems.append(f"{field}_mismatch")
        if not result["complete"]:
            problems.append("incomplete")
        if payload.get("parquet_size_bytes") != parquet_path.stat().st_size:
            problems.append("size_mismatch")
        digest = hashlib.sha256()
        with parquet_path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        if payload.get("parquet_sha256") != digest.hexdigest():
            problems.append("checksum_mismatch")
        parquet_module = importlib.import_module("pyarrow.parquet")
        parquet_file = parquet_module.ParquetFile(parquet_path)
        if parquet_file.metadata.num_rows != payload.get("row_count"):
            problems.append("parquet_row_count_mismatch")
        if check_unique_keys:
            table = parquet_file.read(columns=["provider", "symbol", "minute_start"])
            keys = set(
                zip(*(table.column(name).to_pylist() for name in table.column_names), strict=True)
            )
            if len(keys) != table.num_rows:
                problems.append("duplicate_minute_keys")
    except Exception as exc:
        problems.append(f"validation_error:{type(exc).__name__}")
    result["valid"] = not problems
    return result


def _longest_missing_run(indexes: list[int]) -> int:
    longest = current = 0
    previous: int | None = None
    for index in indexes:
        current = current + 1 if previous is not None and index == previous + 1 else 1
        longest = max(longest, current)
        previous = index
    return longest


def _safe_int(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


def _percent(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator * 100, 3) if denominator else None


def _load_existing_report(path: Path, trade_date: date) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("trade_date") != trade_date.isoformat():
        raise ValueError(f"invalid daily acceptance report: {path}")
    return payload


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.partial")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
