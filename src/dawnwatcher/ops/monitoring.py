"""Persistent runtime heartbeats and deduplicated operational health alerts."""

from __future__ import annotations

import os
import shutil
import socket
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any, NamedTuple

from sqlalchemy import select
from sqlalchemy.orm import Session

from dawnwatcher.config import Settings
from dawnwatcher.market import MarketPhase, MarketSessionStatus
from dawnwatcher.notifications.outbox import enqueue_notification
from dawnwatcher.storage.models import (
    MarketCollectionRun,
    OperationalAlert,
    RuntimeHeartbeat,
)

QUOTE_WATCHER_SERVICE = "quote_watcher"
_MANAGED_ALERT_KEYS = {
    "runtime.quote_watcher.heartbeat",
    "market.collection.gap",
    "storage.disk.free",
}
_HEARTBEAT_REQUIRED_PHASES = frozenset(
    {
        MarketPhase.OPENING_CALL_AUCTION,
        MarketPhase.OPENING_PAUSE,
        MarketPhase.MORNING_CONTINUOUS,
        MarketPhase.MIDDAY_BREAK,
        MarketPhase.AFTERNOON_CONTINUOUS,
        MarketPhase.CLOSING_CALL_AUCTION,
    }
)


class DiskUsage(NamedTuple):
    """Portable subset returned by shutil.disk_usage."""

    total: int
    used: int
    free: int


def _disk_usage(path: Path) -> DiskUsage:
    usage = shutil.disk_usage(path)
    return DiskUsage(total=usage.total, used=usage.used, free=usage.free)


@dataclass(frozen=True, slots=True)
class OperationalIssue:
    """One currently observed operational problem."""

    alert_key: str
    category: str
    severity: str
    summary: str
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def start_runtime_heartbeat(
    session: Session,
    *,
    service_name: str,
    instance_id: str,
    interval_seconds: float,
    now: datetime,
    details: dict[str, Any] | None = None,
) -> RuntimeHeartbeat:
    """Register a new long-running process instance."""
    _require_aware(now)
    heartbeat = RuntimeHeartbeat(
        service_name=service_name,
        instance_id=instance_id,
        process_id=os.getpid(),
        hostname=socket.gethostname(),
        status="running",
        interval_seconds=interval_seconds,
        started_at=now,
        heartbeat_at=now,
        details=details or {},
    )
    session.add(heartbeat)
    session.flush()
    return heartbeat


def touch_runtime_heartbeat(
    session: Session,
    *,
    instance_id: str,
    now: datetime,
    details: dict[str, Any],
) -> RuntimeHeartbeat:
    """Update the liveness time and latest process counters."""
    _require_aware(now)
    heartbeat = _heartbeat_for_update(session, instance_id)
    heartbeat.status = "running"
    heartbeat.heartbeat_at = now
    heartbeat.stopped_at = None
    heartbeat.details = details
    session.flush()
    return heartbeat


def stop_runtime_heartbeat(
    session: Session,
    *,
    instance_id: str,
    now: datetime,
    details: dict[str, Any],
) -> RuntimeHeartbeat:
    """Mark a process instance as cleanly stopped."""
    _require_aware(now)
    heartbeat = _heartbeat_for_update(session, instance_id)
    heartbeat.status = "stopped"
    heartbeat.heartbeat_at = now
    heartbeat.stopped_at = now
    heartbeat.details = details
    session.flush()
    return heartbeat


def run_operational_checks(
    session: Session,
    *,
    settings: Settings,
    market_status: MarketSessionStatus,
    observed_at: datetime,
    disk_usage: Callable[[Path], DiskUsage] = _disk_usage,
) -> dict[str, Any]:
    """Evaluate liveness, market collection freshness, and disk capacity."""
    _require_aware(observed_at)
    issues: list[OperationalIssue] = []
    checks: dict[str, Any] = {}

    heartbeat = session.scalar(
        select(RuntimeHeartbeat)
        .where(
            RuntimeHeartbeat.service_name == QUOTE_WATCHER_SERVICE,
            RuntimeHeartbeat.status == "running",
        )
        .order_by(RuntimeHeartbeat.heartbeat_at.desc())
        .limit(1)
    )
    heartbeat_applicable = (
        market_status.calendar_date_known
        and market_status.is_trading_day
        and market_status.phase in _HEARTBEAT_REQUIRED_PHASES
    )
    heartbeat_age: float | None = None
    if heartbeat is not None:
        heartbeat_age = max(0.0, (observed_at - heartbeat.heartbeat_at).total_seconds())
    heartbeat_ok = not heartbeat_applicable or (
        heartbeat_age is not None and heartbeat_age <= settings.heartbeat_stale_seconds
    )
    checks["heartbeat"] = {
        "ok": heartbeat_ok,
        "applicable": heartbeat_applicable,
        "service_name": QUOTE_WATCHER_SERVICE,
        "instance_id": heartbeat.instance_id if heartbeat is not None else None,
        "last_seen_at": heartbeat.heartbeat_at.isoformat() if heartbeat is not None else None,
        "age_seconds": round(heartbeat_age, 3) if heartbeat_age is not None else None,
        "stale_after_seconds": settings.heartbeat_stale_seconds,
        "reason": (
            "quote watcher is required during the scheduled trading-day runtime"
            if heartbeat_applicable
            else "quote watcher is not required outside the scheduled trading-day runtime"
        ),
    }
    if heartbeat_applicable and not heartbeat_ok:
        issues.append(
            OperationalIssue(
                alert_key="runtime.quote_watcher.heartbeat",
                category="runtime",
                severity="critical",
                summary="quote watcher heartbeat is missing or stale",
                details=checks["heartbeat"],
            )
        )

    gap_check = _collection_gap_check(
        session,
        market_status=market_status,
        observed_at=observed_at,
        gap_seconds=settings.collection_gap_seconds,
        heartbeat=heartbeat,
    )
    checks["collection_gap"] = gap_check
    if not gap_check["ok"]:
        issues.append(
            OperationalIssue(
                alert_key="market.collection.gap",
                category="market_data",
                severity="critical",
                summary="no usable market collection arrived within the configured window",
                details=gap_check,
            )
        )

    usage = disk_usage(settings.data_dir.resolve())
    free_pct = usage.free / usage.total * 100 if usage.total else 0.0
    disk_severity: str | None = None
    if usage.free <= settings.disk_critical_free_bytes:
        disk_severity = "critical"
    elif usage.free <= settings.disk_warning_free_bytes:
        disk_severity = "warning"
    disk_check = {
        "ok": disk_severity is None,
        "path": str(settings.data_dir.resolve()),
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "free_percent": round(free_pct, 3),
        "warning_free_bytes": settings.disk_warning_free_bytes,
        "critical_free_bytes": settings.disk_critical_free_bytes,
    }
    checks["disk"] = disk_check
    if disk_severity is not None:
        issues.append(
            OperationalIssue(
                alert_key="storage.disk.free",
                category="storage",
                severity=disk_severity,
                summary="runtime data filesystem is low on free space",
                details=disk_check,
            )
        )

    transitions = _synchronize_alerts(
        session,
        issues=issues,
        observed_at=observed_at,
        channel=settings.alert_channel,
        recipient=settings.alert_recipient,
    )
    return {
        "observed_at": observed_at.isoformat(),
        "ok": not issues,
        "market_session": market_status.to_dict(),
        "checks": checks,
        "active_issues": [issue.to_dict() for issue in issues],
        "alert_transitions": transitions,
    }


def _collection_gap_check(
    session: Session,
    *,
    market_status: MarketSessionStatus,
    observed_at: datetime,
    gap_seconds: float,
    heartbeat: RuntimeHeartbeat | None,
) -> dict[str, Any]:
    if not market_status.collect_quotes:
        return {
            "ok": True,
            "applicable": False,
            "reason": "market session does not currently collect quotes",
            "gap_after_seconds": gap_seconds,
        }

    recent = list(
        session.scalars(
            select(MarketCollectionRun)
            .where(
                MarketCollectionRun.expected_trade_date == market_status.trade_date,
                MarketCollectionRun.market_phase == market_status.phase,
                MarketCollectionRun.finished_at <= observed_at,
            )
            .order_by(MarketCollectionRun.finished_at.desc())
            .limit(100)
        )
    )
    latest_usable = next((item for item in recent if _collection_is_usable(item)), None)
    phase_started_at = _phase_started_at(market_status)
    reference_times = [phase_started_at]
    if latest_usable is not None:
        reference_times.append(latest_usable.finished_at)
    if heartbeat is not None:
        reference_times.append(heartbeat.started_at)
    reference_at = max(reference_times)
    age = max(0.0, (observed_at - reference_at).total_seconds())
    return {
        "ok": age <= gap_seconds,
        "applicable": True,
        "trade_date": market_status.trade_date.isoformat(),
        "market_phase": market_status.phase.value,
        "latest_usable_collection_at": (
            latest_usable.finished_at.isoformat() if latest_usable is not None else None
        ),
        "reference_at": reference_at.isoformat(),
        "age_seconds": round(age, 3),
        "gap_after_seconds": gap_seconds,
    }


def _collection_is_usable(collection: MarketCollectionRun) -> bool:
    usable_states = ("complete", "near", "degraded")
    return any(_safe_count(collection.quality_counts.get(state)) > 0 for state in usable_states)


def _phase_started_at(status: MarketSessionStatus) -> datetime:
    starts = {
        MarketPhase.OPENING_CALL_AUCTION: time(9, 15),
        MarketPhase.MORNING_CONTINUOUS: time(9, 30),
        MarketPhase.AFTERNOON_CONTINUOUS: time(13, 0),
        MarketPhase.CLOSING_CALL_AUCTION: time(14, 57),
    }
    phase_time = starts.get(status.phase)
    if phase_time is None:
        return status.observed_at
    return datetime.combine(status.trade_date, phase_time, tzinfo=status.observed_at.tzinfo)


def _synchronize_alerts(
    session: Session,
    *,
    issues: list[OperationalIssue],
    observed_at: datetime,
    channel: str,
    recipient: str,
) -> list[dict[str, str]]:
    transitions: list[dict[str, str]] = []
    current = {issue.alert_key: issue for issue in issues}
    existing = {
        alert.alert_key: alert
        for alert in session.scalars(
            select(OperationalAlert).where(OperationalAlert.alert_key.in_(_MANAGED_ALERT_KEYS))
        )
    }

    for key, issue in current.items():
        alert = existing.get(key)
        transition: str | None = None
        if alert is None:
            alert = OperationalAlert(
                alert_key=key,
                category=issue.category,
                severity=issue.severity,
                status="active",
                summary=issue.summary,
                details=issue.details,
                first_triggered_at=observed_at,
                last_observed_at=observed_at,
                occurrence_count=1,
            )
            session.add(alert)
            session.flush()
            existing[key] = alert
            transition = "triggered"
        elif alert.status != "active":
            alert.category = issue.category
            alert.severity = issue.severity
            alert.status = "active"
            alert.summary = issue.summary
            alert.details = issue.details
            alert.first_triggered_at = observed_at
            alert.last_observed_at = observed_at
            alert.resolved_at = None
            alert.occurrence_count += 1
            transition = "triggered"
        else:
            previous_severity = alert.severity
            alert.category = issue.category
            alert.severity = issue.severity
            alert.summary = issue.summary
            alert.details = issue.details
            alert.last_observed_at = observed_at
            alert.occurrence_count += 1
            if _severity_rank(issue.severity) > _severity_rank(previous_severity):
                transition = "escalated"

        if transition is not None:
            _enqueue_alert_notification(
                session,
                alert=alert,
                transition=transition,
                observed_at=observed_at,
                channel=channel,
                recipient=recipient,
            )
            transitions.append({"alert_key": key, "transition": transition})

    for key, alert in existing.items():
        if key in current or alert.status != "active":
            continue
        alert.status = "resolved"
        alert.last_observed_at = observed_at
        alert.resolved_at = observed_at
        _enqueue_alert_notification(
            session,
            alert=alert,
            transition="resolved",
            observed_at=observed_at,
            channel=channel,
            recipient=recipient,
        )
        transitions.append({"alert_key": key, "transition": "resolved"})

    session.flush()
    return transitions


def _enqueue_alert_notification(
    session: Session,
    *,
    alert: OperationalAlert,
    transition: str,
    observed_at: datetime,
    channel: str,
    recipient: str,
) -> None:
    enqueue_notification(
        session,
        idempotency_key=(
            f"operational-alert:{alert.alert_key}:{transition}:{observed_at.isoformat()}"
        ),
        event_type=f"operational.alert.{transition}",
        channel=channel,
        recipient=recipient,
        payload={
            "alert_key": alert.alert_key,
            "transition": transition,
            "severity": alert.severity,
            "summary": alert.summary,
            "details": alert.details,
            "observed_at": observed_at.isoformat(),
        },
    )


def _heartbeat_for_update(session: Session, instance_id: str) -> RuntimeHeartbeat:
    heartbeat = session.scalar(
        select(RuntimeHeartbeat).where(RuntimeHeartbeat.instance_id == instance_id)
    )
    if heartbeat is None:
        raise ValueError(f"unknown runtime heartbeat instance: {instance_id}")
    return heartbeat


def _severity_rank(value: str) -> int:
    return {"warning": 1, "critical": 2}.get(value, 0)


def _safe_count(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _require_aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("monitoring timestamps must be timezone-aware")
    if value.tzinfo is not UTC:
        value.astimezone(UTC)
