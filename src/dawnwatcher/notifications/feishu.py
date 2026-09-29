"""Feishu custom-bot webhook delivery with optional signature verification."""

from __future__ import annotations

import base64
import hashlib
import hmac
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from dawnwatcher.storage.models import NotificationOutbox

_ALLOWED_HOSTS = frozenset({"open.feishu.cn", "open.larksuite.com"})
_WEBHOOK_PATH_PREFIX = "/open-apis/bot/v2/hook/"


class FeishuConfigurationError(ValueError):
    """Raised when local Feishu credentials are missing or unsafe."""


class FeishuDeliveryError(RuntimeError):
    """Raised for a sanitized HTTP or provider-level delivery failure."""


@dataclass(frozen=True, slots=True)
class FeishuCredentials:
    """Secrets required by one custom-bot webhook."""

    webhook_url: str
    signing_secret: str | None = None

    @classmethod
    def from_files(cls, webhook_file: Path, signing_secret_file: Path) -> FeishuCredentials:
        """Read secrets without exposing their contents in errors or logs."""
        webhook_url = _read_required_secret(webhook_file, label="Feishu webhook")
        _validate_webhook_url(webhook_url)
        signing_secret = _read_optional_secret(signing_secret_file, label="Feishu signing secret")
        return cls(webhook_url=webhook_url, signing_secret=signing_secret)


@dataclass(frozen=True, slots=True)
class FeishuDeliveryReceipt:
    """Non-secret result returned after Feishu accepts a message."""

    provider_message_id: str | None = None


class FeishuWebhookClient:
    """Send operational alerts to a Feishu group custom bot."""

    def __init__(
        self,
        credentials: FeishuCredentials,
        *,
        timeout_seconds: float = 8.0,
        client: httpx.AsyncClient | None = None,
        time_provider: Callable[[], float] = time.time,
    ) -> None:
        self.credentials = credentials
        self._owns_client = client is None
        self.client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds),
            follow_redirects=False,
        )
        self.time_provider = time_provider

    async def __aenter__(self) -> FeishuWebhookClient:
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc_type, exc, traceback
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def send(self, notification: NotificationOutbox) -> FeishuDeliveryReceipt:
        """Send one leased outbox notification as a Feishu Card JSON 2.0 card."""
        return await self.send_card(build_alert_card(notification))

    async def send_text(self, message: str) -> FeishuDeliveryReceipt:
        """Send one direct message as a Card JSON 2.0 card without an outbox record.

        The method name is retained for CLI/API compatibility.  All DawnWatcher
        pushes, including direct test messages, use an interactive card payload.
        """
        normalized = message.strip()
        if not normalized:
            raise ValueError("Feishu text message cannot be empty")
        if len(normalized) > 4_000:
            raise ValueError("Feishu text message cannot exceed 4000 characters")
        return await self.send_card(build_text_card(normalized))

    async def send_card(self, card: dict[str, Any]) -> FeishuDeliveryReceipt:
        """Send an interactive Feishu Card JSON 2.0 payload."""
        if card.get("schema") != "2.0":
            raise ValueError('Feishu card must declare schema "2.0"')
        if not isinstance(card.get("body"), dict):
            raise ValueError("Feishu card must include a body object")
        request_payload: dict[str, Any] = {"msg_type": "interactive", "card": card}
        if self.credentials.signing_secret is not None:
            timestamp = int(self.time_provider())
            request_payload.update(
                {
                    "timestamp": timestamp,
                    "sign": generate_signature(timestamp, self.credentials.signing_secret),
                }
            )
        return await self._post_payload(request_payload)

    async def _post_payload(self, request_payload: dict[str, Any]) -> FeishuDeliveryReceipt:
        try:
            response = await self.client.post(
                self.credentials.webhook_url,
                json=request_payload,
                headers={"Content-Type": "application/json; charset=utf-8"},
            )
        except httpx.TimeoutException as exc:
            raise FeishuDeliveryError("Feishu request timed out") from exc
        except httpx.HTTPError as exc:
            raise FeishuDeliveryError(
                "Feishu request failed before a response was received"
            ) from exc

        if response.status_code < 200 or response.status_code >= 300:
            raise FeishuDeliveryError(f"Feishu returned HTTP {response.status_code}")
        try:
            payload = response.json()
        except ValueError as exc:
            raise FeishuDeliveryError("Feishu returned a non-JSON response") from exc
        if not isinstance(payload, dict):
            raise FeishuDeliveryError("Feishu returned an invalid response object")
        _require_success(payload)
        request_id = response.headers.get("x-tt-logid") or response.headers.get("x-request-id")
        return FeishuDeliveryReceipt(provider_message_id=request_id)


def generate_signature(timestamp: int, signing_secret: str) -> str:
    """Generate the signature required by Feishu custom-bot secret verification."""
    if timestamp < 0:
        raise ValueError("timestamp cannot be negative")
    if not signing_secret:
        raise ValueError("signing secret cannot be empty")
    string_to_sign = f"{timestamp}\n{signing_secret}".encode()
    digest = hmac.new(string_to_sign, digestmod=hashlib.sha256).digest()
    return base64.b64encode(digest).decode("ascii")


def format_notification_text(notification: NotificationOutbox) -> str:
    """Render a concise mobile-friendly operational alert."""
    payload = notification.payload
    transition = str(payload.get("transition", "triggered"))
    transition_text = {
        "triggered": "触发",
        "escalated": "升级",
        "resolved": "恢复",
    }.get(transition, transition)
    severity = str(payload.get("severity", "unknown"))
    severity_text = {"critical": "严重", "warning": "警告"}.get(severity, severity)
    lines = [
        "[DawnWatcher] 行情系统告警",
        f"状态：{transition_text}",
        f"级别：{severity_text}",
        f"告警：{payload.get('summary', notification.event_type)}",
        f"标识：{payload.get('alert_key', notification.event_type)}",
    ]
    observed_at = payload.get("observed_at")
    if observed_at:
        lines.append(f"时间：{observed_at}")
    details = payload.get("details")
    if isinstance(details, dict):
        lines.extend(_format_operational_details(str(payload.get("alert_key", "")), details))
    lines.append(f"通知ID：{notification.id}")
    # Feishu text messages have generous limits, but bounded content prevents accidental floods.
    return "\n".join(lines)[:4_000]


def build_alert_card(notification: NotificationOutbox) -> dict[str, Any]:
    """Build a mobile-friendly Feishu Card JSON 2.0 alert."""
    payload = notification.payload
    transition = str(payload.get("transition", "triggered"))
    transition_text = {
        "triggered": "触发",
        "escalated": "升级",
        "resolved": "恢复",
    }.get(transition, transition)
    severity = str(payload.get("severity", "unknown"))
    severity_text = {"critical": "严重", "warning": "警告"}.get(severity, severity)
    template = {
        "critical": "red",
        "warning": "orange",
        "resolved": "green",
    }.get(severity if transition != "resolved" else "resolved", "blue")
    title = "DawnWatcher 告警恢复" if transition == "resolved" else "DawnWatcher 行情系统告警"
    content = format_notification_text(notification)
    # Card 2.0 markdown is deliberately kept to one bounded element so that
    # long provider details cannot create an unbounded request body.
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "title": {"tag": "plain_text", "content": title},
            "template": template,
        },
        "body": {
            "elements": [
                {
                    "tag": "markdown",
                    "content": (
                        f"**状态**：{transition_text}  ·  **级别**：{severity_text}\n\n{content}"
                    )[:4_000],
                }
            ]
        },
    }


def build_text_card(message: str) -> dict[str, Any]:
    """Build a Card JSON 2.0 wrapper for a direct test message."""
    normalized = message.strip()
    if not normalized:
        raise ValueError("Feishu text message cannot be empty")
    if len(normalized) > 4_000:
        raise ValueError("Feishu text message cannot exceed 4000 characters")
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "title": {"tag": "plain_text", "content": "DawnWatcher 测试消息"},
            "template": "blue",
        },
        "body": {"elements": [{"tag": "markdown", "content": normalized}]},
    }


def build_market_status_card(message: str, *, healthy: bool) -> dict[str, Any]:
    """Build a mobile-friendly Card JSON 2.0 market collection status report."""
    normalized = message.strip()
    if not normalized:
        raise ValueError("Feishu market status report cannot be empty")
    if len(normalized) > 4_000:
        raise ValueError("Feishu market status report cannot exceed 4000 characters")
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "title": {"tag": "plain_text", "content": "DawnWatcher 行情采集状态"},
            "template": "green" if healthy else "orange",
        },
        "body": {"elements": [{"tag": "markdown", "content": normalized}]},
    }


def build_market_analysis_card(message: str, *, direction: str) -> dict[str, Any]:
    """Build a Card JSON 2.0 market-temperature and sector-rotation report."""
    normalized = message.strip()
    if not normalized:
        raise ValueError("Feishu market analysis report cannot be empty")
    if len(normalized) > 4_000:
        raise ValueError("Feishu market analysis report cannot exceed 4000 characters")
    template = {
        "过热": "orange",
        "偏暖": "green",
        "偏冷": "orange",
        "风险收缩": "red",
        "偏强": "green",
        "偏弱": "red",
        "震荡/分化": "blue",
        "中性/分化": "blue",
        "数据不足": "grey",
    }.get(direction, "blue")
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "title": {
                "tag": "plain_text",
                "content": "DawnWatcher 市场温度与15分钟概览",
            },
            "template": template,
        },
        "body": {"elements": [{"tag": "markdown", "content": normalized}]},
    }


def _format_operational_details(alert_key: str, details: dict[str, Any]) -> list[str]:
    if alert_key == "runtime.quote_watcher.heartbeat":
        return [
            f"最后心跳：{details.get('last_seen_at') or '无'}",
            f"心跳年龄：{_seconds(details.get('age_seconds'))}",
            f"告警阈值：{_seconds(details.get('stale_after_seconds'))}",
        ]
    if alert_key == "market.collection.gap":
        return [
            f"交易阶段：{details.get('market_phase') or '未知'}",
            f"最近可用采集：{details.get('latest_usable_collection_at') or '无'}",
            f"缺口时长：{_seconds(details.get('age_seconds'))}",
            f"告警阈值：{_seconds(details.get('gap_after_seconds'))}",
        ]
    if alert_key == "storage.disk.free":
        return [
            f"路径：{details.get('path') or '未知'}",
            f"剩余空间：{_bytes(details.get('free_bytes'))}",
            f"剩余比例：{details.get('free_percent', '未知')}%",
        ]
    return []


def _seconds(value: object) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f"{value:.3f} 秒"
    return "未知"


def _bytes(value: object) -> str:
    if isinstance(value, int) and not isinstance(value, bool):
        return f"{value / 1024**3:.2f} GiB"
    return "未知"


def _require_success(payload: dict[str, Any]) -> None:
    if "code" in payload:
        code = payload.get("code")
        if code in (0, "0"):
            return
        message = payload.get("msg") or "unknown provider error"
        raise FeishuDeliveryError(f"Feishu rejected the message: code={code}, message={message}")
    if "StatusCode" in payload:
        code = payload.get("StatusCode")
        if code in (0, "0"):
            return
        message = payload.get("StatusMessage") or "unknown provider error"
        raise FeishuDeliveryError(f"Feishu rejected the message: code={code}, message={message}")
    raise FeishuDeliveryError("Feishu response did not include a result code")


def _read_required_secret(path: Path, *, label: str) -> str:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError as exc:
        raise FeishuConfigurationError(f"{label} file does not exist: {path}") from exc
    except OSError as exc:
        raise FeishuConfigurationError(f"{label} file cannot be read: {path}") from exc
    if not value or "\n" in value or "\r" in value:
        raise FeishuConfigurationError(f"{label} file must contain exactly one non-empty value")
    return value


def _read_optional_secret(path: Path, *, label: str) -> str | None:
    if not path.exists():
        return None
    return _read_required_secret(path, label=label)


def _validate_webhook_url(webhook_url: str) -> None:
    parsed = urlparse(webhook_url)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in _ALLOWED_HOSTS
        or not parsed.path.startswith(_WEBHOOK_PATH_PREFIX)
        or len(parsed.path) <= len(_WEBHOOK_PATH_PREFIX)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise FeishuConfigurationError("Feishu webhook URL is not an approved custom-bot endpoint")
