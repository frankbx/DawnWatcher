"""Feishu webhook and durable delivery-worker tests."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from regimebeacon.domain import NotificationStatus
from regimebeacon.notifications.feishu import (
    FeishuConfigurationError,
    FeishuCredentials,
    FeishuDeliveryError,
    FeishuDeliveryReceipt,
    FeishuWebhookClient,
    build_alert_card,
    build_market_analysis_card,
    build_market_status_card,
    build_text_card,
    format_notification_text,
    generate_signature,
)
from regimebeacon.notifications.outbox import enqueue_notification
from regimebeacon.notifications.worker import NotificationDeliveryWorker
from regimebeacon.storage.models import NotificationAttempt, NotificationOutbox


def _notification() -> NotificationOutbox:
    return NotificationOutbox(
        id="notification-1",
        idempotency_key="alert-1",
        event_type="operational.alert.triggered",
        channel="feishu",
        recipient="operators",
        payload={
            "alert_key": "market.collection.gap",
            "transition": "triggered",
            "severity": "critical",
            "summary": "no usable market collection arrived",
            "observed_at": "2026-09-28T02:00:00+00:00",
            "details": {
                "market_phase": "morning_continuous",
                "latest_usable_collection_at": None,
                "age_seconds": 61.5,
                "gap_after_seconds": 60,
            },
        },
    )


def test_credentials_are_read_from_secret_files(tmp_path: Path) -> None:
    webhook = tmp_path / "webhook"
    secret = tmp_path / "secret"
    webhook.write_text("https://open.feishu.cn/open-apis/bot/v2/hook/example\n")
    secret.write_text("signing-secret\n")

    credentials = FeishuCredentials.from_files(webhook, secret)

    assert credentials.webhook_url.endswith("/example")
    assert credentials.signing_secret == "signing-secret"


def test_credentials_reject_unapproved_webhook_host(tmp_path: Path) -> None:
    webhook = tmp_path / "webhook"
    webhook.write_text("https://example.test/open-apis/bot/v2/hook/leaked")

    with pytest.raises(FeishuConfigurationError, match="approved"):
        FeishuCredentials.from_files(webhook, tmp_path / "missing-secret")


def test_signature_and_alert_message_are_deterministic() -> None:
    assert generate_signature(1_700_000_000, "secret") == (
        "fiWS2+gh28DOydAv7hzONH/mDn9+b1Y4Y5ivXWXy8vA="
    )
    message = format_notification_text(_notification())
    assert "[RegimeBeacon]" in message
    assert "采集" in message
    assert "61.500 秒" in message


def test_alert_card_uses_schema_2_and_mobile_friendly_body() -> None:
    card = build_alert_card(_notification())

    assert card["schema"] == "2.0"
    assert card["config"] == {"update_multi": True, "width_mode": "fill"}
    assert card["header"] == {
        "title": {"tag": "plain_text", "content": "RegimeBeacon 行情系统告警"},
        "template": "red",
    }
    body = card["body"]
    assert isinstance(body, dict)
    elements = body["elements"]
    assert isinstance(elements, list)
    assert elements[0]["tag"] == "markdown"
    assert "61.500 秒" in elements[0]["content"]


def test_text_card_normalizes_and_validates_message() -> None:
    card = build_text_card("  旺财旺财  ")

    assert card["schema"] == "2.0"
    assert card["body"] == {"elements": [{"tag": "markdown", "content": "旺财旺财"}]}
    with pytest.raises(ValueError, match="cannot be empty"):
        build_text_card(" \n")


def test_market_status_card_uses_health_colored_schema_2_card() -> None:
    healthy = build_market_status_card("**状态**：正常", healthy=True)
    warning = build_market_status_card("**状态**：需关注", healthy=False)

    assert healthy["schema"] == "2.0"
    assert healthy["header"]["template"] == "green"
    assert healthy["header"]["title"]["content"] == "RegimeBeacon 行情采集状态"
    assert healthy["body"]["elements"] == [{"tag": "markdown", "content": "**状态**：正常"}]
    assert warning["header"]["template"] == "orange"


def test_market_analysis_card_uses_direction_color() -> None:
    card = build_market_analysis_card("**市场状态**：偏弱", direction="偏弱")

    assert card["schema"] == "2.0"
    assert card["header"] == {
        "title": {
            "tag": "plain_text",
            "content": "RegimeBeacon 市场温度与15分钟概览",
        },
        "template": "red",
    }

    risk_card = build_market_analysis_card("**市场温度**：风险收缩", direction="风险收缩")
    assert risk_card["header"]["template"] == "red"


def test_feishu_client_sends_signed_card_message() -> None:
    async def exercise() -> tuple[dict[str, object], FeishuDeliveryReceipt]:
        request_payload: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            request_payload.update(json.loads(request.content))
            return httpx.Response(
                200, json={"code": 0, "msg": "success"}, headers={"x-tt-logid": "log-1"}
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
            client = FeishuWebhookClient(
                FeishuCredentials(
                    webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/example",
                    signing_secret="secret",
                ),
                client=http_client,
                time_provider=lambda: 1_700_000_000,
            )
            receipt = await client.send(_notification())
        return request_payload, receipt

    payload, receipt = asyncio.run(exercise())

    assert payload["timestamp"] == 1_700_000_000
    assert payload["sign"] == generate_signature(1_700_000_000, "secret")
    assert payload["msg_type"] == "interactive"
    card = payload["card"]
    assert isinstance(card, dict)
    assert card["schema"] == "2.0"
    assert card["body"]["elements"][0]["tag"] == "markdown"
    assert receipt.provider_message_id == "log-1"


def test_feishu_client_sends_direct_card_without_outbox() -> None:
    async def exercise() -> dict[str, object]:
        request_payload: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            request_payload.update(json.loads(request.content))
            return httpx.Response(200, json={"code": 0, "msg": "success"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
            client = FeishuWebhookClient(
                FeishuCredentials(
                    webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/example",
                    signing_secret="secret",
                ),
                client=http_client,
                time_provider=lambda: 1_700_000_000,
            )
            await client.send_text("  旺财旺财  ")
        return request_payload

    payload = asyncio.run(exercise())

    assert payload["msg_type"] == "interactive"
    assert payload["card"]["schema"] == "2.0"
    assert payload["card"]["body"] == {"elements": [{"tag": "markdown", "content": "旺财旺财"}]}
    assert payload["sign"] == generate_signature(1_700_000_000, "secret")


def test_feishu_provider_error_is_sanitized() -> None:
    async def exercise() -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            del request
            return httpx.Response(200, json={"code": 19021, "msg": "sign match fail"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
            client = FeishuWebhookClient(
                FeishuCredentials(
                    webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/secret-token"
                ),
                client=http_client,
            )
            with pytest.raises(FeishuDeliveryError, match="19021") as raised:
                await client.send(_notification())
            assert "secret-token" not in str(raised.value)

    asyncio.run(exercise())


def test_delivery_worker_marks_success_and_attempt(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    class Sender:
        async def send(self, notification: NotificationOutbox) -> FeishuDeliveryReceipt:
            assert notification.channel == "feishu"
            return FeishuDeliveryReceipt(provider_message_id="provider-1")

    with session_factory_fixture.begin() as session:
        notification = enqueue_notification(
            session,
            idempotency_key="feishu-success",
            event_type="operational.alert.triggered",
            channel="feishu",
            recipient="operators",
            payload={},
        )

    result = asyncio.run(
        NotificationDeliveryWorker(session_factory_fixture, Sender()).deliver_batch(max_items=20)
    )

    with session_factory_fixture() as session:
        stored = session.get(NotificationOutbox, notification.id)
        attempts = list(session.scalars(select(NotificationAttempt)))
    assert result.sent == 1
    assert result.failed == 0
    assert stored is not None and stored.status is NotificationStatus.SENT
    assert stored.provider_message_id == "provider-1"
    assert len(attempts) == 1 and attempts[0].success is True


def test_delivery_worker_retries_sanitized_failure(
    session_factory_fixture: sessionmaker[Session],
) -> None:
    class Sender:
        async def send(self, notification: NotificationOutbox) -> FeishuDeliveryReceipt:
            del notification
            raise FeishuDeliveryError("Feishu returned HTTP 503")

    with session_factory_fixture.begin() as session:
        notification = enqueue_notification(
            session,
            idempotency_key="feishu-failure",
            event_type="operational.alert.triggered",
            channel="feishu",
            recipient="operators",
            payload={},
        )

    result = asyncio.run(
        NotificationDeliveryWorker(session_factory_fixture, Sender()).deliver_batch(max_items=20)
    )

    with session_factory_fixture() as session:
        stored = session.get(NotificationOutbox, notification.id)
    assert result.failed == 1
    assert stored is not None and stored.status is NotificationStatus.RETRYING
    assert stored.last_error == "Feishu returned HTTP 503"
