"""Feishu custom-bot webhook delivery with optional signature verification."""

from __future__ import annotations

import base64
import hashlib
import hmac
import time
from collections.abc import Callable
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from regimebeacon.storage.models import NotificationOutbox

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

        The method name is retained for CLI/API compatibility.  All RegimeBeacon
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
        "[RegimeBeacon] 行情系统告警",
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
    if notification.event_type == "market.daily_acceptance.completed":
        return build_daily_acceptance_card(notification.payload)
    if notification.event_type == "portfolio.holdings_review.completed":
        return build_holdings_card(notification.payload)
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
    title = "RegimeBeacon 告警恢复" if transition == "resolved" else "RegimeBeacon 行情系统告警"
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


def build_holdings_card(payload: dict[str, Any]) -> dict[str, Any]:
    """A dedicated holdings review card, separate from market breadth reports."""
    markdown = str(payload.get("markdown", "")).strip()
    if not markdown:
        raise ValueError("holdings card requires report markdown")
    healthy = payload.get("data_complete") is True
    report = payload.get("report")
    window = report.get("window_minutes") if isinstance(report, dict) else None
    title = (
        f"RegimeBeacon 持仓{window}分钟观察"
        if isinstance(window, int) and not isinstance(window, bool) and window > 0
        else "RegimeBeacon 持仓观察"
    )
    positions = report.get("positions") if isinstance(report, dict) else None
    if (
        isinstance(positions, list)
        and positions
        and all(isinstance(item, dict) for item in positions)
    ):
        elements = _holdings_table_elements(report, positions)
        states = {str(item.get("state", "")) for item in positions}
        template = (
            "red"
            if "风险升高" in states
            else "orange"
            if not healthy or states & {"放量走弱", "短线偏弱"}
            else "blue"
        )
    else:
        elements = [{"tag": "markdown", "content": markdown[:4_000]}]
        template = "blue" if healthy else "orange"
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "title": {"tag": "plain_text", "content": title},
            "template": template,
        },
        "body": {"elements": elements},
    }


def _holdings_table_elements(
    report: dict[str, Any], positions: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    observed_at = str(report.get("observed_at", ""))
    window = report.get("window_minutes")
    window_label = f"近{window}分" if isinstance(window, int) else "本窗"
    valid = sum(item.get("latest_cny") is not None for item in positions)
    risk = [item for item in positions if item.get("state") in {"风险升高", "放量走弱", "短线偏弱"}]
    summary = [
        f"**{observed_at[:16].replace('T', ' ')}** · 有效行情 {valid}/{len(positions)}",
    ]
    if risk:
        summary.append(
            "**优先复核**："
            + "、".join(
                f"{item.get('name', item.get('symbol', '未知'))}（{item['state']}）"
                for item in risk
            )
        )
    if report.get("data_complete") is True:
        summary.append(
            "**组合浮盈亏**："
            f"{_card_number(report.get('total_pnl_cny'), prefix='¥', signed=True)}"
            f"（{_card_number(report.get('total_pnl_pct'), signed=True, suffix='%')}）"
            f" · 市值 {_card_number(report.get('total_value_cny'), prefix='¥')}"
        )
    else:
        summary.append("**组合估值**：行情不完整，暂停汇总")
    thresholds = report.get("guidance_thresholds")
    if isinstance(thresholds, dict):
        summary.append(
            "**本窗判定档位**："
            f"{_card_number(thresholds.get('moderate_move_pct'), prefix='±', suffix='%')} / "
            f"{_card_number(thresholds.get('large_move_pct'), prefix='±', suffix='%')}"
        )
    snapshot_rows = []
    comparison_rows = []
    advice = ["**操作提示**"]
    for item in positions:
        name = str(item.get("name") or item.get("symbol") or "未知")
        symbol = str(item.get("symbol") or "")
        price = _card_price(item.get("latest_cny"), item.get("cost_cny"))
        snapshot_rows.append(
            {
                "name": name,
                "price": price,
                "daily": _card_colored_pct(item.get("day_pct")),
                "window": _card_colored_pct(item.get("window_pct")),
                "pnl": _card_colored_pct(item.get("pnl_pct")),
            }
        )
        comparison_rows.append(
            {
                "name": name,
                "relative": _card_number(item.get("relative_window_pct"), signed=True),
                "historical": _card_number(
                    item.get("historical_median_pct"), signed=True, suffix="%"
                ),
                "volume": _card_number(item.get("historical_volume_ratio"), suffix="倍"),
            }
        )
        state = str(item.get("state") or "状态未知")
        guidance = str(item.get("guidance") or "暂无建议")
        historical_note = (
            f"；历史{item.get('historical_days')}日截至{item.get('historical_latest_date')}"
            if item.get("historical_median_pct") is not None and item.get("historical_latest_date")
            else f"；历史：{item.get('history_status')}"
            if item.get("history_status") not in (None, "可比")
            else ""
        )
        quote_at = item.get("quote_at")
        quote_note = f"；报价{str(quote_at)[11:19]}" if quote_at else ""
        advice.append(
            f"**{name} {symbol} · {_card_state(state)}**：{guidance}{historical_note}{quote_note}"
        )
    advice.append(
        "*红/绿价格与盈亏＝高/低于成本；涨跌红/绿＝上涨/下跌。"
        "当日涨跌相对昨收；相对300单位为百分点；"
        "历史量比为腾讯实时与新浪分钟线的粗略对照。"
        "金额展示保留两位，计算仍用原始精度；不自动下单。*"
    )
    return [
        {"tag": "markdown", "content": "\n".join(summary)},
        {"tag": "markdown", "content": "**持仓价格快照**"},
        _card_table(
            [
                ("name", "标的", "text"),
                ("price", "现价", "markdown"),
                ("daily", "当日", "markdown"),
                ("window", window_label, "markdown"),
                ("pnl", "较成本", "markdown"),
            ],
            snapshot_rows,
        ),
        {"tag": "markdown", "content": "**相对与历史同窗**"},
        _card_table(
            [
                ("name", "标的", "text"),
                ("relative", "相对300(pp)", "text"),
                ("historical", "历史中位", "text"),
                ("volume", "量比", "text"),
            ],
            comparison_rows,
        ),
        {"tag": "markdown", "content": "\n".join(advice)[:4_000]},
    ]


def _card_table(columns: list[tuple[str, str, str]], rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "tag": "table",
        "page_size": min(max(len(rows), 1), 10),
        "row_height": "middle",
        "freeze_first_column": True,
        "header_style": {"bold": True, "background_style": "grey"},
        "columns": [
            {"name": key, "display_name": label, "data_type": data_type}
            for key, label, data_type in columns
        ],
        "rows": rows,
    }


def _card_decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return number if number.is_finite() else None


def _card_number(value: Any, *, prefix: str = "", suffix: str = "", signed: bool = False) -> str:
    number = _card_decimal(value)
    if number is None:
        return "—"
    rounded = number.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    formatted = f"{rounded:+,.2f}" if signed else f"{rounded:,.2f}"
    return f"{prefix}{formatted}{suffix}"


def _card_price(price: Any, cost: Any) -> str:
    number = _card_decimal(price)
    basis = _card_decimal(cost)
    if number is None:
        return "—"
    rendered = _card_number(number, prefix="¥")
    color = (
        "red"
        if basis is not None and number > basis
        else "green"
        if basis is not None and number < basis
        else None
    )
    return f"**<font color='{color}'>{rendered}</font>**" if color else f"**{rendered}**"


def _card_colored_pct(value: Any) -> str:
    number = _card_decimal(value)
    if number is None:
        return "—"
    rendered = _card_number(number, signed=True, suffix="%")
    color = "red" if number > 0 else "green" if number < 0 else None
    return f"**<font color='{color}'>{rendered}</font>**" if color else f"**{rendered}**"


def _card_state(state: str) -> str:
    color = (
        "red"
        if state == "风险升高"
        else "orange"
        if state in {"放量走弱", "短线偏弱", "行情缺失或过期", "收盘价未确认"}
        else "green"
        if state in {"放量走强", "短线走强", "温和走强"}
        else "blue"
    )
    return f"<font color='{color}'>{state}</font>"


def build_daily_acceptance_card(payload: dict[str, Any]) -> dict[str, Any]:
    """Render the durable post-close verdict as a compact phone card."""
    verdict = str(payload.get("verdict", "failed"))
    label = {"passed": "通过", "warning": "需关注", "failed": "失败"}.get(verdict, "失败")
    template = {"passed": "green", "warning": "orange", "failed": "red"}.get(verdict, "red")
    reasons = [str(item) for item in payload.get("failures", [])] + [
        str(item) for item in payload.get("warnings", [])
    ]
    lines = [
        f"**交易日**：{payload.get('trade_date', '未知')}",
        f"**验收结论**：{label}",
        f"**采集轮次**：{payload.get('collection_count', '未知')}",
        f"**有效行情率**：{payload.get('valid_quote_rate_pct', '无数据')}%",
        f"**完整轮次率**：{payload.get('successful_run_rate_pct', '无数据')}%",
        f"**P95 延迟**：{payload.get('p95_latency_ms', '无数据')} ms",
        f"**缺失槽位**：{payload.get('missing_slot_count', '未知')}，最大缺口 {payload.get('max_missing_gap_seconds', '未知')} 秒",
        f"**整日分钟数据**：{'完整' if payload.get('day_complete') else '不完整'}，{payload.get('day_row_count', '无数据')}/{payload.get('expected_day_row_count', '未知')} 行",
    ]
    lines.extend(f"- {reason}" for reason in reasons[:8])
    lines.append(f"报告：{payload.get('report_path', '未知')}")
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "title": {"tag": "plain_text", "content": "RegimeBeacon 每日运行验收"},
            "template": template,
        },
        "body": {"elements": [{"tag": "markdown", "content": "\n".join(lines)[:4_000]}]},
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
            "title": {"tag": "plain_text", "content": "RegimeBeacon 测试消息"},
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
            "title": {"tag": "plain_text", "content": "RegimeBeacon 行情采集状态"},
            "template": "green" if healthy else "orange",
        },
        "body": {"elements": [{"tag": "markdown", "content": normalized}]},
    }


def build_market_analysis_card(
    message: str, *, direction: str, report: dict[str, Any] | None = None
) -> dict[str, Any]:
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
    elements = (
        _market_table_elements(report)
        if isinstance(report, dict) and isinstance(report.get("temperature"), dict)
        else [{"tag": "markdown", "content": normalized}]
    )
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "title": {
                "tag": "plain_text",
                "content": "RegimeBeacon 市场温度与15分钟概览",
            },
            "template": template,
        },
        "body": {"elements": elements},
    }


def _market_table_elements(report: dict[str, Any]) -> list[dict[str, Any]]:
    temperature = report["temperature"]
    window = report.get("window_minutes", 15)
    observed_at = str(report.get("observed_at") or "")
    label = str(temperature.get("label") or "数据不足")
    color = (
        "red"
        if label == "风险收缩"
        else "orange"
        if label in {"偏冷", "过热"}
        else "green"
        if label == "偏暖"
        else "grey"
    )
    summary = [
        f"**{observed_at[:16].replace('T', ' ')} 市场速览** · 近{window}分钟",
        f"**市场温度**：**<font color='{color}'>{label}</font>**"
        f" · {_card_number(temperature.get('score'))}/100"
        f" · 信号一致性 {_card_number(temperature.get('confidence_pct'), suffix='%')}",
        f"**风险动作**：{temperature.get('posture') or '仅监控，等待数据'}",
        f"**市场状态**：{report.get('market_direction') or '数据不足'}"
        f" · **沪深300近{window}分**：{_card_colored_pct(report.get('market_window_return_pct'))}",
    ]
    daily = report.get("fixed_sample_daily_breadth") or {}
    current = report.get("fixed_sample_window_breadth") or {}
    breadth_rows = [
        {
            "period": "当日",
            "up": str(daily.get("up", "—")),
            "flat": str(daily.get("flat", "—")),
            "down": str(daily.get("down", "—")),
        },
        {
            "period": f"近{window}分",
            "up": str(current.get("up", "—")),
            "flat": str(current.get("flat", "—")),
            "down": str(current.get("down", "—")),
        },
    ]
    elements: list[dict[str, Any]] = [
        {"tag": "markdown", "content": "\n".join(summary)},
        {"tag": "markdown", "content": "**固定样本涨平跌**"},
        _card_table(
            [
                ("period", "口径", "text"),
                ("up", "涨", "text"),
                ("flat", "平", "text"),
                ("down", "跌", "text"),
            ],
            breadth_rows,
        ),
    ]
    benchmarks = report.get("benchmarks")
    if isinstance(benchmarks, (list, tuple)) and benchmarks:
        benchmark_rows = [
            {
                "name": str(item.get("name") or item.get("symbol") or "未知"),
                "daily": _card_colored_pct(item.get("daily_return_pct")),
                "window": _card_colored_pct(item.get("window_return_pct")),
            }
            for item in benchmarks
            if isinstance(item, dict)
        ]
        if benchmark_rows:
            elements.extend(
                [
                    {"tag": "markdown", "content": "**大盘基准**"},
                    _card_table(
                        [
                            ("name", "基准", "text"),
                            ("daily", "日内", "markdown"),
                            ("window", f"近{window}分", "markdown"),
                        ],
                        benchmark_rows,
                    ),
                ]
            )
    sector_rows = []
    seen: set[str] = set()
    for side, key in (("前列", "top_sectors"), ("后列", "bottom_sectors")):
        sectors = report.get(key)
        if not isinstance(sectors, (list, tuple)):
            continue
        for item in sectors[:3]:
            if not isinstance(item, dict):
                continue
            industry = str(item.get("industry") or "未知")
            if industry in seen:
                continue
            seen.add(industry)
            sector_rows.append(
                {
                    "name": f"{side} · {industry}",
                    "sample": _card_colored_pct(item.get("sample_return_pct")),
                    "etf": _card_colored_pct(item.get("etf_return_pct")),
                }
            )
    if sector_rows:
        elements.extend(
            [
                {"tag": "markdown", "content": "**行业排序与 ETF 对照**"},
                _card_table(
                    [
                        ("name", "板块", "text"),
                        ("sample", f"样本{window}分", "markdown"),
                        ("etf", f"ETF{window}分", "markdown"),
                    ],
                    sector_rows,
                ),
            ]
        )
    evidence = temperature.get("evidence")
    details = [
        f"**样本覆盖**：{current.get('available', '—')}/{current.get('expected', '—')}"
        f"（{_card_number(report.get('fixed_sample_coverage_pct'), suffix='%')}）"
        f" · 本窗样本均值 {_card_number(current.get('average_return_pct'), signed=True, suffix='%')}",
    ]
    if isinstance(evidence, (list, tuple)) and evidence:
        details.append("**温度依据**：" + "；".join(str(item) for item in evidence))
    warming = report.get("warming_sectors")
    cooling = report.get("cooling_sectors")
    if isinstance(warming, (list, tuple)) and warming:
        details.append(
            "**升温**："
            + "、".join(str(item.get("industry")) for item in warming if isinstance(item, dict))
        )
    if isinstance(cooling, (list, tuple)) and cooling:
        details.append(
            "**降温**："
            + "、".join(str(item.get("industry")) for item in cooling if isinstance(item, dict))
        )
    details.append(
        "*红/绿涨跌＝上涨/下跌；数值保留两位。固定样本不是全市场精确统计；"
        "温度分数与信号一致性不代表涨跌概率，开盘首个15分钟不构成独立买入信号。*"
    )
    elements.append({"tag": "markdown", "content": "\n".join(details)[:4_000]})
    return elements


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
