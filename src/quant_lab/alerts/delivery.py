from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from quant_lab.alerts.email import send_email_message
from quant_lab.alerts.telegram import send_telegram_message
from quant_lab.config import AlertsConfig


@dataclass(frozen=True)
class AlertDeliveryResult:
    channel: str
    status: str
    delivered: bool
    error: str | None = None
    delivered_at: datetime | None = None


def deliver_alerts(
    config: AlertsConfig,
    *,
    title: str,
    message: str,
    telegram_message: str | None = None,
    email_subject: str | None = None,
    email_message: str | None = None,
) -> list[AlertDeliveryResult]:
    results: list[AlertDeliveryResult] = []

    if config.telegram_enabled:
        results.append(
            _attempt_delivery(
                channel="telegram",
                send=lambda: send_telegram_message(config, telegram_message or message),
            )
        )

    if config.email_enabled:
        subject = email_subject or _email_subject(config, title)
        results.append(
            _attempt_delivery(
                channel="email",
                send=lambda: send_email_message(config, subject, email_message or message),
            )
        )

    if results:
        return results

    return [
        AlertDeliveryResult(
            channel="disabled",
            status="skipped",
            delivered=False,
            error="No alert channels are enabled.",
        )
    ]


def _attempt_delivery(channel: str, send) -> AlertDeliveryResult:
    try:
        delivered = bool(send())
    except Exception as exc:
        return AlertDeliveryResult(channel=channel, status="error", delivered=False, error=str(exc))

    if delivered:
        return AlertDeliveryResult(
            channel=channel,
            status="sent",
            delivered=True,
            delivered_at=datetime.now(timezone.utc),
        )
    return AlertDeliveryResult(
        channel=channel,
        status="skipped",
        delivered=False,
        error="Channel enabled but credentials or connection settings are incomplete.",
    )


def _email_subject(config: AlertsConfig, title: str) -> str:
    prefix = (config.email_subject_prefix or "").strip()
    return f"{prefix} {title}".strip()
