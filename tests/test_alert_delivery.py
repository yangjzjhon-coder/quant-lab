from __future__ import annotations

from quant_lab.alerts.delivery import deliver_alerts
from quant_lab.config import AlertsConfig


def test_deliver_alerts_returns_disabled_result_when_no_channels_enabled() -> None:
    results = deliver_alerts(AlertsConfig(), title="Test", message="Hello")

    assert len(results) == 1
    assert results[0].channel == "disabled"
    assert results[0].status == "skipped"
    assert results[0].delivered is False


def test_deliver_alerts_dispatches_to_enabled_channels(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(
        "quant_lab.alerts.delivery.send_telegram_message",
        lambda config, text: calls.append(("telegram", text)) or True,
    )
    monkeypatch.setattr(
        "quant_lab.alerts.delivery.send_email_message",
        lambda config, subject, text: calls.append(("email", f"{subject}|{text}")) or True,
    )

    config = AlertsConfig(
        telegram_enabled=True,
        telegram_bot_token="bot",
        telegram_chat_id="chat",
        email_enabled=True,
        email_from="bot@example.com",
        email_to=["desk@example.com"],
        smtp_host="smtp.example.com",
    )

    results = deliver_alerts(config, title="Ping", message="runtime ok")

    assert [result.channel for result in results] == ["telegram", "email"]
    assert all(result.status == "sent" for result in results)
    assert calls[0] == ("telegram", "runtime ok")
    assert calls[1][0] == "email"
    assert "Ping" in calls[1][1]
