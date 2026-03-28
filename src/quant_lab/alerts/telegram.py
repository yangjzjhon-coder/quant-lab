from __future__ import annotations

import httpx

from quant_lab.config import AlertsConfig


def send_telegram_message(config: AlertsConfig, text: str) -> bool:
    if not config.telegram_enabled:
        return False
    if not config.telegram_bot_token or not config.telegram_chat_id:
        return False

    url = f"https://api.telegram.org/bot{config.telegram_bot_token}/sendMessage"
    response = httpx.post(
        url,
        json={
            "chat_id": config.telegram_chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        },
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    return bool(payload.get("ok"))
