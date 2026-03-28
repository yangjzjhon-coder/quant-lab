from __future__ import annotations

import smtplib
from email.message import EmailMessage

from quant_lab.config import AlertsConfig


def send_email_message(config: AlertsConfig, subject: str, text: str) -> bool:
    if not config.email_enabled:
        return False
    if not config.email_from or not config.email_to or not config.smtp_host:
        return False
    if config.smtp_username and not config.smtp_password:
        return False

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = config.email_from
    message["To"] = ", ".join(config.email_to)
    message.set_content(text)

    smtp_class = smtplib.SMTP_SSL if config.smtp_use_ssl else smtplib.SMTP
    with smtp_class(config.smtp_host, config.smtp_port, timeout=config.smtp_timeout_seconds) as client:
        if not config.smtp_use_ssl and config.smtp_use_tls:
            client.starttls()
        if config.smtp_username:
            client.login(config.smtp_username, config.smtp_password or "")
        client.send_message(message)
    return True
