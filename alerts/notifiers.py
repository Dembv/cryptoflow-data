"""Canaux de notification : Slack (webhook) et Telegram (bot)."""

import logging
import os
from typing import List, Mapping

import requests

from alerts.rules import Alert

log = logging.getLogger("alerts.notifiers")

ICONS = {"info": "[INFO]", "warning": "[ATTENTION]", "critical": "[CRITIQUE]"}


def format_alert(alert: Alert) -> str:
    return f"{ICONS.get(alert.severity, '[INFO]')} {alert.symbol} — {alert.text}"


class SlackNotifier:
    name = "slack"

    def __init__(self, webhook: str):
        self.webhook = webhook

    def send(self, alert: Alert) -> None:
        requests.post(self.webhook, json={"text": format_alert(alert)}, timeout=10).raise_for_status()


class TelegramNotifier:
    name = "telegram"

    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id

    def send(self, alert: Alert) -> None:
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        requests.post(url, json={"chat_id": self.chat_id, "text": format_alert(alert)}, timeout=10).raise_for_status()


def build_notifiers(env: Mapping[str, str] = os.environ) -> List:
    notifiers: List = []
    webhook = env.get("ALERT_SLACK_WEBHOOK") or env.get("SLACK_WEBHOOK_URL") or env.get("CRYPTOFLOW_SLACK_WEBHOOK")
    if webhook:
        notifiers.append(SlackNotifier(webhook))
    if env.get("TELEGRAM_BOT_TOKEN") and env.get("TELEGRAM_CHAT_ID"):
        notifiers.append(TelegramNotifier(env["TELEGRAM_BOT_TOKEN"], env["TELEGRAM_CHAT_ID"]))
    return notifiers


def dispatch(notifiers: List, alert: Alert) -> int:
    """Envoie l'alerte sur chaque canal ; retourne le nombre d'envois réussis."""
    sent = 0
    for notifier in notifiers:
        try:
            notifier.send(alert)
            sent += 1
        except Exception as exc:
            # on ne logue que le type d'erreur : l'URL (webhook, token bot) ne doit pas fuiter dans les logs
            log.error("Notification %s échouée (%s)", notifier.name, type(exc).__name__)
    return sent
