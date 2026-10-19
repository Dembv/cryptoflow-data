# path: airflow/plugins/slack_alert.py

"""
Plugin Airflow — Alertes Slack centralisées.

Fournit des callbacks réutilisables pour on_failure_callback
et on_success_callback dans tous les DAGs CryptoFlow.
"""

import os
import logging
from typing import Optional

import requests

log = logging.getLogger(__name__)

SLACK_WEBHOOK = os.getenv("CRYPTOFLOW_SLACK_WEBHOOK", "")

EMOJI_MAP = {
    "failure": ":x:",
    "success": ":white_check_mark:",
    "warning": ":warning:",
    "info":    ":information_source:",
}


def _post(payload: dict) -> bool:
    if not SLACK_WEBHOOK:
        log.debug("SLACK_WEBHOOK not set, skipping notification.")
        return False
    try:
        resp = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        log.error(f"Slack notification failed: {e}")
        return False


def on_failure_callback(context: dict):
    """Callback sur échec — compatible Airflow on_failure_callback."""
    dag_id   = context["dag"].dag_id
    task_id  = context["task_instance"].task_id
    log_url  = context["task_instance"].log_url
    exc      = context.get("exception", "")

    _post({
        "attachments": [{
            "color": "danger",
            "title": f"{EMOJI_MAP['failure']} CryptoFlow Task Failed",
            "fields": [
                {"title": "DAG",   "value": dag_id,  "short": True},
                {"title": "Task",  "value": task_id, "short": True},
                {"title": "Error", "value": str(exc)[:300]},
            ],
            "actions": [{"type": "button", "text": "View Logs", "url": log_url}],
        }]
    })


def on_success_callback(context: dict):
    """Callback sur succès DAG complet."""
    dag_id    = context["dag"].dag_id
    exec_date = context["execution_date"].strftime("%Y-%m-%d %H:%M UTC")

    _post({
        "text": (
            f"{EMOJI_MAP['success']} *{dag_id}* completed successfully "
            f"at `{exec_date}`"
        )
    })


def send_data_quality_alert(
    model_name: str,
    violations: int,
    details: Optional[str] = None,
):
    """Alerte manuelle sur échec de test dbt."""
    _post({
        "attachments": [{
            "color":  "warning",
            "title":  f"{EMOJI_MAP['warning']} dbt Test Failure — {model_name}",
            "fields": [
                {"title": "Violations", "value": str(violations), "short": True},
                {"title": "Details",    "value": details or "N/A"},
            ],
        }]
    })