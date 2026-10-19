"""Service d'alertes : lit crypto.indicators et crypto.onchain, notifie Slack / Telegram.

Lancement : python -m alerts.service
"""

import logging
import os
import signal
from typing import List, Optional

from alerts.notifiers import build_notifiers, dispatch
from alerts.rules import Alert, AlertConfig, Cooldown, evaluate
from common.heartbeat import beat
from common.jsonutil import safe_json

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
log = logging.getLogger("alert-service")

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
TOPICS = [
    os.getenv("TOPIC_INDICATORS", "crypto.indicators"),
    os.getenv("TOPIC_ONCHAIN", "crypto.onchain"),
]


def process(topic: str, msg, cfg: AlertConfig, cooldown: Cooldown, notifiers: List,
            now: Optional[float] = None) -> List[Alert]:
    """Évalue un message, applique le cooldown et notifie. Retourne les alertes émises."""
    emitted: List[Alert] = []
    for alert in evaluate(topic, msg, cfg):
        if cooldown.allow(alert.key, now):
            log.info("ALERTE %s %s : %s", alert.rule, alert.symbol, alert.text)
            dispatch(notifiers, alert)
            emitted.append(alert)
    return emitted


def run(shutdown_flag: list) -> None:
    from kafka import KafkaConsumer

    cfg = AlertConfig.from_env()
    cooldown = Cooldown(cfg.cooldown_sec)
    notifiers = build_notifiers()
    log.info("Règles : %s | canaux : %s", cfg, [n.name for n in notifiers] or "logs uniquement")

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="cryptoflow-alerts",
        auto_offset_reset="latest",          # pas d'alertes sur l'historique
        enable_auto_commit=True,
        value_deserializer=safe_json,
    )
    try:
        while not shutdown_flag[0]:
            beat()
            for records in consumer.poll(timeout_ms=1000, max_records=500).values():
                for record in records:
                    process(record.topic, record.value, cfg, cooldown, notifiers)
    finally:
        consumer.close()
        log.info("Alert service stopped.")


def main() -> None:
    shutdown_flag = [False]

    def handle_shutdown(sig, frame):
        shutdown_flag[0] = True

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    run(shutdown_flag)


if __name__ == "__main__":
    main()
