# path: spark/redis_sink.py

"""
CryptoFlow — Sprint 3
Consumer Kafka crypto.prices → Redis

Cache le dernier prix par asset avec TTL (5 s par défaut).
Clés : crypto:last_price:{SYMBOL}
Valeur : JSON {price, quantity, side, trade_time_iso, ingested_at, source}

Les messages sont lus par lots (poll) : seul le dernier prix de chaque asset
est écrit, dans un pipeline Redis unique.
"""

import json
import logging
import os
import signal
import sys
from typing import Dict

import redis
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))   # racine du dépôt
from common.heartbeat import beat        # noqa: E402
from common.jsonutil import safe_json    # noqa: E402

load_dotenv(dotenv_path="config/.env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("redis-sink")

# ── Config ───────────────────────────────────────────────────────────────────

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
TOPIC         = os.getenv("TOPIC_PRICES", "crypto.prices")
REDIS_HOST    = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT    = int(os.getenv("REDIS_PORT", 6379))
REDIS_TTL     = int(os.getenv("REDIS_TTL_SECONDS", 5))

# ── Logique pure (testable) ──────────────────────────────────────────────────

def build_payload(msg: dict) -> dict:
    return {
        "price":          msg.get("price"),
        "quantity":       msg.get("quantity"),
        "side":           msg.get("side"),
        "trade_time_iso": msg.get("trade_time_iso"),
        "ingested_at":    msg.get("ingested_at"),
        "source":         msg.get("source"),
    }


def latest_by_symbol(batches) -> Dict[str, dict]:
    """Garde le dernier message de chaque symbole dans un résultat de consumer.poll()."""
    latest: Dict[str, dict] = {}
    for records in batches.values():
        for record in records:
            msg = record.value
            if isinstance(msg, dict) and msg.get("symbol"):
                latest[msg["symbol"]] = msg
    return latest

# ── Main loop ─────────────────────────────────────────────────────────────────

def run(shutdown_flag: list):
    from kafka import KafkaConsumer

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    log.info("Connected to Redis %s:%s", REDIS_HOST, REDIS_PORT)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="cryptoflow-redis-sink",
        auto_offset_reset="latest",   # last price uniquement → pas besoin de l'historique
        value_deserializer=safe_json,
        enable_auto_commit=True,
    )

    log.info("Subscribed to %s | TTL=%ss", TOPIC, REDIS_TTL)
    count = 0

    try:
        while not shutdown_flag[0]:
            beat()
            latest = latest_by_symbol(consumer.poll(timeout_ms=500, max_records=2000))
            if not latest:
                continue

            pipe = r.pipeline(transaction=False)
            for symbol, msg in latest.items():
                pipe.set(f"crypto:last_price:{symbol}", json.dumps(build_payload(msg)), ex=REDIS_TTL)
            pipe.execute()

            count += len(latest)
            if count % 1000 < len(latest):
                log.info("%d prices cached | last: %s", count, list(latest)[-1])

    except Exception:
        log.exception("Redis sink error")
        raise
    finally:
        consumer.close()
        log.info("Redis sink stopped. Total cached: %d", count)

# ── Entrypoint ────────────────────────────────────────────────────────────────

def main():
    shutdown_flag = [False]

    def handle_shutdown(sig, frame):
        log.info(f"Signal {sig} reçu, arrêt...")
        shutdown_flag[0] = True

    signal.signal(signal.SIGINT,  handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    run(shutdown_flag)

if __name__ == "__main__":
    main()
