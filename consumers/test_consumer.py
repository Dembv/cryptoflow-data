# path consumers/test_consumer.py

"""
Consumer de test — vérifie que les 3 topics reçoivent bien des messages.

Usage:
  python consumers/test_consumer.py --topic crypto.prices
  python consumers/test_consumer.py --topic crypto.history
  python consumers/test_consumer.py --topic crypto.onchain
  python consumers/test_consumer.py --all
"""

import argparse
import json
import logging
import os
import signal
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv(dotenv_path="config/.env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("test-consumer")

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

TOPICS = {
    "prices":  os.getenv("TOPIC_PRICES",  "crypto.prices"),
    "history": os.getenv("TOPIC_HISTORY", "crypto.history"),
    "onchain": os.getenv("TOPIC_ONCHAIN", "crypto.onchain"),
}

# ── Pretty-print helpers ─────────────────────────────────────────────────────

def fmt_prices(msg: dict) -> str:
    return (
        f"[{msg.get('symbol'):>5}] "
        f"${msg.get('price'):>12,.4f} "
        f"qty={msg.get('quantity'):>12.6f} "
        f"side={msg.get('side')}"
    )

def fmt_history(msg: dict) -> str:
    et = msg.get("event_type", "?")
    if et == "ohlcv":
        return (
            f"[{msg.get('symbol'):>5}] OHLCV "
            f"O={msg.get('open'):>10,.2f} "
            f"H={msg.get('high'):>10,.2f} "
            f"L={msg.get('low'):>10,.2f} "
            f"C={msg.get('close'):>10,.2f}"
        )
    return (
        f"[{msg.get('symbol'):>5}] snapshot "
        f"${msg.get('price_usd'):>12,.2f} "
        f"24h={msg.get('price_change_24h_pct', 0):+.2f}%"
    )

def fmt_onchain(msg: dict) -> str:
    et = msg.get("event_type", "?")
    if et == "eth_transaction":
        return (
            f"[ETH whale] "
            f"{msg.get('value_eth'):>10,.2f} ETH "
            f"{msg.get('from_address', '')[:8]}… → "
            f"{msg.get('to_address', '')[:8]}…"
        )
    return (
        f"[{msg.get('symbol'):>6} transfer] "
        f"{msg.get('value'):>16,.2f} "
        f"block={msg.get('block_number')}"
    )

FORMATTERS = {
    "crypto.prices":  fmt_prices,
    "crypto.history": fmt_history,
    "crypto.onchain": fmt_onchain,
}

# ── Consumer ─────────────────────────────────────────────────────────────────

def consume(topics: list[str]):
    log.info(f"Subscribing to topics: {topics}")

    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="cryptoflow-test-consumer",
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=60_000,  # 60s inactivity timeout
        enable_auto_commit=True,
    )

    shutdown = [False]

    def handle_shutdown(sig, frame):
        log.info("Shutting down consumer...")
        shutdown[0] = True

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    stats: dict[str, int] = {t: 0 for t in topics}

    try:
        for record in consumer:
            if shutdown[0]:
                break

            topic = record.topic
            msg = record.value
            stats[topic] = stats.get(topic, 0) + 1

            formatter = FORMATTERS.get(topic, lambda m: json.dumps(m, indent=2))
            timestamp = datetime.utcnow().strftime("%H:%M:%S")

            print(
                f"  {timestamp} "
                f"[{topic}] "
                f"part={record.partition} "
                f"offset={record.offset:>8} | "
                f"{formatter(msg)}"
            )

    except Exception as e:
        log.error(f"Consumer error: {e}")
    finally:
        consumer.close()
        print("\n── Stats ──────────────────────────────────────────")
        for t, count in stats.items():
            print(f"  {t}: {count} messages received")

# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="CryptoFlow test consumer")
    parser.add_argument("--topic", choices=list(TOPICS.values()), help="Single topic to consume")
    parser.add_argument("--all", action="store_true", help="Subscribe to all 3 topics")
    args = parser.parse_args()

    if args.all:
        topics = list(TOPICS.values())
    elif args.topic:
        topics = [args.topic]
    else:
        parser.print_help()
        return

    consume(topics)

if __name__ == "__main__":
    main()