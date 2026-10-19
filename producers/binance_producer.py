# path producers/binance_producer.py

"""
Producer Binance WebSocket → Kafka topic: crypto.prices

Streams real-time trade data from Binance for multiple assets.
Each message published to Kafka contains: symbol, price, volume,
trade_id, timestamp, and side (buy/sell).
"""

import asyncio
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

import websockets
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv(dotenv_path="config/.env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("binance-producer")

# ── Configuration ────────────────────────────────────────────────────────────

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
TOPIC = os.getenv("TOPIC_PRICES", "crypto.prices")
ASSETS_RAW = os.getenv("ASSETS", "BTC,ETH,SOL,BNB,XRP")
ASSETS = [a.strip().upper() for a in ASSETS_RAW.split(",")]
BINANCE_WS_URL = os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443/stream")

# Binance symbols format: btcusdt, ethusdt, ...
STREAMS = [f"{asset.lower()}usdt@trade" for asset in ASSETS]
WS_URL = f"{BINANCE_WS_URL}?streams={'/'.join(STREAMS)}"

# ── Kafka Producer ───────────────────────────────────────────────────────────

def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks=os.getenv("KAFKA_ACKS", "all"),
        retries=int(os.getenv("KAFKA_RETRIES", 5)),
        linger_ms=int(os.getenv("KAFKA_LINGER_MS", 5)),
        batch_size=int(os.getenv("KAFKA_BATCH_SIZE", 16384)),
        compression_type="snappy",
    )

# ── Message parsing ──────────────────────────────────────────────────────────

def parse_trade(raw: dict) -> dict:
    """
    Binance trade event schema:
    {
      "e": "trade",   event type
      "E": 123456789, event time (ms)
      "s": "BTCUSDT", symbol
      "t": 12345,     trade ID
      "p": "0.001",   price
      "q": "100",     quantity
      "m": true,      is buyer the market maker (sell = true, buy = false)
      "T": 123456785  trade time (ms)
    }
    """
    data = raw.get("data", raw)
    symbol = data["s"].replace("USDT", "")
    trade_time_ms = data["T"]

    return {
        "event_type": "trade",
        "symbol": symbol,
        "pair": data["s"],
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "trade_id": data["t"],
        "side": "sell" if data["m"] else "buy",
        "trade_time": trade_time_ms,
        "trade_time_iso": datetime.fromtimestamp(
            trade_time_ms / 1000, tz=timezone.utc
        ).isoformat(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "binance_ws",
    }

# ── WebSocket consumer loop ──────────────────────────────────────────────────

async def consume(producer: KafkaProducer, shutdown_event: asyncio.Event):
    reconnect_delay = 1
    msg_count = 0
    last_log = time.time()

    while not shutdown_event.is_set():
        try:
            log.info(f"Connecting to Binance WebSocket — streams: {STREAMS}")
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                reconnect_delay = 1  # reset on successful connect
                log.info("Connected to Binance WebSocket ✓")

                async for raw_msg in ws:
                    if shutdown_event.is_set():
                        break

                    try:
                        raw = json.loads(raw_msg)
                        msg = parse_trade(raw)
                        key = msg["symbol"]

                        future = producer.send(TOPIC, key=key, value=msg)
                        future.add_errback(
                            lambda e: log.error(f"Kafka send error: {e}")
                        )

                        msg_count += 1

                        # Log throughput every 30s
                        now = time.time()
                        if now - last_log >= 30:
                            log.info(
                                f"Throughput: {msg_count} messages in last 30s "
                                f"({msg_count / 30:.1f} msg/s)"
                            )
                            msg_count = 0
                            last_log = now

                    except (KeyError, ValueError, json.JSONDecodeError) as e:
                        log.warning(f"Failed to parse message: {e} — raw: {raw_msg[:200]}")

        except websockets.exceptions.ConnectionClosed as e:
            log.warning(f"WebSocket closed: {e}. Reconnecting in {reconnect_delay}s...")
        except Exception as e:
            log.error(f"Unexpected error: {e}. Reconnecting in {reconnect_delay}s...")

        if not shutdown_event.is_set():
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)  # exponential backoff, max 60s

# ── Entrypoint ───────────────────────────────────────────────────────────────

async def main():
    shutdown_event = asyncio.Event()

    def handle_shutdown(sig, frame):
        log.info(f"Received signal {sig}, shutting down...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    log.info(f"Starting Binance producer | assets={ASSETS} | topic={TOPIC}")

    producer = build_producer()
    try:
        await consume(producer, shutdown_event)
    finally:
        log.info("Flushing Kafka producer...")
        producer.flush(timeout=10)
        producer.close()
        log.info("Binance producer stopped.")

if __name__ == "__main__":
    asyncio.run(main())