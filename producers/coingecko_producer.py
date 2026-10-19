# path producers/coingecko_producer.py

"""
Producer CoinGecko API → Kafka topic: crypto.history

Fetches OHLCV (Open/High/Low/Close/Volume) historical data
for configured assets every COINGECKO_FETCH_INTERVAL_SEC seconds.
Uses the free CoinGecko API — no API key required.
"""

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv(dotenv_path="config/.env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("coingecko-producer")

# ── Configuration ────────────────────────────────────────────────────────────

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
TOPIC = os.getenv("TOPIC_HISTORY", "crypto.history")
BASE_URL = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
INTERVAL_SEC = int(os.getenv("COINGECKO_FETCH_INTERVAL_SEC", 60))

# CoinGecko coin IDs (different from Binance symbols)
COIN_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "BNB": "binancecoin",
    "XRP": "ripple",
}

ASSETS_RAW = os.getenv("ASSETS", "BTC,ETH,SOL,BNB,XRP")
ASSETS = [a.strip().upper() for a in ASSETS_RAW.split(",")]

# ── Kafka Producer ───────────────────────────────────────────────────────────

def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks=os.getenv("KAFKA_ACKS", "all"),
        retries=int(os.getenv("KAFKA_RETRIES", 5)),
        compression_type="snappy",
    )

# ── CoinGecko API calls ──────────────────────────────────────────────────────

def fetch_current_price(coin_id: str) -> dict | None:
    """Fetch current price + market data for one coin."""
    url = f"{BASE_URL}/coins/{coin_id}"
    params = {
        "localization": "false",
        "tickers": "false",
        "community_data": "false",
        "developer_data": "false",
        "sparkline": "false",
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        market = data.get("market_data", {})

        return {
            "event_type": "market_snapshot",
            "coin_id": coin_id,
            "symbol": data.get("symbol", "").upper(),
            "name": data.get("name"),
            "price_usd": market.get("current_price", {}).get("usd"),
            "price_btc": market.get("current_price", {}).get("btc"),
            "market_cap_usd": market.get("market_cap", {}).get("usd"),
            "volume_24h_usd": market.get("total_volume", {}).get("usd"),
            "price_change_24h_pct": market.get("price_change_percentage_24h"),
            "price_change_7d_pct": market.get("price_change_percentage_7d"),
            "ath_usd": market.get("ath", {}).get("usd"),
            "atl_usd": market.get("atl", {}).get("usd"),
            "circulating_supply": market.get("circulating_supply"),
            "total_supply": market.get("total_supply"),
            "last_updated": market.get("last_updated"),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "source": "coingecko_api",
        }
    except requests.RequestException as e:
        log.error(f"Failed to fetch {coin_id} from CoinGecko: {e}")
        return None


def fetch_ohlcv(coin_id: str, days: int = 1) -> list[dict]:
    """
    Fetch OHLCV candles for the past N days.
    CoinGecko free tier: auto granularity
      - 1 day  → 5-minute intervals
      - 7-90 days → hourly
      - >90 days  → daily
    """
    url = f"{BASE_URL}/coins/{coin_id}/ohlc"
    params = {"vs_currency": "usd", "days": days}
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        raw = resp.json()  # [[timestamp_ms, open, high, low, close], ...]

        symbol = next(
            (k for k, v in COIN_IDS.items() if v == coin_id), coin_id.upper()
        )
        candles = []
        for row in raw:
            ts_ms, open_, high, low, close = row
            candles.append({
                "event_type": "ohlcv",
                "coin_id": coin_id,
                "symbol": symbol,
                "timestamp_ms": ts_ms,
                "timestamp_iso": datetime.fromtimestamp(
                    ts_ms / 1000, tz=timezone.utc
                ).isoformat(),
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "days_window": days,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "source": "coingecko_ohlcv",
            })
        return candles
    except requests.RequestException as e:
        log.error(f"Failed to fetch OHLCV for {coin_id}: {e}")
        return []

# ── Main loop ────────────────────────────────────────────────────────────────

def run(producer: KafkaProducer, shutdown_flag: list):
    log.info(f"Starting CoinGecko producer | assets={ASSETS} | interval={INTERVAL_SEC}s")

    while not shutdown_flag[0]:
        batch_start = time.time()
        published = 0

        for symbol in ASSETS:
            coin_id = COIN_IDS.get(symbol)
            if not coin_id:
                log.warning(f"No CoinGecko ID for symbol {symbol}, skipping.")
                continue

            # Market snapshot
            snapshot = fetch_current_price(coin_id)
            if snapshot:
                producer.send(TOPIC, key=symbol, value=snapshot)
                published += 1
                log.debug(f"Published snapshot for {symbol}: ${snapshot['price_usd']}")

            # OHLCV last 24h
            candles = fetch_ohlcv(coin_id, days=1)
            for candle in candles:
                producer.send(TOPIC, key=symbol, value=candle)
                published += 1

            # CoinGecko rate limit: 30 calls/min on free tier → sleep between coins
            time.sleep(2.5)

            if shutdown_flag[0]:
                break

        producer.flush()
        elapsed = time.time() - batch_start
        log.info(f"Batch complete — published {published} messages in {elapsed:.1f}s")

        # Sleep until next interval
        sleep_time = max(0, INTERVAL_SEC - elapsed)
        log.info(f"Next fetch in {sleep_time:.0f}s...")
        for _ in range(int(sleep_time)):
            if shutdown_flag[0]:
                break
            time.sleep(1)

# ── Entrypoint ───────────────────────────────────────────────────────────────

def main():
    shutdown_flag = [False]

    def handle_shutdown(sig, frame):
        log.info(f"Received signal {sig}, shutting down...")
        shutdown_flag[0] = True

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    producer = build_producer()
    try:
        run(producer, shutdown_flag)
    finally:
        log.info("Flushing Kafka producer...")
        producer.flush(timeout=10)
        producer.close()
        log.info("CoinGecko producer stopped.")

if __name__ == "__main__":
    main()