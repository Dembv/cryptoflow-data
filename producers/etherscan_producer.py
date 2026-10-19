# path producers/etherscan_producer.py

"""
Producer Etherscan API → Kafka topic: crypto.onchain

Tracks large ETH transactions (whale movements) and ERC-20
token transfers for WBTC, USDT, USDC every ETHERSCAN_FETCH_INTERVAL_SEC seconds.
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

from urllib.parse import urlparse, parse_qs


load_dotenv(dotenv_path="config/.env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("etherscan-producer")

# ── Configuration ────────────────────────────────────────────────────────────

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
TOPIC = os.getenv("TOPIC_ONCHAIN", "crypto.onchain")
API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
INTERVAL_SEC = int(os.getenv("ETHERSCAN_FETCH_INTERVAL_SEC", 30))

_raw_url    = os.getenv("ETHERSCAN_BASE_URL", "https://api.etherscan.io/api")
_parsed     = urlparse(_raw_url)
BASE_URL    = f"{_parsed.scheme}://{_parsed.netloc}{_parsed.path}"
BASE_PARAMS = {k: v[0] for k, v in parse_qs(_parsed.query).items()}

# Whale threshold: 100 ETH minimum
WHALE_THRESHOLD_ETH = 100.0

# ERC-20 token contracts to monitor
TOKEN_CONTRACTS = {
    "WBTC": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
    "USDT": "0xdac17f958d2ee523a2206206994597c13d831ec7",
    "USDC": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
}

# Block tracking (keep last processed block in memory)
_last_block: dict[str, int] = {}

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

# ── Etherscan API calls ──────────────────────────────────────────────────────

def get_latest_block() -> int:
    """Return the latest confirmed Ethereum block number."""
    params = {
        "module": "proxy",
        "action": "eth_blockNumber",
        "apikey": API_KEY,
    }
    try:
        resp = requests.get(BASE_URL, params={**BASE_PARAMS, **params}, timeout=10)
        resp.raise_for_status()
        return int(resp.json()["result"], 16)
    except Exception as e:
        log.error(f"Failed to get latest block: {e}")
        return 0


def fetch_eth_transactions(start_block: int, end_block: int) -> list[dict]:
    """
    Fetch normal ETH transactions from the last N blocks.
    Filter for whale transactions (>= WHALE_THRESHOLD_ETH).
    """
    params = {
        "module": "account",
        "action": "txlist",
        "address": "0x0000000000000000000000000000000000000000",  # Placeholder
        "startblock": start_block,
        "endblock": end_block,
        "sort": "desc",
        "apikey": API_KEY,
    }

    # In practice, you'd iterate over addresses of interest.
    # Here we use the "beaconchain" approach: fetch top transfers by value
    # via eth_getLogs — simplified version below:
    url = f"{BASE_URL}"
    params_top = {
        "module": "account",
        "action": "txlistinternal",
        "startblock": start_block,
        "endblock": end_block,
        "sort": "desc",
        "apikey": API_KEY,
    }

    try:
        resp = requests.get(BASE_URL, params={**BASE_PARAMS, **params}, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != "1":
            return []

        txns = []
        for tx in data.get("result", [])[:50]:  # cap at 50 per batch
            try:
                value_eth = int(tx.get("value", "0")) / 1e18
                if value_eth < WHALE_THRESHOLD_ETH:
                    continue

                txns.append({
                    "event_type": "eth_transaction",
                    "symbol": "ETH",
                    "tx_hash": tx.get("hash"),
                    "block_number": int(tx.get("blockNumber", 0)),
                    "timestamp_unix": int(tx.get("timeStamp", 0)),
                    "timestamp_iso": datetime.fromtimestamp(
                        int(tx.get("timeStamp", 0)), tz=timezone.utc
                    ).isoformat(),
                    "from_address": tx.get("from"),
                    "to_address": tx.get("to"),
                    "value_eth": value_eth,
                    "value_usd": None,  # enriched downstream by dbt
                    "gas_used": int(tx.get("gasUsed", 0)),
                    "gas_price_gwei": int(tx.get("gasPrice", 0)) / 1e9,
                    "is_whale": True,
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "source": "etherscan_api",
                })
            except (ValueError, KeyError):
                continue
        return txns
    except requests.RequestException as e:
        log.error(f"Failed to fetch ETH transactions: {e}")
        return []


def fetch_token_transfers(token_symbol: str, contract: str,
                          start_block: int, end_block: int) -> list[dict]:
    """Fetch ERC-20 token transfer events from Etherscan."""
    params = {
        "module": "account",
        "action": "tokentx",
        "contractaddress": contract,
        "startblock": start_block,
        "endblock": end_block,
        "sort": "desc",
        "apikey": API_KEY,
    }
    try:
        resp = requests.get(BASE_URL, params={**BASE_PARAMS, **params}, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != "1":
            return []

        transfers = []
        for tx in data.get("result", [])[:30]:
            try:
                decimals = int(tx.get("tokenDecimal", 18))
                value = int(tx.get("value", 0)) / (10 ** decimals)

                transfers.append({
                    "event_type": "token_transfer",
                    "symbol": token_symbol,
                    "contract": contract,
                    "tx_hash": tx.get("hash"),
                    "block_number": int(tx.get("blockNumber", 0)),
                    "timestamp_unix": int(tx.get("timeStamp", 0)),
                    "timestamp_iso": datetime.fromtimestamp(
                        int(tx.get("timeStamp", 0)), tz=timezone.utc
                    ).isoformat(),
                    "from_address": tx.get("from"),
                    "to_address": tx.get("to"),
                    "value": value,
                    "token_name": tx.get("tokenName"),
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "source": "etherscan_api",
                })
            except (ValueError, KeyError):
                continue
        return transfers
    except requests.RequestException as e:
        log.error(f"Failed to fetch {token_symbol} transfers: {e}")
        return []

# ── Main loop ────────────────────────────────────────────────────────────────

def run(producer: KafkaProducer, shutdown_flag: list):
    log.info(f"Starting Etherscan producer | interval={INTERVAL_SEC}s")

    while not shutdown_flag[0]:
        batch_start = time.time()
        published = 0

        latest_block = get_latest_block()
        if latest_block == 0:
            log.warning("Could not fetch latest block, retrying...")
            time.sleep(INTERVAL_SEC)
            continue

        # Determine block range (last ~30 blocks ≈ 6 min)
        last = _last_block.get("eth", latest_block - 30)
        start_block = last + 1
        end_block = latest_block

        if start_block > end_block:
            log.info("No new blocks to process.")
        else:
            log.info(f"Processing blocks {start_block} → {end_block}")

            # ETH whale transactions
            eth_txns = fetch_eth_transactions(start_block, end_block)
            for tx in eth_txns:
                producer.send(TOPIC, key="ETH", value=tx)
                published += 1

            log.info(f"ETH whale txns: {len(eth_txns)}")

            # ERC-20 token transfers
            for symbol, contract in TOKEN_CONTRACTS.items():
                if shutdown_flag[0]:
                    break
                transfers = fetch_token_transfers(symbol, contract, start_block, end_block)
                for t in transfers:
                    producer.send(TOPIC, key=symbol, value=t)
                    published += 1
                log.info(f"{symbol} transfers: {len(transfers)}")
                time.sleep(0.3)  # Etherscan rate limit

            _last_block["eth"] = end_block

        producer.flush()
        elapsed = time.time() - batch_start
        log.info(f"Batch complete — published {published} messages in {elapsed:.1f}s")

        sleep_time = max(0, INTERVAL_SEC - elapsed)
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

    if not API_KEY:
        log.warning("ETHERSCAN_API_KEY not set — requests will be rate-limited (1 req/5s)")

    producer = build_producer()
    try:
        run(producer, shutdown_flag)
    finally:
        producer.flush(timeout=10)
        producer.close()
        log.info("Etherscan producer stopped.")

if __name__ == "__main__":
    main()