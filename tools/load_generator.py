"""Générateur de charge : injecte des trades synthétiques dans crypto.prices.

Même format de message que producers/binance_producer.parse_trade (source = synthetic_load).
Sert à mesurer le débit de bout en bout (sink, Spark, API) avant d'optimiser.

    python tools/load_generator.py --rate 500 --duration 60
"""

import argparse
import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Dict, List

BASE_PRICES: Dict[str, float] = {"BTC": 65000.0, "ETH": 3400.0, "SOL": 150.0, "BNB": 580.0, "XRP": 0.55}


def next_price(price: float, rng: random.Random, volatility: float = 0.0005) -> float:
    """Marche aléatoire multiplicative ; le prix reste strictement positif."""
    return max(price * (1.0 + rng.gauss(0.0, volatility)), 1e-8)


def make_trade(symbol: str, price: float, trade_id: int, now_ms: int, rng: random.Random) -> dict:
    return {
        "event_type": "trade",
        "symbol": symbol,
        "pair": f"{symbol}USDT",
        "price": round(price, 8),
        "quantity": round(rng.uniform(0.0005, 2.0), 6),
        "trade_id": trade_id,
        "side": rng.choice(("buy", "sell")),
        "trade_time": now_ms,
        "trade_time_iso": datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "synthetic_load",
    }


def plan_ticks(rate: float):
    """(durée d'un tick en secondes, messages par tick) pour approcher `rate` msg/s."""
    tick = 0.05 if rate >= 20 else 1.0 / rate
    return tick, max(1, round(rate * tick))


def run(rate: float, duration: float, assets: List[str], bootstrap: str, topic: str, seed: int) -> dict:
    from kafka import KafkaProducer

    rng = random.Random(seed)
    prices = {a: BASE_PRICES.get(a, 100.0) for a in assets}
    trade_ids = {a: 1_000_000 * (i + 1) for i, a in enumerate(assets)}
    producer = KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=20,
        batch_size=65536,
        acks=1,
    )

    tick, per_tick = plan_ticks(rate)
    sent = 0
    start = time.time()
    next_tick = start
    while time.time() - start < duration:
        now_ms = int(time.time() * 1000)
        for _ in range(per_tick):
            symbol = rng.choice(assets)
            prices[symbol] = next_price(prices[symbol], rng)
            trade_ids[symbol] += 1
            producer.send(topic, key=symbol, value=make_trade(symbol, prices[symbol], trade_ids[symbol], now_ms, rng))
        sent += per_tick
        next_tick += tick
        pause = next_tick - time.time()
        if pause > 0:
            time.sleep(pause)

    producer.flush(timeout=30)
    producer.close()
    elapsed = time.time() - start
    return {"sent": sent, "seconds": round(elapsed, 2), "achieved_rate": round(sent / elapsed, 1)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rate", type=float, default=100, help="messages par seconde (défaut 100)")
    parser.add_argument("--duration", type=float, default=30, help="durée en secondes (défaut 30)")
    parser.add_argument("--assets", default=os.getenv("ASSETS", "BTC,ETH,SOL,BNB,XRP"))
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("TOPIC_PRICES", "crypto.prices"))
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    assets = [a.strip().upper() for a in args.assets.split(",") if a.strip()]
    result = run(args.rate, args.duration, assets, args.bootstrap, args.topic, args.seed)
    print(f"Envoyé {result['sent']} trades en {result['seconds']} s → {result['achieved_rate']} msg/s")


if __name__ == "__main__":
    main()
