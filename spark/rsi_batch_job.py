# path spark/rsi_batch_job.py

"""
CryptoFlow — Sprint 2
Job batch RSI précis depuis TimescaleDB

Le RSI dans le job Spark Streaming est une approximation
basée sur les stats d'une fenêtre (open/close/avg).

Ce job calcule le RSI précis (méthode Wilder, 14 périodes)
en récupérant les 30 derniers prix depuis TimescaleDB,
puis publie les résultats dans crypto.indicators.

Exécuté toutes les 5 minutes par Airflow (Sprint 4).
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaProducer

# Import des indicateurs depuis Sprint 2
from indicators import rsi, bollinger_bands, vwap

load_dotenv(dotenv_path="config/.env")

log = logging.getLogger("rsi-batch-job")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ── Config ───────────────────────────────────────────────────────────────────

KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
TOPIC_OUT      = os.getenv("TOPIC_INDICATORS", "crypto.indicators")
TIMESCALE_DSN  = os.getenv(
    "TIMESCALE_DSN",
    "postgresql://crypto:crypto@localhost:5433/cryptoflow"
)
ASSETS_RAW     = os.getenv("ASSETS", "BTC,ETH,SOL,BNB,XRP")
ASSETS         = [a.strip().upper() for a in ASSETS_RAW.split(",")]
RSI_PERIOD     = 14
LOOKBACK_ROWS  = RSI_PERIOD + 20  # 34 prix pour un RSI(14) stable

# ── Kafka Producer ────────────────────────────────────────────────────────────

def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

# ── TimescaleDB queries ──────────────────────────────────────────────────────

def fetch_recent_closes(conn, symbol: str, limit: int) -> list[float]:
    """
    Récupère les `limit` derniers prix de clôture 5-minutes pour un asset.
    Lit la vue `candles_5m` (temps réel + historique Binance, cf. init_timescale_policies.sql).
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT close
            FROM candles_5m
            WHERE symbol = %s
            ORDER BY bucket DESC
            LIMIT %s
        """, (symbol, limit))
        rows = cur.fetchall()

    # Inverser pour avoir l'ordre chronologique (ancien → récent)
    return [float(r[0]) for r in reversed(rows)]


def fetch_window_stats(conn, symbol: str, window: str) -> Optional[dict]:
    """
    Récupère les stats agrégées de la dernière fenêtre complète
    depuis TimescaleDB pour calculer VWAP et Bollinger précis.
    """
    interval_map = {
        "1 minute":  "1 minute",
        "5 minutes": "5 minutes",
        "1 hour":    "1 hour",
    }
    interval = interval_map.get(window, "5 minutes")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                SUM(price * quantity)   AS sum_pq,
                SUM(quantity)           AS sum_q,
                AVG(price)              AS price_avg,
                STDDEV(price)           AS price_stddev,
                MIN(price)              AS price_low,
                MAX(price)              AS price_high,
                FIRST(price, trade_time) AS price_open,
                LAST(price, trade_time)  AS price_close,
                SUM(quantity)            AS volume,
                COUNT(*)                 AS trade_count
            FROM raw_trades
            WHERE symbol = %s
              AND trade_time >= NOW() - INTERVAL %s
              AND trade_time <  NOW()
        """, (symbol, interval))
        row = cur.fetchone()

    if not row or row[1] is None or float(row[1]) == 0:
        return None

    return {
        "sum_pq":       float(row[0]) if row[0] else None,
        "sum_q":        float(row[1]),
        "price_avg":    float(row[2]) if row[2] else None,
        "price_stddev": float(row[3]) if row[3] else None,
        "price_low":    float(row[4]) if row[4] else None,
        "price_high":   float(row[5]) if row[5] else None,
        "price_open":   float(row[6]) if row[6] else None,
        "price_close":  float(row[7]) if row[7] else None,
        "volume":       float(row[8]) if row[8] else None,
        "trade_count":  int(row[9])   if row[9] else 0,
    }

# ── Calcul et publication ─────────────────────────────────────────────────────

def compute_and_publish(producer: KafkaProducer, conn):
    now = datetime.now(timezone.utc).isoformat()
    published = 0

    for symbol in ASSETS:
        log.info(f"Processing {symbol}...")

        # RSI précis depuis l'historique 5min
        closes = fetch_recent_closes(conn, symbol, LOOKBACK_ROWS)
        rsi_value = rsi(closes, period=RSI_PERIOD) if len(closes) >= RSI_PERIOD + 1 else None

        for window in ["1 minute", "5 minutes", "1 hour"]:
            stats = fetch_window_stats(conn, symbol, window)
            if not stats:
                log.warning(f"No data for {symbol} window={window}")
                continue

            vwap_value = vwap(stats["sum_pq"], stats["sum_q"])
            bb = bollinger_bands(
                avg=stats["price_avg"],
                stddev=stats["price_stddev"],
                num_std=2.0,
            )

            msg = {
                "event_type":   "indicators",
                "symbol":       symbol,
                "window_label": window,
                "computed_at":  now,

                # VWAP
                "vwap": vwap_value,

                # RSI (précis via historique)
                "rsi":         rsi_value,
                "rsi_method":  "wilder_14" if rsi_value else None,

                # Bollinger Bands
                "bb_upper":     bb["upper"],
                "bb_middle":    bb["middle"],
                "bb_lower":     bb["lower"],
                "bb_bandwidth": bb["bandwidth"],

                # Prix de la fenêtre
                "price_open":  stats["price_open"],
                "price_close": stats["price_close"],
                "price_high":  stats["price_high"],
                "price_low":   stats["price_low"],
                "volume":      stats["volume"],
                "trade_count": stats["trade_count"],

                "source": "rsi_batch_job",
            }

            producer.send(TOPIC_OUT, key=symbol, value=msg)
            published += 1

    producer.flush()
    log.info(f"Published {published} indicator messages.")


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main():
    log.info("Starting RSI batch job...")
    producer = build_producer()
    conn = psycopg2.connect(TIMESCALE_DSN)
    try:
        compute_and_publish(producer, conn)
    finally:
        conn.close()
        producer.flush(timeout=10)
        producer.close()
    log.info("RSI batch job complete.")

if __name__ == "__main__":
    main()