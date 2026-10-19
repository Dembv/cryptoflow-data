# path spark/timescale_sink.py

"""
CryptoFlow — Sprint 3
Consumer Kafka → TimescaleDB

Lit les 4 topics et insère dans les hypertables TimescaleDB par lots
(executemany en mode pipeline psycopg3). Un lot en erreur est rejoué ligne par
ligne, chaque ligne protégée par un SAVEPOINT : un message invalide n'invalide
jamais le reste du batch.
"""

import logging
import os
import signal
import sys
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import psycopg
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))   # racine du dépôt
from common.heartbeat import beat          # noqa: E402
from common.jsonutil import safe_json      # noqa: E402,F401  (réexporté pour les tests)

def _force_utf8_stdio() -> None:
    """Évite les UnicodeEncodeError des logs sous Windows (appelé au lancement du service uniquement)."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError, OSError):
            pass


load_dotenv(dotenv_path="config/.env", encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("timescale-sink")

# ── Config ───────────────────────────────────────────────────────────────────

KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
BATCH_SIZE     = int(os.getenv("SINK_BATCH_SIZE", 500))        # flush tous les N messages
FLUSH_INTERVAL = float(os.getenv("SINK_FLUSH_INTERVAL", 2))    # ou toutes les N secondes

TOPICS = [
    os.getenv("TOPIC_PRICES",     "crypto.prices"),
    os.getenv("TOPIC_HISTORY",    "crypto.history"),
    os.getenv("TOPIC_ONCHAIN",    "crypto.onchain"),
    os.getenv("TOPIC_INDICATORS", "crypto.indicators"),
]

# ── SQL ───────────────────────────────────────────────────────────────────────

SQL_RAW_TRADE = """
    INSERT INTO raw_trades
        (trade_id, symbol, price, quantity, side, trade_time, ingested_at, source)
    VALUES
        (%(trade_id)s, %(symbol)s, %(price)s, %(quantity)s, %(side)s,
         to_timestamp(%(trade_time)s / 1000.0),
         %(ingested_at)s, %(source)s)
    ON CONFLICT DO NOTHING
"""

SQL_PRICES_5MIN = """
    INSERT INTO prices_5min
        (bucket, symbol, open, high, low, close, volume, trade_count)
    VALUES
        (%(timestamp_iso)s, %(symbol)s, %(open)s, %(high)s,
         %(low)s, %(close)s, %(days_window)s, 0)
    ON CONFLICT (symbol, bucket) DO UPDATE SET
        high        = GREATEST(prices_5min.high, EXCLUDED.high),
        low         = LEAST(prices_5min.low,     EXCLUDED.low),
        close       = EXCLUDED.close
"""

SQL_INDICATOR = """
    INSERT INTO indicators
        (computed_at, window_start, symbol, window_label, vwap, rsi_approx,
         bb_upper, bb_middle, bb_lower, bb_bandwidth,
         buy_ratio, trade_count, volume)
    VALUES
        (%(computed_at)s, %(window_start)s, %(symbol)s, %(window_label)s, %(vwap)s, %(rsi_approx)s,
         %(bb_upper)s, %(bb_middle)s, %(bb_lower)s, %(bb_bandwidth)s,
         %(buy_ratio)s, %(trade_count)s, %(volume)s)
    ON CONFLICT DO NOTHING
"""

SQL_ONCHAIN = """
    INSERT INTO onchain_events
        (tx_hash, event_type, symbol, block_number, timestamp_ts,
         from_address, to_address, value_eth, value_token, gas_used, ingested_at)
    VALUES
        (%(tx_hash)s, %(event_type)s, %(symbol)s, %(block_number)s,
         %(timestamp_iso)s, %(from_address)s, %(to_address)s,
         %(value_eth)s, %(value_token)s, %(gas_used)s, %(ingested_at)s)
    ON CONFLICT DO NOTHING
"""

# ── Routing ───────────────────────────────────────────────────────────────────

class Row(dict):
    """dict qui renvoie None pour une clé absente (champs Kafka optionnels)."""
    def __missing__(self, key):
        return None


INDICATOR_KEYS = (
    "computed_at", "window_start", "symbol", "window_label", "vwap", "rsi_approx",
    "bb_upper", "bb_middle", "bb_lower", "bb_bandwidth",
    "buy_ratio", "trade_count", "volume",
)


def build_statement(topic: str, msg) -> Optional[Tuple[str, dict]]:
    """Retourne (sql, params) ou None si le message doit être ignoré."""
    if not isinstance(msg, dict):
        return None

    if topic == "crypto.prices":
        return SQL_RAW_TRADE, msg

    if topic == "crypto.history":
        # market_snapshot ignoré pour TimescaleDB (va dans Redis)
        return (SQL_PRICES_5MIN, msg) if msg.get("event_type") == "ohlcv" else None

    if topic == "crypto.indicators":
        row = {k: msg.get(k) for k in INDICATOR_KEYS}
        if row["rsi_approx"] is None:
            row["rsi_approx"] = msg.get("rsi")              # rsi_batch_job publie "rsi"
        if row["window_start"] is None:
            row["window_start"] = row["computed_at"]        # messages batch : pas de fenêtre Spark
        if not (row["symbol"] and row["computed_at"] and row["window_label"]):
            return None
        return SQL_INDICATOR, row

    if topic == "crypto.onchain":
        is_token = msg.get("event_type") == "token_transfer"
        return SQL_ONCHAIN, {**msg, "value_token": msg.get("value") if is_token else None}

    return None


Pending = List[Tuple[str, str, Row]]      # (topic, sql, params)


def _insert_one(conn, sql: str, row: Row) -> bool:
    """Insertion unitaire protégée par un SAVEPOINT."""
    conn.execute("SAVEPOINT msg")
    try:
        conn.execute(sql, row)
        conn.execute("RELEASE SAVEPOINT msg")
        return True
    except (psycopg.Error, ValueError, TypeError) as e:
        conn.execute("ROLLBACK TO SAVEPOINT msg")
        log.warning("Insert error: %s", e)
        return False


def write_batch(conn, pending: Pending) -> Dict[str, int]:
    """Écrit les lignes groupées par requête SQL. Retourne {topic: lignes écrites}."""
    written: Dict[str, int] = defaultdict(int)
    groups: Dict[str, List[Tuple[str, Row]]] = defaultdict(list)
    for topic, sql, row in pending:
        groups[sql].append((topic, row))

    for sql, items in groups.items():
        conn.execute("SAVEPOINT batch")
        try:
            with conn.cursor() as cur:
                cur.executemany(sql, [row for _, row in items])
            conn.execute("RELEASE SAVEPOINT batch")
            for topic, _ in items:
                written[topic] += 1
        except (psycopg.Error, ValueError, TypeError) as e:
            conn.execute("ROLLBACK TO SAVEPOINT batch")
            log.warning("Batch insert failed (%s) — repli ligne par ligne", e)
            for topic, row in items:
                if _insert_one(conn, sql, row):
                    written[topic] += 1
    return dict(written)

# ── Main loop ─────────────────────────────────────────────────────────────────

def run(shutdown_flag: list):
    from kafka import KafkaConsumer

    conn = psycopg.connect(
        host=os.getenv("TIMESCALE_HOST", "localhost"),
        port=int(os.getenv("TIMESCALE_PORT", 5433)),
        dbname=os.getenv("TIMESCALE_DB", "cryptoflow"),
        user=os.getenv("TIMESCALE_USER", "crypto"),
        password=os.getenv("TIMESCALE_PASSWORD", "crypto"),
    )

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="cryptoflow-timescale-sink",
        auto_offset_reset="earliest",
        value_deserializer=safe_json,
        enable_auto_commit=False,
        consumer_timeout_ms=1_000,
    )

    log.info("Subscribed to topics: %s", TOPICS)

    stats = {t: 0 for t in TOPICS}
    pending: Pending = []
    last_flush = time.time()

    def flush():
        nonlocal pending, last_flush
        if pending:
            written = write_batch(conn, pending)
            conn.commit()
            consumer.commit()
            for topic, n in written.items():
                stats[topic] = stats.get(topic, 0) + n
            log.info("Flushed %d/%d rows | stats: %s", sum(written.values()), len(pending), stats)
            pending = []
        last_flush = time.time()

    try:
        while not shutdown_flag[0]:
            beat()
            for record in consumer:          # se termine après consumer_timeout_ms sans message
                if shutdown_flag[0]:
                    break
                stmt = build_statement(record.topic, record.value)
                if stmt is not None:
                    sql, params = stmt
                    pending.append((record.topic, sql, Row(params)))
                if len(pending) >= BATCH_SIZE or time.time() - last_flush >= FLUSH_INTERVAL:
                    flush()
                    beat()
            flush()
    except Exception:
        log.exception("Sink error")
        pending = []                         # ne pas committer les offsets de lignes perdues
        conn.rollback()
        raise
    finally:
        try:
            flush()
        except Exception:
            conn.rollback()
        consumer.close()
        conn.close()
        log.info("Timescale sink stopped. Final stats: %s", stats)

# ── Entrypoint ────────────────────────────────────────────────────────────────

def main():
    _force_utf8_stdio()
    shutdown_flag = [False]

    def handle_shutdown(sig, frame):
        log.info(f"Signal {sig} reçu, arrêt...")
        shutdown_flag[0] = True

    signal.signal(signal.SIGINT,  handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    run(shutdown_flag)

if __name__ == "__main__":
    main()
