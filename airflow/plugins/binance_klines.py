"""Backfill des bougies 5 min depuis l'API publique Binance (klines).

Remplace l'ancien backfill CoinGecko, dont la granularité (4 h à 4 jours) ne permettait
ni RSI ni bougies 5 min cohérents. Les bougies alimentent la table klines_5m ; la vue
candles_5m les fusionne avec les bougies temps réel (ohlcv_5m).
"""

import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterator, List, Optional, Tuple

BASE_URL = "https://api.binance.com/api/v3/klines"
INTERVAL_MS = 5 * 60 * 1000
PAGE_LIMIT = 1000

SQL_UPSERT = """
    INSERT INTO klines_5m (bucket, symbol, open, high, low, close, volume, trade_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, bucket) DO UPDATE SET
        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
        close = EXCLUDED.close, volume = EXCLUDED.volume, trade_count = EXCLUDED.trade_count
"""


def pair(symbol: str) -> str:
    return f"{symbol}USDT"


def fetch_klines(symbol: str, start_ms: int, end_ms: int, session=None,
                 base_url: str = BASE_URL, pause: float = 0.2) -> Iterator[list]:
    """Génère les klines 5 min de [start_ms, end_ms), page par page (1000 bougies max)."""
    import requests

    http = session or requests
    cursor = start_ms
    while cursor < end_ms:
        resp = http.get(
            base_url,
            params={"symbol": pair(symbol), "interval": "5m", "startTime": cursor,
                    "endTime": end_ms, "limit": PAGE_LIMIT},
            timeout=30,
        )
        resp.raise_for_status()
        page = resp.json()
        if not page:
            return
        yield from page
        next_cursor = page[-1][0] + INTERVAL_MS
        if next_cursor <= cursor or len(page) < PAGE_LIMIT:
            return
        cursor = next_cursor
        time.sleep(pause)


def to_row(symbol: str, kline: list) -> Tuple:
    """kline Binance → (bucket, symbol, open, high, low, close, volume, trade_count)."""
    open_time, o, h, l, c, v = kline[:6]
    trades = kline[8]
    bucket = datetime.fromtimestamp(open_time / 1000, tz=timezone.utc)
    return (bucket, symbol, float(o), float(h), float(l), float(c), float(v), int(trades))


def last_bucket(conn, symbol: str) -> Optional[datetime]:
    with conn.cursor() as cur:
        cur.execute("SELECT max(bucket) FROM klines_5m WHERE symbol = %s", (symbol,))
        return cur.fetchone()[0]


def backfill_symbol(conn, symbol: str, days: int, now: Optional[datetime] = None,
                    fetch: Callable[..., Iterator[list]] = fetch_klines) -> int:
    """Complète klines_5m pour `symbol` ; reprend à la dernière bougie connue (idempotent).

    Seules les bougies terminées sont insérées. Retourne le nombre de lignes écrites.
    """
    now = now or datetime.now(timezone.utc)
    end_ms = int(now.timestamp() * 1000)
    last = last_bucket(conn, symbol)
    start = (last - timedelta(hours=1)) if last else now - timedelta(days=days)

    rows: List[Tuple] = [
        to_row(symbol, k)
        for k in fetch(symbol, int(start.timestamp() * 1000), end_ms)
        if k[6] < end_ms                       # k[6] = closeTime : on écarte la bougie en cours
    ]
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(SQL_UPSERT, rows)
    conn.commit()
    return len(rows)
