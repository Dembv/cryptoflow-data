# path: api/main.py
"""CryptoFlow API v1 — Redis (temps réel), TimescaleDB (historique), schémas marts/gold (dbt)."""

import asyncio
import csv
import io
import json
import logging
import secrets
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Literal, Optional

import psycopg
import redis
from fastapi import (
    APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect,
)
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from api.schemas import Candle, Indicator, Page, Price, SourceFreshness, Status, WhaleTx
from api.settings import get_settings
from common.kafka_lag import lag_report

log = logging.getLogger("cryptoflow.api")

settings = get_settings()                      # échoue vite si la configuration est invalide
ASSETS = settings.asset_list
Window = Literal["1 minute", "5 minutes", "1 hour"]
Fmt = Literal["json", "csv"]
KAFKA_GROUPS = ("cryptoflow-timescale-sink", "cryptoflow-redis-sink")

pool = ConnectionPool(
    conninfo=make_conninfo(
        host=settings.timescale_host,
        port=settings.timescale_port,
        dbname=settings.timescale_db,
        user=settings.timescale_user,
        password=settings.timescale_password,
    ),
    min_size=1,
    max_size=max(settings.db_pool_max, 1),
    open=False,
    kwargs={"row_factory": dict_row},
)
cache = redis.Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    decode_responses=True,
    socket_timeout=2,
    socket_connect_timeout=2,
)

# ── Utilitaires ───────────────────────────────────────────────────────────────


class RateLimiter:
    """Fenêtre glissante par client, en mémoire (suffisant pour une instance unique)."""

    def __init__(self, limit: int, window: float = 60.0):
        self.limit = limit
        self.window = window
        self.hits: Dict[str, deque] = defaultdict(deque)

    def allow(self, key: str, now: Optional[float] = None) -> bool:
        if self.limit <= 0:
            return True
        now = time.monotonic() if now is None else now
        hits = self.hits[key]
        while hits and now - hits[0] > self.window:
            hits.popleft()
        if len(hits) >= self.limit:
            return False
        hits.append(now)
        return True


limiter = RateLimiter(settings.rate_limit_per_min)


class Hub:
    """Diffuse les derniers prix à tous les clients WebSocket : une seule lecture par tick."""

    def __init__(self) -> None:
        self.clients: set = set()

    async def run(self) -> None:
        while True:
            await asyncio.sleep(settings.ws_interval_sec)
            if not self.clients:
                continue
            try:
                payload = jsonable_encoder(await asyncio.to_thread(latest_prices))
            except Exception:
                log.exception("Lecture des prix impossible")
                continue
            for ws in list(self.clients):
                try:
                    await ws.send_json(payload)
                except Exception:
                    self.clients.discard(ws)


hub = Hub()


def rows(sql: str, params=()) -> List[dict]:
    with pool.connection() as conn:
        return conn.execute(sql, params).fetchall()


def gold_rows(sql: str, params=()) -> List[dict]:
    try:
        return rows(sql, params)
    except psycopg.errors.UndefinedTable:
        raise HTTPException(503, "Tables dbt absentes : lancer le DAG cryptoflow_dbt_hourly")


def cached_json(key: str, ttl: int, loader: Callable[[], Any]) -> Any:
    try:
        raw = cache.get(key)
        if raw:
            return json.loads(raw)
    except redis.RedisError:
        pass
    data = jsonable_encoder(loader())
    try:
        cache.set(key, json.dumps(data), ex=ttl)
    except redis.RedisError:
        pass
    return data


def check_symbol(symbol: str) -> str:
    sym = symbol.upper()
    if sym not in ASSETS:
        raise HTTPException(404, f"Symbole inconnu : {symbol} (disponibles : {ASSETS})")
    return sym


def paginate(items: List[Any], limit: int, offset: int) -> Dict[str, Any]:
    """`items` a été lu avec LIMIT limit+1 : l'élément en trop indique qu'il reste une page."""
    has_next = len(items) > limit
    return {
        "items": items[:limit],
        "limit": limit,
        "offset": offset,
        "next_offset": offset + limit if has_next else None,
    }


def csv_response(items: List[dict], filename: str) -> Response:
    buf = io.StringIO()
    if items:
        writer = csv.DictWriter(buf, fieldnames=list(items[0]))
        writer.writeheader()
        for item in items:
            writer.writerow({k: v.isoformat() if isinstance(v, datetime) else v for k, v in item.items()})
    return Response(
        buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def latest_prices() -> Dict[str, dict]:
    keys = [f"crypto:last_price:{s}" for s in ASSETS]
    out: Dict[str, dict] = {}
    try:
        for sym, raw in zip(ASSETS, cache.mget(keys)):
            if raw:
                out[sym] = {**json.loads(raw), "cache": "redis"}
    except redis.RedisError:
        log.warning("Redis indisponible, repli sur TimescaleDB")
    missing = [s for s in ASSETS if s not in out]
    if missing:   # TTL Redis expiré : repli sur le dernier trade en base
        for r in rows(
            """SELECT DISTINCT ON (symbol) symbol, price, quantity, side,
                      trade_time AS trade_time_iso
               FROM raw_trades
               WHERE symbol = ANY(%s) AND trade_time > now() - interval '1 hour'
               ORDER BY symbol, trade_time DESC""",
            (missing,),
        ):
            r["trade_time_iso"] = r["trade_time_iso"].isoformat()
            out[r.pop("symbol")] = {**r, "cache": "timescale"}
    return out


def require_key(x_api_key: Optional[str] = Header(default=None)) -> None:
    expected = settings.api_key
    if expected and not (x_api_key and secrets.compare_digest(x_api_key, expected)):
        raise HTTPException(401, "Clé d'API absente ou invalide (en-tête X-API-Key)")


# ── Application ───────────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(_: FastAPI):
    pool.open(wait=True, timeout=30)
    task = asyncio.create_task(hub.run())
    yield
    task.cancel()
    pool.close()


app = FastAPI(title="CryptoFlow API", version="1.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_list,
    allow_methods=["GET"],
    allow_headers=["X-API-Key"],
)


@app.middleware("http")
async def rate_limit(request: Request, call_next):
    if request.url.path.startswith("/v1"):
        client = request.client.host if request.client else "unknown"
        if not limiter.allow(client):
            return JSONResponse({"detail": "Trop de requêtes"}, status_code=429, headers={"Retry-After": "60"})
    return await call_next(request)


@app.get("/health", tags=["ops"])
def health():
    status: Dict[str, str] = {}
    try:
        cache.ping()
        status["redis"] = "ok"
    except redis.RedisError as e:
        status["redis"] = str(e)
    try:
        rows("SELECT 1")
        status["timescale"] = "ok"
    except Exception as e:
        status["timescale"] = str(e)
    if any(v != "ok" for v in status.values()):
        raise HTTPException(503, status)
    return status


router = APIRouter(prefix="/v1", dependencies=[Depends(require_key)])


@router.get("/prices", response_model=Dict[str, Price], tags=["marché"])
def prices():
    return latest_prices()


@router.get("/prices/{symbol}", response_model=Price, tags=["marché"])
def price(symbol: str):
    sym = check_symbol(symbol)
    data = latest_prices().get(sym)
    if not data:
        raise HTTPException(404, f"Aucun trade récent pour {sym}")
    return data


@router.get("/market/summary", response_model=List[Dict[str, Any]], tags=["marché"])
def market_summary():
    return cached_json(
        "cache:market_summary",
        settings.cache_ttl_summary,
        lambda: gold_rows("SELECT * FROM gold.market_summary"),
    )


@router.get("/market/correlations", response_model=Dict[str, Any], tags=["marché"])
def correlations(window: Literal["7d", "30d"] = "7d"):
    data = gold_rows(
        "SELECT * FROM marts.correlation_matrix WHERE window_label = %s ORDER BY computed_date DESC LIMIT 1",
        (window,),
    )
    if not data:
        raise HTTPException(404, f"Aucune corrélation {window} (dbt pas encore exécuté ?)")
    return data[0]


@router.get("/indicators/{symbol}", response_model=Page[Indicator], tags=["marché"])
def indicators(
    symbol: str,
    window: Window = "5 minutes",
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    format: Fmt = "json",
):
    sym = check_symbol(symbol)
    # Spark réécrit chaque fenêtre à chaque trigger : on ne garde que la dernière version.
    data = rows(
        """SELECT DISTINCT ON (window_start)
                  window_start, computed_at, window_label, vwap, rsi_approx AS rsi,
                  bb_upper, bb_middle, bb_lower, bb_bandwidth, buy_ratio, trade_count, volume
           FROM indicators
           WHERE symbol = %s AND window_label = %s AND window_start IS NOT NULL
           ORDER BY window_start DESC, computed_at DESC
           LIMIT %s OFFSET %s""",
        (sym, window, limit + 1, offset),
    )
    page = paginate(data, limit, offset)
    if format == "csv":
        return csv_response(page["items"], f"{sym}_indicators.csv")
    return page


@router.get("/ohlcv/{symbol}", response_model=Page[Candle], tags=["marché"])
def ohlcv(
    symbol: str,
    limit: int = Query(200, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    format: Fmt = "json",
):
    sym = check_symbol(symbol)
    data = rows(
        """SELECT bucket, open, high, low, close, volume, trade_count
           FROM candles_5m WHERE symbol = %s
           ORDER BY bucket DESC LIMIT %s OFFSET %s""",
        (sym, limit + 1, offset),
    )
    page = paginate(data, limit, offset)
    page["items"] = list(reversed(page["items"]))      # ordre chronologique pour les graphiques
    if format == "csv":
        return csv_response(page["items"], f"{sym}_ohlcv_5m.csv")
    return page


@router.get("/whales/daily", response_model=List[Dict[str, Any]], tags=["on-chain"])
def whales_daily(
    symbol: Optional[str] = None,
    min_eth: float = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    sym = check_symbol(symbol) if symbol else None
    return gold_rows(
        """SELECT * FROM gold.whale_activity
           WHERE (%(s)s::text IS NULL OR symbol = %(s)s)
             AND coalesce(max_eth_tx, 0) >= %(m)s
           ORDER BY event_date DESC, total_eth DESC NULLS LAST
           LIMIT %(l)s OFFSET %(o)s""",
        {"s": sym, "m": min_eth, "l": limit, "o": offset},
    )


@router.get("/whales/recent", response_model=List[WhaleTx], tags=["on-chain"])
def whales_recent(
    min_eth: float = Query(100, ge=0),
    limit: int = Query(50, ge=1, le=500),
    format: Fmt = "json",
):
    data = rows(
        """SELECT tx_hash, event_type, symbol, timestamp_ts, from_address, to_address,
                  value_eth, value_token, block_number
           FROM onchain_events
           WHERE coalesce(value_eth, 0) >= %s
           ORDER BY timestamp_ts DESC LIMIT %s""",
        (min_eth, limit),
    )
    return csv_response(data, "whales_recent.csv") if format == "csv" else data


# ── Statut ────────────────────────────────────────────────────────────────────

FRESHNESS_SOURCES = {
    "trades":     ("SELECT max(trade_time)   AS ts FROM raw_trades",     60),
    "indicators": ("SELECT max(computed_at)  AS ts FROM indicators",     180),
    "onchain":    ("SELECT max(timestamp_ts) AS ts FROM onchain_events", 3600),
}
_lag_cache: Dict[str, Any] = {"at": 0.0, "value": {}}


def kafka_lag_cached(ttl: float = 15.0) -> Dict[str, Any]:
    if time.monotonic() - _lag_cache["at"] > ttl:
        _lag_cache["value"] = lag_report(settings.kafka_bootstrap_servers, KAFKA_GROUPS)
        _lag_cache["at"] = time.monotonic()
    return _lag_cache["value"]


def build_status(now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    services: Dict[str, str] = {}
    try:
        cache.ping()
        services["redis"] = "ok"
    except redis.RedisError as e:
        services["redis"] = f"erreur: {e}"

    freshness: Dict[str, dict] = {}
    trades_last_minute = None
    try:
        for name, (sql, max_age) in FRESHNESS_SOURCES.items():
            ts = rows(sql)[0]["ts"]
            if ts is None:
                freshness[name] = {"last_event": None, "age_seconds": None, "status": "empty"}
            else:
                age = (now - ts).total_seconds()
                freshness[name] = {
                    "last_event": ts,
                    "age_seconds": round(age, 1),
                    "status": "ok" if age <= max_age else "stale",
                }
        trades_last_minute = rows(
            "SELECT count(*) AS n FROM raw_trades WHERE trade_time > now() - interval '1 minute'"
        )[0]["n"]
        services["timescale"] = "ok"
    except Exception as e:
        services["timescale"] = f"erreur: {e}"

    if services.get("timescale") != "ok":
        overall = "down"
    elif services.get("redis") != "ok" or any(f["status"] != "ok" for f in freshness.values()):
        overall = "degraded"
    else:
        overall = "ok"

    return {
        "status": overall,
        "generated_at": now,
        "services": services,
        "freshness": freshness,
        "trades_last_minute": trades_last_minute,
        "kafka": kafka_lag_cached(),
    }


@router.get("/status", response_model=Status, tags=["ops"])
def status():
    """Fraîcheur des données par source, débit et retard des consumers Kafka."""
    return build_status()


app.include_router(router)


@app.websocket("/v1/ws/prices")
async def ws_prices(ws: WebSocket, api_key: Optional[str] = None):
    if settings.api_key and not (api_key and secrets.compare_digest(api_key, settings.api_key)):
        await ws.close(code=4401)
        return
    await ws.accept()
    hub.clients.add(ws)
    try:
        while True:
            await ws.receive_text()        # les messages entrants sont ignorés ; détecte la déconnexion
    except WebSocketDisconnect:
        pass
    finally:
        hub.clients.discard(ws)
