"""Tests de l'API v1 (Redis et TimescaleDB remplacés par des doublures)."""

import json
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("psycopg_pool")
pytest.importorskip("pydantic_settings")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient  # noqa: E402

from api import main  # noqa: E402
from api.settings import Settings  # noqa: E402

NOW = datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc)
BTC = {"price": 65000.0, "quantity": 0.1, "side": "buy", "trade_time_iso": "2026-03-15T12:00:00+00:00",
       "ingested_at": "2026-03-15T12:00:00+00:00", "source": "binance_ws"}


class FakeCache:
    def __init__(self, store=None):
        self.store = dict(store or {})

    def mget(self, keys):
        return [self.store.get(k) for k in keys]

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, ex=None):
        self.store[key] = value

    def ping(self):
        return True


@pytest.fixture
def env(monkeypatch):
    """(client, cache, calls) — `main.rows` renvoie la réponse du premier marqueur présent dans le SQL."""
    cache = FakeCache()
    answers = {}
    calls = []

    def fake_rows(sql, params=()):
        calls.append(sql)
        for marker, result in answers.items():
            if marker in sql:
                return result
        return []

    monkeypatch.setattr(main, "cache", cache)
    monkeypatch.setattr(main, "rows", fake_rows)
    monkeypatch.setattr(main, "limiter", main.RateLimiter(0))
    monkeypatch.setattr(main.settings, "api_key", None)
    monkeypatch.setattr(main, "kafka_lag_cached", lambda: {})
    client = TestClient(main.app)          # sans `with` : le lifespan (pool, hub) n'est pas démarré
    return client, cache, answers, calls


def indicator_row(i):
    ts = NOW - timedelta(minutes=i)
    return {"window_start": ts, "computed_at": ts, "window_label": "5 minutes", "vwap": 1.0, "rsi": 50.0,
            "bb_upper": 2.0, "bb_middle": 1.0, "bb_lower": 0.0, "bb_bandwidth": 0.1, "buy_ratio": 0.5,
            "trade_count": 10, "volume": 3.0}


def test_health_ok(env):
    client, _, answers, _ = env
    answers["SELECT 1"] = [{"?column?": 1}]
    assert client.get("/health").status_code == 200


def test_prices_served_from_redis(env):
    client, cache, _, _ = env
    cache.store["crypto:last_price:BTC"] = json.dumps(BTC)
    body = client.get("/v1/prices").json()
    assert body["BTC"]["price"] == 65000.0 and body["BTC"]["cache"] == "redis"
    assert "ETH" not in body


def test_prices_fall_back_to_timescale(env):
    client, _, answers, _ = env
    answers["FROM raw_trades"] = [{"symbol": "ETH", "price": 3400.0, "quantity": 1.0, "side": "sell",
                                   "trade_time_iso": NOW}]
    eth = client.get("/v1/prices/eth").json()
    assert eth["cache"] == "timescale" and eth["trade_time_iso"] == NOW.isoformat()


def test_unknown_symbol_is_404(env):
    assert env[0].get("/v1/prices/DOGE").status_code == 404


def test_missing_price_is_404(env):
    assert env[0].get("/v1/prices/BTC").status_code == 404


def test_api_key_is_enforced_when_configured(env, monkeypatch):
    client, cache, _, _ = env
    cache.store["crypto:last_price:BTC"] = json.dumps(BTC)
    monkeypatch.setattr(main.settings, "api_key", "secret")
    assert client.get("/v1/prices").status_code == 401
    assert client.get("/v1/prices", headers={"X-API-Key": "faux"}).status_code == 401
    assert client.get("/v1/prices", headers={"X-API-Key": "secret"}).status_code == 200
    assert client.get("/health").status_code != 401            # la santé reste publique


def test_indicators_are_paginated(env):
    client, _, answers, _ = env
    answers["FROM indicators"] = [indicator_row(i) for i in range(3)]       # limit+1 lignes → page suivante
    page = client.get("/v1/indicators/BTC?limit=2&offset=4").json()
    assert len(page["items"]) == 2
    assert (page["limit"], page["offset"], page["next_offset"]) == (2, 4, 6)

    answers["FROM indicators"] = [indicator_row(0)]
    assert client.get("/v1/indicators/BTC?limit=2").json()["next_offset"] is None


def test_indicators_csv_export(env):
    client, _, answers, _ = env
    answers["FROM indicators"] = [indicator_row(0)]
    resp = client.get("/v1/indicators/BTC?format=csv")
    assert resp.headers["content-type"].startswith("text/csv")
    header, line = resp.text.strip().splitlines()[:2]
    assert header.startswith("window_start,computed_at,window_label")
    assert "5 minutes" in line


def test_ohlcv_is_returned_in_chronological_order(env):
    client, _, answers, _ = env
    answers["FROM candles_5m"] = [
        {"bucket": NOW - timedelta(minutes=5 * i), "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
         "volume": 3.0, "trade_count": 7}
        for i in range(3)                                                    # lues du plus récent au plus ancien
    ]
    buckets = [c["bucket"] for c in client.get("/v1/ohlcv/BTC").json()["items"]]
    assert buckets == sorted(buckets)


def test_market_summary_is_cached(env):
    client, _, answers, calls = env
    answers["gold.market_summary"] = [{"symbol": "BTC", "price_usd": 65000.0}]
    first = client.get("/v1/market/summary").json()
    second = client.get("/v1/market/summary").json()
    assert first == second == [{"symbol": "BTC", "price_usd": 65000.0}]
    assert sum("gold.market_summary" in c for c in calls) == 1


def test_correlations_404_when_dbt_has_not_run(env):
    assert env[0].get("/v1/market/correlations").status_code == 404


def test_status_ok_when_all_sources_are_fresh(env):
    client, _, answers, _ = env
    answers["count(*)"] = [{"n": 42}]
    answers["max("] = [{"ts": datetime.now(timezone.utc) - timedelta(seconds=5)}]
    body = client.get("/v1/status").json()
    assert body["status"] == "ok" and body["trades_last_minute"] == 42
    assert body["freshness"]["trades"]["status"] == "ok"


def test_status_degraded_when_data_is_stale(env):
    client, _, answers, _ = env
    answers["count(*)"] = [{"n": 0}]
    answers["max("] = [{"ts": datetime.now(timezone.utc) - timedelta(days=1)}]
    body = client.get("/v1/status").json()
    assert body["status"] == "degraded" and body["freshness"]["trades"]["status"] == "stale"


def test_rate_limit_returns_429(env, monkeypatch):
    client, cache, _, _ = env
    cache.store["crypto:last_price:BTC"] = json.dumps(BTC)
    monkeypatch.setattr(main, "limiter", main.RateLimiter(1))
    assert client.get("/v1/prices").status_code == 200
    assert client.get("/v1/prices").status_code == 429


def test_rate_limiter_window():
    limiter = main.RateLimiter(2, window=60)
    assert limiter.allow("ip", now=0) and limiter.allow("ip", now=1)
    assert not limiter.allow("ip", now=2)
    assert limiter.allow("ip", now=62)
    assert main.RateLimiter(0).allow("ip")                  # 0 = illimité


def test_settings_reject_empty_assets():
    with pytest.raises(ValueError):
        Settings(assets=" , ")
    assert Settings(api_key="").api_key is None
