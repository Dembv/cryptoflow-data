"""Tests du générateur de charge (sans Kafka)."""

import random
import re

import pytest

from tools import load_generator as lg


def test_make_trade_matches_binance_producer_format():
    trade = lg.make_trade("BTC", 65000.0, 7, 1_700_000_000_000, random.Random(1))
    assert set(trade) == {"event_type", "symbol", "pair", "price", "quantity", "trade_id", "side",
                          "trade_time", "trade_time_iso", "ingested_at", "source"}
    assert trade["pair"] == "BTCUSDT" and trade["side"] in ("buy", "sell")
    assert trade["trade_time_iso"].startswith("2023-11-14T")
    assert trade["source"] == "synthetic_load"


def test_next_price_stays_positive_and_is_deterministic():
    walk = lambda seed: [lg.next_price(0.5, random.Random(seed), volatility=5.0) for _ in range(50)]
    assert all(p > 0 for p in walk(3))
    assert walk(3) == walk(3)


@pytest.mark.parametrize("rate,tick,per_tick", [(500, 0.05, 25), (100, 0.05, 5), (5, 0.2, 1)])
def test_plan_ticks_approximates_rate(rate, tick, per_tick):
    assert lg.plan_ticks(rate) == (pytest.approx(tick), per_tick)


def test_synthetic_trades_satisfy_the_sink_contract():
    """Chaque paramètre nommé de l'INSERT raw_trades doit exister dans le message généré."""
    pytest.importorskip("psycopg")
    pytest.importorskip("dotenv")
    from spark import timescale_sink as sink

    trade = lg.make_trade("ETH", 3400.0, 1, 1_700_000_000_000, random.Random(2))
    sql, params = sink.build_statement("crypto.prices", trade)
    assert sql == sink.SQL_RAW_TRADE
    assert set(re.findall(r"%\((\w+)\)s", sql)) <= set(params)
