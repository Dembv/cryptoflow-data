"""Tests du sink Kafka → TimescaleDB (sans Kafka ni base : connexion factice)."""

import pytest

psycopg = pytest.importorskip("psycopg")
pytest.importorskip("dotenv")

from spark import timescale_sink as sink  # noqa: E402


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def executemany(self, sql, rows):
        self.conn.log.append(("executemany", sql, len(rows)))
        if self.conn.fail_batch:
            raise psycopg.errors.DataError("lot invalide")


class FakeConn:
    def __init__(self, fail_batch=False, bad_ids=()):
        self.fail_batch = fail_batch
        self.bad_ids = set(bad_ids)
        self.log = []

    def cursor(self):
        return FakeCursor(self)

    def execute(self, sql, params=None):
        self.log.append(("execute", sql))
        if params is not None and params["trade_id"] in self.bad_ids:
            raise psycopg.errors.DataError("ligne invalide")

    def statements(self):
        return [entry[1] for entry in self.log if entry[0] == "execute"]


def trade(trade_id):
    return sink.Row({"trade_id": trade_id, "symbol": "BTC", "price": 1.0, "quantity": 1.0, "trade_time": 0})


def test_row_returns_none_for_missing_key():
    assert sink.Row({"a": 1})["b"] is None


def test_trade_message_is_passed_through():
    msg = {"symbol": "BTC", "price": 1.0}
    assert sink.build_statement("crypto.prices", msg) == (sink.SQL_RAW_TRADE, msg)


def test_history_keeps_only_ohlcv():
    assert sink.build_statement("crypto.history", {"event_type": "market_snapshot"}) is None
    assert sink.build_statement("crypto.history", {"event_type": "ohlcv"})[0] == sink.SQL_PRICES_5MIN


def test_indicator_falls_back_to_batch_rsi_and_window_start():
    msg = {"symbol": "BTC", "window_label": "5 minutes", "computed_at": "2026-01-01T00:00:00Z", "rsi": 55.0}
    sql, params = sink.build_statement("crypto.indicators", msg)
    assert sql == sink.SQL_INDICATOR
    assert params["rsi_approx"] == 55.0
    assert params["window_start"] == "2026-01-01T00:00:00Z"


def test_indicator_keeps_spark_window_start():
    msg = {"symbol": "BTC", "window_label": "1 minute", "computed_at": "t2", "window_start": "t1", "rsi_approx": 50.0}
    params = sink.build_statement("crypto.indicators", msg)[1]
    assert params["window_start"] == "t1"
    assert params["rsi_approx"] == 50.0


@pytest.mark.parametrize("missing", ["symbol", "computed_at", "window_label"])
def test_indicator_requires_identity_fields(missing):
    msg = {"symbol": "BTC", "window_label": "1 minute", "computed_at": "t"}
    del msg[missing]
    assert sink.build_statement("crypto.indicators", msg) is None


def test_onchain_maps_token_value_only_for_token_transfers():
    token = sink.build_statement("crypto.onchain", {"event_type": "token_transfer", "value": 12.5})[1]
    eth = sink.build_statement("crypto.onchain", {"event_type": "eth_transaction", "value": 12.5})[1]
    assert token["value_token"] == 12.5
    assert eth["value_token"] is None


def test_non_dict_and_unknown_topic_are_ignored():
    assert sink.build_statement("crypto.prices", None) is None
    assert sink.build_statement("crypto.prices", "texte") is None
    assert sink.build_statement("autre.topic", {"a": 1}) is None


def test_safe_json():
    assert sink.safe_json(b'{"a": 1}') == {"a": 1}
    assert sink.safe_json(b"pas du json") is None
    assert sink.safe_json(b"\xff\xfe") is None


def test_write_batch_groups_rows_by_statement():
    conn = FakeConn()
    ind = sink.Row({"symbol": "BTC"})
    pending = [
        ("crypto.prices", sink.SQL_RAW_TRADE, trade(1)),
        ("crypto.prices", sink.SQL_RAW_TRADE, trade(2)),
        ("crypto.indicators", sink.SQL_INDICATOR, ind),
    ]

    written = sink.write_batch(conn, pending)

    assert written == {"crypto.prices": 2, "crypto.indicators": 1}
    batches = [e for e in conn.log if e[0] == "executemany"]
    assert sorted(b[2] for b in batches) == [1, 2]
    assert conn.statements().count("RELEASE SAVEPOINT batch") == 2


def test_write_batch_falls_back_to_row_by_row_on_error():
    conn = FakeConn(fail_batch=True, bad_ids={2})
    pending = [("crypto.prices", sink.SQL_RAW_TRADE, trade(i)) for i in (1, 2, 3)]

    written = sink.write_batch(conn, pending)

    assert written == {"crypto.prices": 2}               # la ligne 2 est rejetée, les autres passent
    statements = conn.statements()
    assert "ROLLBACK TO SAVEPOINT batch" in statements
    assert statements.count("ROLLBACK TO SAVEPOINT msg") == 1
    assert statements.count("RELEASE SAVEPOINT msg") == 2
