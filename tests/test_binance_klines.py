"""Tests du backfill klines Binance (HTTP et base remplacés par des doublures)."""

from datetime import datetime, timedelta, timezone

import binance_klines as bk

T0 = 1_700_000_000_000 - (1_700_000_000_000 % bk.INTERVAL_MS)       # aligné sur 5 min


def kline(open_time, price=100.0):
    return [open_time, str(price), str(price + 1), str(price - 1), str(price + 0.5), "10.5",
            open_time + bk.INTERVAL_MS - 1, "1050", 77, "0", "0", "0"]


class FakeResponse:
    def __init__(self, data):
        self.data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self.data


class FakeSession:
    def __init__(self, pages):
        self.pages = list(pages)
        self.params = []

    def get(self, url, params, timeout):
        self.params.append(params)
        return FakeResponse(self.pages.pop(0))


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.conn.queries.append(params)

    def fetchone(self):
        return (self.conn.last,)

    def executemany(self, sql, rows):
        self.conn.rows = list(rows)


class FakeConn:
    def __init__(self, last=None):
        self.last = last
        self.queries = []
        self.rows = None
        self.committed = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.committed = True


def test_to_row_converts_binance_kline():
    bucket, symbol, o, h, l, c, v, trades = bk.to_row("BTC", kline(T0, 100.0))
    assert bucket == datetime.fromtimestamp(T0 / 1000, tz=timezone.utc)
    assert (symbol, o, h, l, c, v, trades) == ("BTC", 100.0, 101.0, 99.0, 100.5, 10.5, 77)


def test_fetch_klines_follows_pages():
    page1 = [kline(T0 + i * bk.INTERVAL_MS) for i in range(bk.PAGE_LIMIT)]
    page2 = [kline(T0 + (bk.PAGE_LIMIT + i) * bk.INTERVAL_MS) for i in range(5)]
    session = FakeSession([page1, page2])

    out = list(bk.fetch_klines("BTC", T0, T0 + 2000 * bk.INTERVAL_MS, session=session, pause=0))

    assert len(out) == bk.PAGE_LIMIT + 5
    assert session.params[0]["symbol"] == "BTCUSDT" and session.params[0]["interval"] == "5m"
    assert session.params[1]["startTime"] == page1[-1][0] + bk.INTERVAL_MS      # reprend après la 1re page
    assert len(session.params) == 2                                              # page courte = fin


def test_fetch_klines_stops_on_empty_page():
    session = FakeSession([[]])
    assert list(bk.fetch_klines("ETH", T0, T0 + bk.INTERVAL_MS * 10, session=session, pause=0)) == []


def test_backfill_starts_days_back_and_skips_open_candle():
    now = datetime.fromtimestamp((T0 + 10 * bk.INTERVAL_MS + 60_000) / 1000, tz=timezone.utc)
    closed = [kline(T0 + i * bk.INTERVAL_MS) for i in range(3)]
    in_progress = kline(T0 + 10 * bk.INTERVAL_MS)            # closeTime > now : bougie non terminée
    seen = {}

    def fake_fetch(symbol, start_ms, end_ms):
        seen.update(start=start_ms, end=end_ms)
        return iter(closed + [in_progress])

    conn = FakeConn(last=None)
    written = bk.backfill_symbol(conn, "BTC", days=2, now=now, fetch=fake_fetch)

    assert written == 3 and len(conn.rows) == 3 and conn.committed
    assert seen["start"] == int((now - timedelta(days=2)).timestamp() * 1000)
    assert seen["end"] == int(now.timestamp() * 1000)


def test_backfill_resumes_one_hour_before_last_bucket():
    now = datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc)
    last = datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc)
    seen = {}

    def fake_fetch(symbol, start_ms, end_ms):
        seen["start"] = start_ms
        return iter([])

    conn = FakeConn(last=last)
    assert bk.backfill_symbol(conn, "SOL", days=30, now=now, fetch=fake_fetch) == 0
    assert seen["start"] == int((last - timedelta(hours=1)).timestamp() * 1000)
    assert not conn.committed                                   # rien à écrire
