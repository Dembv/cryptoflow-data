"""Tests des règles d'alerte, du cooldown et des notifiers (aucun réseau)."""

import pytest

from alerts import notifiers
from alerts.rules import Alert, AlertConfig, Cooldown, evaluate
from alerts.service import process

CFG = AlertConfig()


def indicator(**kw):
    return {"symbol": "BTC", "window_label": "5 minutes", **kw}


# ── Règles ────────────────────────────────────────────────────────────────────

def test_rsi_overbought_and_oversold():
    over = evaluate("crypto.indicators", indicator(rsi=75.0), CFG)
    under = evaluate("crypto.indicators", indicator(rsi=25.0), CFG)
    assert [a.rule for a in over] == ["rsi_overbought"]
    assert [a.rule for a in under] == ["rsi_oversold"]


def test_neutral_rsi_is_silent():
    assert evaluate("crypto.indicators", indicator(rsi=50.0), CFG) == []


def test_rsi_only_on_five_minute_window():
    msg = {"symbol": "BTC", "window_label": "1 hour", "rsi": 90.0}
    assert evaluate("crypto.indicators", msg, CFG) == []


def test_spark_rsi_approx_is_ignored():
    assert evaluate("crypto.indicators", indicator(rsi_approx=100.0), CFG) == []


def test_bollinger_squeeze_on_hourly_window():
    msg = {"symbol": "ETH", "window_label": "1 hour", "bb_bandwidth": 0.01}
    assert [a.rule for a in evaluate("crypto.indicators", msg, CFG)] == ["bollinger_squeeze"]


@pytest.mark.parametrize("bandwidth", [0, 0.02, 0.5, None])
def test_no_squeeze_when_bandwidth_flat_or_wide(bandwidth):
    msg = {"symbol": "ETH", "window_label": "1 hour", "bb_bandwidth": bandwidth}
    assert evaluate("crypto.indicators", msg, CFG) == []


def whale(value, **kw):
    return {"event_type": "eth_transaction", "symbol": "ETH", "value_eth": value,
            "from_address": "0xabcdef123456", "to_address": "0x987654fedcba", "tx_hash": "0x" + "a" * 64, **kw}


def test_whale_threshold_and_severity():
    assert evaluate("crypto.onchain", whale(499), CFG) == []
    warning = evaluate("crypto.onchain", whale(600), CFG)[0]
    critical = evaluate("crypto.onchain", whale(6000), CFG)[0]
    assert (warning.severity, critical.severity) == ("warning", "critical")
    assert "ETH" in warning.text and "0xabcdef" in warning.text


def test_token_transfers_do_not_trigger_whale_alert():
    msg = whale(10_000, event_type="token_transfer")
    assert evaluate("crypto.onchain", msg, CFG) == []


def test_invalid_messages_and_topics_are_ignored():
    assert evaluate("crypto.indicators", None, CFG) == []
    assert evaluate("crypto.prices", {"symbol": "BTC"}, CFG) == []


def test_config_from_env(monkeypatch):
    monkeypatch.setenv("ALERT_WHALE_ETH", "42")
    monkeypatch.setenv("ALERT_COOLDOWN_SEC", "5")
    cfg = AlertConfig.from_env()
    assert (cfg.whale_eth, cfg.cooldown_sec, cfg.rsi_overbought) == (42.0, 5, 70.0)


# ── Cooldown ──────────────────────────────────────────────────────────────────

def test_cooldown_blocks_repeats_until_expiry():
    cooldown = Cooldown(60)
    assert cooldown.allow("rsi:BTC", now=0)
    assert not cooldown.allow("rsi:BTC", now=30)
    assert cooldown.allow("rsi:ETH", now=30)           # autre clé : indépendante
    assert cooldown.allow("rsi:BTC", now=61)


# ── Notifiers ─────────────────────────────────────────────────────────────────

ALERT = Alert("rsi_overbought", "BTC", "warning", "RSI 75.0")


def test_format_alert():
    assert notifiers.format_alert(ALERT) == "[ATTENTION] BTC — RSI 75.0"


def test_build_notifiers_from_env():
    assert notifiers.build_notifiers({}) == []
    built = notifiers.build_notifiers({"SLACK_WEBHOOK_URL": "https://hooks/x", "TELEGRAM_BOT_TOKEN": "t",
                                       "TELEGRAM_CHAT_ID": "1"})
    assert [n.name for n in built] == ["slack", "telegram"]
    assert notifiers.build_notifiers({"TELEGRAM_BOT_TOKEN": "t"}) == []     # chat_id manquant


def test_slack_notifier_posts_formatted_text(monkeypatch):
    calls = []

    class Resp:
        def raise_for_status(self):
            pass

    monkeypatch.setattr(notifiers.requests, "post", lambda url, json, timeout: calls.append((url, json)) or Resp())
    notifiers.SlackNotifier("https://hooks/x").send(ALERT)
    assert calls == [("https://hooks/x", {"text": "[ATTENTION] BTC — RSI 75.0"})]


def test_dispatch_counts_successes_and_swallows_errors():
    class Ok:
        name = "ok"
        def send(self, alert): pass

    class Broken:
        name = "broken"
        def send(self, alert): raise RuntimeError("https://hooks/secret-token")

    assert notifiers.dispatch([Broken(), Ok()], ALERT) == 1


# ── Service ───────────────────────────────────────────────────────────────────

def test_process_applies_cooldown():
    sent = []

    class Spy:
        name = "spy"
        def send(self, alert): sent.append(alert)

    cooldown = Cooldown(900)
    first = process("crypto.indicators", indicator(rsi=80.0), CFG, cooldown, [Spy()], now=0)
    again = process("crypto.indicators", indicator(rsi=81.0), CFG, cooldown, [Spy()], now=10)
    assert len(first) == 1 and again == []
    assert len(sent) == 1
