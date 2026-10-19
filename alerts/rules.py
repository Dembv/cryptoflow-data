"""Règles d'alerte pures — testables sans Kafka ni réseau."""

import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class Alert:
    rule: str
    symbol: str
    severity: str          # info | warning | critical
    text: str

    @property
    def key(self) -> str:
        return f"{self.rule}:{self.symbol}"


@dataclass
class AlertConfig:
    rsi_overbought: float = 70.0
    rsi_oversold: float = 30.0
    squeeze_bandwidth: float = 0.02      # bandwidth Bollinger (fenêtre 1 h) sous lequel on signale un squeeze
    whale_eth: float = 500.0
    cooldown_sec: int = 900

    @classmethod
    def from_env(cls) -> "AlertConfig":
        d = cls()
        return cls(
            rsi_overbought=float(os.getenv("ALERT_RSI_OVERBOUGHT", d.rsi_overbought)),
            rsi_oversold=float(os.getenv("ALERT_RSI_OVERSOLD", d.rsi_oversold)),
            squeeze_bandwidth=float(os.getenv("ALERT_SQUEEZE_BANDWIDTH", d.squeeze_bandwidth)),
            whale_eth=float(os.getenv("ALERT_WHALE_ETH", d.whale_eth)),
            cooldown_sec=int(os.getenv("ALERT_COOLDOWN_SEC", d.cooldown_sec)),
        )


class Cooldown:
    """Évite de répéter la même alerte (règle + symbole) pendant `seconds`."""

    def __init__(self, seconds: float):
        self.seconds = seconds
        self._last: Dict[str, float] = {}

    def allow(self, key: str, now: Optional[float] = None) -> bool:
        now = time.monotonic() if now is None else now
        last = self._last.get(key)
        if last is not None and now - last < self.seconds:
            return False
        self._last[key] = now
        return True


def _short(value: Optional[str], n: int = 8) -> str:
    if not value:
        return "?"
    return value[:n] + "…" if len(value) > n else value


def _indicator_alerts(msg: dict, cfg: AlertConfig) -> List[Alert]:
    symbol = msg.get("symbol")
    window = msg.get("window_label")
    if not symbol:
        return []
    alerts: List[Alert] = []

    # RSI précis (Wilder 14) uniquement : publié par rsi_batch_job, une fois par fenêtre → on se
    # limite à la fenêtre 5 minutes pour ne pas tripler l'alerte. Le rsi_approx de Spark est ignoré.
    rsi = msg.get("rsi")
    if rsi is not None and window == "5 minutes":
        if rsi >= cfg.rsi_overbought:
            alerts.append(Alert("rsi_overbought", symbol, "warning", f"RSI {rsi:.1f} — zone de surachat"))
        elif rsi <= cfg.rsi_oversold:
            alerts.append(Alert("rsi_oversold", symbol, "warning", f"RSI {rsi:.1f} — zone de survente"))

    bandwidth = msg.get("bb_bandwidth")
    if window == "1 hour" and bandwidth is not None and 0 < bandwidth < cfg.squeeze_bandwidth:
        alerts.append(Alert(
            "bollinger_squeeze", symbol, "info",
            f"Squeeze Bollinger (bandwidth {bandwidth:.4f}) — breakout possible",
        ))
    return alerts


def _onchain_alerts(msg: dict, cfg: AlertConfig) -> List[Alert]:
    if msg.get("event_type") != "eth_transaction":
        return []
    value = msg.get("value_eth")
    if value is None or value < cfg.whale_eth:
        return []
    symbol = msg.get("symbol") or "ETH"
    severity = "critical" if value >= cfg.whale_eth * 10 else "warning"
    return [Alert(
        "whale_transfer", symbol, severity,
        f"Whale : {value:,.0f} ETH {_short(msg.get('from_address'))} → "
        f"{_short(msg.get('to_address'))} (tx {_short(msg.get('tx_hash'), 10)})",
    )]


def evaluate(topic: str, msg, cfg: AlertConfig) -> List[Alert]:
    """Retourne les alertes déclenchées par un message Kafka (liste vide si rien)."""
    if not isinstance(msg, dict):
        return []
    if topic == "crypto.indicators":
        return _indicator_alerts(msg, cfg)
    if topic == "crypto.onchain":
        return _onchain_alerts(msg, cfg)
    return []
