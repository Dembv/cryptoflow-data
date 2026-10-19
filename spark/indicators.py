# path spark/indicators.py

"""
CryptoFlow — Sprint 2
Bibliothèque d'indicateurs financiers

Contient :
  - UDFs Spark  : compute_vwap, compute_rsi, compute_bollinger
  - Fonctions Python pures (testables sans Spark) :
      vwap(), rsi(), bollinger_bands()

Les fonctions pures sont utilisées dans les tests unitaires.
Les UDFs Spark wrappent ces fonctions pour Structured Streaming.
"""

import math
from typing import Dict, List, Optional

# ═══════════════════════════════════════════════════════════════════════════════
# FONCTIONS PYTHON PURES (testables sans Spark)
# ═══════════════════════════════════════════════════════════════════════════════

def vwap(sum_price_qty: float, sum_qty: float) -> Optional[float]:
    """
    VWAP = Σ(price × volume) / Σ(volume)

    Mesure le prix moyen pondéré par le volume sur une fenêtre.
    Utilisé par les traders institutionnels comme benchmark.

    Args:
        sum_price_qty : somme cumulée de (price × quantity)
        sum_qty       : somme cumulée des quantités échangées

    Returns:
        VWAP ou None si sum_qty == 0
    """
    if sum_qty is None or sum_qty == 0:
        return None
    if sum_price_qty is None:
        return None
    return round(sum_price_qty / sum_qty, 8)


def rsi(prices: List[float], period: int = 14) -> Optional[float]:
    """
    RSI = 100 - (100 / (1 + RS))
    RS  = Average Gain / Average Loss sur `period` périodes

    Interprétation :
      RSI > 70  → zone de surachat (potentiel retournement baissier)
      RSI < 30  → zone de survente (potentiel retournement haussier)
      RSI ~ 50  → momentum neutre

    Args:
        prices : liste de prix de clôture (du plus ancien au plus récent)
        period : nombre de périodes (défaut : 14)

    Returns:
        RSI entre 0 et 100, ou None si données insuffisantes
    """
    if prices is None or len(prices) < period + 1:
        return None

    # Calcul des variations journalières
    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]

    gains  = [d if d > 0 else 0.0 for d in deltas]
    losses = [-d if d < 0 else 0.0 for d in deltas]

    # Moyenne initiale sur `period` périodes (SMA)
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    # Lissage exponentiel (méthode Wilder) pour les périodes suivantes
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 4)


def bollinger_bands(
    avg: float,
    stddev: float,
    num_std: float = 2.0
) -> Dict[str, float]:
    """
    Bandes de Bollinger :
      Upper  = Moyenne + (num_std × écart-type)
      Middle = Moyenne mobile simple
      Lower  = Moyenne - (num_std × écart-type)

    Interprétation :
      - Prix > upper  → surachat potentiel
      - Prix < lower  → survente potentielle
      - Bandes étroites (squeeze) → faible volatilité, breakout imminent
      - Bandes larges             → forte volatilité

    Args:
        avg     : prix moyen sur la fenêtre
        stddev  : écart-type des prix sur la fenêtre
        num_std : multiplicateur d'écart-type (défaut : 2.0)

    Returns:
        dict avec upper, middle, lower, bandwidth
    """
    if avg is None or stddev is None:
        return {"upper": None, "middle": None, "lower": None, "bandwidth": None}

    std = stddev if stddev is not None else 0.0
    upper    = round(avg + num_std * std, 8)
    lower    = round(avg - num_std * std, 8)
    middle   = round(avg, 8)
    bandwidth = round((upper - lower) / avg, 8) if avg != 0 else None

    return {
        "upper":     upper,
        "middle":    middle,
        "lower":     lower,
        "bandwidth": bandwidth,
    }


def rsi_from_window_stats(
    price_open: float,
    price_close: float,
    price_avg: float,
) -> Optional[float]:
    """
    Approximation RSI depuis les statistiques d'une fenêtre Spark.

    En streaming, on ne dispose pas de la série complète des prix.
    On utilise une approximation basée sur open/close/avg :
      - Si close > open  → la fenêtre est haussière
      - Si close < open  → la fenêtre est baissière

    Pour un RSI précis, utiliser rsi() avec une série de prix complète
    (disponible en batch depuis TimescaleDB).

    Returns:
        Valeur RSI approximée [0–100] ou None
    """
    if any(v is None for v in [price_open, price_close, price_avg]):
        return None

    delta = price_close - price_open
    if delta == 0:
        return 50.0

    # Normalisation : gain/perte relative au prix moyen
    gain = max(delta, 0) / price_avg
    loss = max(-delta, 0) / price_avg

    if loss == 0:
        return 100.0
    if gain == 0:
        return 0.0

    rs = gain / loss
    return round(100.0 - (100.0 / (1.0 + rs)), 4)


# ═══════════════════════════════════════════════════════════════════════════════
# UDFs SPARK
# ═══════════════════════════════════════════════════════════════════════════════

def vwap_col(sum_pq, sum_q):
    """Équivalent natif de vwap() — Column Spark (NULL si sum_q nul ou absent)."""
    from pyspark.sql import functions as F

    return F.when(
        sum_q.isNotNull() & (sum_q != 0) & sum_pq.isNotNull(),
        F.round(sum_pq / sum_q, 8),
    )


def bollinger_cols(avg, stddev, num_std: float = 2.0) -> Dict[str, object]:
    """Équivalent natif de bollinger_bands() — dict de Columns upper/middle/lower/bandwidth."""
    from pyspark.sql import functions as F

    valid = avg.isNotNull() & stddev.isNotNull()
    upper = F.round(avg + F.lit(num_std) * stddev, 8)
    lower = F.round(avg - F.lit(num_std) * stddev, 8)
    return {
        "upper":     F.when(valid, upper),
        "middle":    F.when(valid, F.round(avg, 8)),
        "lower":     F.when(valid, lower),
        "bandwidth": F.when(valid & (avg != 0), F.round((upper - lower) / avg, 8)),
    }


def rsi_approx_col(price_open, price_close, price_avg):
    """Équivalent natif de rsi_from_window_stats() — ne prend que les valeurs 0, 50 ou 100.

    Ce n'est qu'un indicateur de direction de la fenêtre ; le RSI précis (Wilder 14)
    est calculé par rsi_batch_job.py.
    """
    from pyspark.sql import functions as F

    known = price_open.isNotNull() & price_close.isNotNull() & price_avg.isNotNull()
    delta = price_close - price_open
    return (
        F.when(~known, F.lit(None).cast("double"))
        .when(delta == 0, F.lit(50.0))
        .when(delta > 0, F.lit(100.0))
        .otherwise(F.lit(0.0))
    )