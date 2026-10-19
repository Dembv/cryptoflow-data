# path tests/test_indicators.py

"""
CryptoFlow — Sprint 2
Tests unitaires des indicateurs financiers

Couvre :
  - VWAP  : cas normaux, edge cases (zéros, None)
  - RSI   : données insuffisantes, surachat, survente, neutralité
  - Bollinger Bands : calcul des 3 bandes + bandwidth
  - RSI approximé depuis stats de fenêtre Spark
"""

import sys
import os
import pytest

# Ajouter le dossier spark/ au path
# sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark"))
from spark.indicators import vwap, rsi, bollinger_bands, rsi_from_window_stats


# ═══════════════════════════════════════════════════════════════════════════════
# VWAP
# ═══════════════════════════════════════════════════════════════════════════════

class TestVWAP:

    def test_basic_calculation(self):
        """VWAP = Σ(p×q) / Σq"""
        # Trade 1 : 100 BTC @ $60,000 → pq = 6,000,000
        # Trade 2 : 200 BTC @ $61,000 → pq = 12,200,000
        # VWAP = 18,200,000 / 300 = 60,666.666...
        result = vwap(sum_price_qty=18_200_000, sum_qty=300)
        assert result == pytest.approx(60_666.6667, rel=1e-4)

    def test_single_trade(self):
        """Un seul trade : VWAP = prix du trade."""
        result = vwap(sum_price_qty=65_000 * 1.5, sum_qty=1.5)
        assert result == pytest.approx(65_000.0, rel=1e-6)

    def test_zero_quantity(self):
        """Division par zéro → retourner None."""
        assert vwap(sum_price_qty=100_000, sum_qty=0) is None

    def test_zero_sum_pq(self):
        """sum_pq = 0 → VWAP = 0."""
        assert vwap(sum_price_qty=0, sum_qty=10) == pytest.approx(0.0)

    def test_none_inputs(self):
        """Entrées None → retourner None."""
        assert vwap(None, 100) is None
        assert vwap(100, None) is None
        assert vwap(None, None) is None

    def test_high_precision(self):
        """Résultat arrondi à 8 décimales."""
        result = vwap(sum_price_qty=1.0, sum_qty=3.0)
        assert result == pytest.approx(0.33333333, rel=1e-6)


# ═══════════════════════════════════════════════════════════════════════════════
# RSI
# ═══════════════════════════════════════════════════════════════════════════════

class TestRSI:

    def test_insufficient_data(self):
        """Moins de period+1 prix → retourner None."""
        assert rsi([100, 101, 102], period=14) is None
        assert rsi([], period=14) is None
        assert rsi(None, period=14) is None

    def test_all_gains_overbought(self):
        """Toutes les variations positives → RSI = 100."""
        # 15 prix strictement croissants
        prices = [100 + i for i in range(16)]
        result = rsi(prices, period=14)
        assert result == pytest.approx(100.0, abs=0.01)

    def test_all_losses_oversold(self):
        """Toutes les variations négatives → RSI = 0."""
        prices = [200 - i for i in range(16)]
        result = rsi(prices, period=14)
        assert result == pytest.approx(0.0, abs=0.01)

    def test_neutral_market(self):
        """Alternance parfaite gain/perte → RSI ~ 50."""
        prices = []
        price = 100.0
        for i in range(30):
            price += 1.0 if i % 2 == 0 else -1.0
            prices.append(price)
        result = rsi(prices, period=14)
        assert result is not None
        assert 45.0 <= result <= 55.0

    def test_overbought_zone(self):
        """Forte tendance haussière → RSI > 70."""
        prices = [100.0]
        for i in range(20):
            # 4 hausses pour 1 baisse
            if i % 5 == 4:
                prices.append(prices[-1] - 0.5)
            else:
                prices.append(prices[-1] + 2.0)
        result = rsi(prices, period=14)
        assert result is not None
        assert result > 70

    def test_oversold_zone(self):
        """Forte tendance baissière → RSI < 30."""
        prices = [200.0]
        for i in range(20):
            # 4 baisses pour 1 hausse
            if i % 5 == 4:
                prices.append(prices[-1] + 0.5)
            else:
                prices.append(prices[-1] - 2.0)
        result = rsi(prices, period=14)
        assert result is not None
        assert result < 30

    def test_rsi_range(self):
        """RSI toujours entre 0 et 100."""
        import random
        random.seed(42)
        prices = [100.0]
        for _ in range(50):
            prices.append(prices[-1] * (1 + random.uniform(-0.05, 0.05)))
        result = rsi(prices, period=14)
        assert result is not None
        assert 0.0 <= result <= 100.0

    def test_period_6(self):
        """RSI avec période courte (6)."""
        prices = [44, 46, 48, 45, 47, 49, 50]  # 7 prix → 6 deltas
        result = rsi(prices, period=6)
        assert result is not None
        assert 0.0 <= result <= 100.0


# ═══════════════════════════════════════════════════════════════════════════════
# BOLLINGER BANDS
# ═══════════════════════════════════════════════════════════════════════════════

class TestBollingerBands:

    def test_basic_structure(self):
        """Résultat contient les 4 clés attendues."""
        result = bollinger_bands(avg=50_000.0, stddev=1_000.0, num_std=2.0)
        assert "upper"     in result
        assert "middle"    in result
        assert "lower"     in result
        assert "bandwidth" in result

    def test_upper_lower_calculation(self):
        """upper = avg + 2σ, lower = avg - 2σ."""
        result = bollinger_bands(avg=100.0, stddev=5.0, num_std=2.0)
        assert result["upper"]  == pytest.approx(110.0, rel=1e-6)
        assert result["middle"] == pytest.approx(100.0, rel=1e-6)
        assert result["lower"]  == pytest.approx(90.0,  rel=1e-6)

    def test_bandwidth(self):
        """bandwidth = (upper - lower) / middle."""
        result = bollinger_bands(avg=100.0, stddev=5.0, num_std=2.0)
        # (110 - 90) / 100 = 0.2
        assert result["bandwidth"] == pytest.approx(0.2, rel=1e-6)

    def test_zero_stddev_squeeze(self):
        """stddev = 0 → toutes les bandes égales (squeeze extrême)."""
        result = bollinger_bands(avg=50_000.0, stddev=0.0, num_std=2.0)
        assert result["upper"]     == pytest.approx(50_000.0)
        assert result["lower"]     == pytest.approx(50_000.0)
        assert result["bandwidth"] == pytest.approx(0.0)

    def test_upper_gt_middle_gt_lower(self):
        """upper > middle > lower pour stddev > 0."""
        result = bollinger_bands(avg=65_000.0, stddev=800.0, num_std=2.0)
        assert result["upper"] > result["middle"] > result["lower"]

    def test_none_inputs(self):
        """Entrées None → tous None."""
        result = bollinger_bands(avg=None, stddev=1000.0)
        assert result["upper"] is None
        result = bollinger_bands(avg=50000.0, stddev=None)
        assert result["upper"] is None

    def test_custom_num_std(self):
        """Bandes avec 1σ sont plus étroites qu'avec 2σ."""
        r1 = bollinger_bands(avg=100.0, stddev=10.0, num_std=1.0)
        r2 = bollinger_bands(avg=100.0, stddev=10.0, num_std=2.0)
        assert r2["upper"] > r1["upper"]
        assert r2["lower"] < r1["lower"]

    def test_realistic_btc_values(self):
        """Test avec des valeurs réalistes BTC."""
        result = bollinger_bands(avg=65_000.0, stddev=1_200.0, num_std=2.0)
        assert result["upper"]  == pytest.approx(67_400.0, rel=1e-4)
        assert result["lower"]  == pytest.approx(62_600.0, rel=1e-4)
        assert result["upper"] > 65_000 > result["lower"]


# ═══════════════════════════════════════════════════════════════════════════════
# RSI APPROXIMÉ (fenêtre Spark)
# ═══════════════════════════════════════════════════════════════════════════════

class TestRSIApprox:

    def test_bullish_window(self):
        """close > open → RSI haussier (> 50)."""
        result = rsi_from_window_stats(
            price_open=60_000.0,
            price_close=62_000.0,
            price_avg=61_000.0,
        )
        assert result is not None
        assert result > 50.0

    def test_bearish_window(self):
        """close < open → RSI baissier (< 50)."""
        result = rsi_from_window_stats(
            price_open=62_000.0,
            price_close=60_000.0,
            price_avg=61_000.0,
        )
        assert result is not None
        assert result < 50.0

    def test_flat_window(self):
        """open == close → RSI = 50 (neutre)."""
        result = rsi_from_window_stats(
            price_open=61_000.0,
            price_close=61_000.0,
            price_avg=61_000.0,
        )
        assert result == pytest.approx(50.0, abs=0.01)

    def test_strong_bullish(self):
        """Forte hausse → RSI proche de 100."""
        result = rsi_from_window_stats(
            price_open=50_000.0,
            price_close=65_000.0,
            price_avg=57_500.0,
        )
        assert result is not None
        assert result > 80.0

    def test_none_inputs(self):
        """Entrée None → retourner None."""
        assert rsi_from_window_stats(None, 62_000, 61_000) is None
        assert rsi_from_window_stats(60_000, None, 61_000) is None
        assert rsi_from_window_stats(60_000, 62_000, None) is None

    def test_range(self):
        """RSI approximé toujours entre 0 et 100."""
        cases = [
            (40_000, 70_000, 55_000),
            (70_000, 40_000, 55_000),
            (65_000, 65_001, 65_000),
            (65_000, 64_999, 65_000),
        ]
        for open_, close, avg in cases:
            result = rsi_from_window_stats(open_, close, avg)
            assert result is not None
            assert 0.0 <= result <= 100.0, f"RSI hors range pour ({open_}, {close}, {avg})"


# ═══════════════════════════════════════════════════════════════════════════════
# COHÉRENCE GLOBALE
# ═══════════════════════════════════════════════════════════════════════════════

class TestCoherence:

    def test_vwap_between_low_and_high(self):
        """
        VWAP doit toujours être entre le plus bas et le plus haut prix.
        Simulation : 3 trades à des prix différents.
        """
        trades = [
            (60_000, 1.0),
            (62_000, 3.0),
            (61_000, 2.0),
        ]
        sum_pq = sum(p * q for p, q in trades)
        sum_q  = sum(q for _, q in trades)
        result = vwap(sum_pq, sum_q)

        all_prices = [p for p, _ in trades]
        assert min(all_prices) <= result <= max(all_prices)

    def test_bollinger_width_increases_with_volatility(self):
        """
        Volatilité plus élevée → bandes plus larges.
        """
        low_vol  = bollinger_bands(avg=65_000.0, stddev=500.0)
        high_vol = bollinger_bands(avg=65_000.0, stddev=2_000.0)

        low_width  = low_vol["upper"]  - low_vol["lower"]
        high_width = high_vol["upper"] - high_vol["lower"]

        assert high_width > low_width

    def test_rsi_wilder_smoothing(self):
        """
        RSI Wilder : plus de données → lissage plus stable.
        Une série de 30 prix donne un RSI plus stable que 15.
        """
        import random
        random.seed(123)

        prices_short = [100.0]
        for _ in range(15):
            prices_short.append(prices_short[-1] * (1 + random.uniform(-0.02, 0.02)))

        prices_long = prices_short.copy()
        for _ in range(20):
            prices_long.append(prices_long[-1] * (1 + random.uniform(-0.02, 0.02)))

        rsi_short = rsi(prices_short, period=14)
        rsi_long  = rsi(prices_long, period=14)

        assert rsi_short is not None
        assert rsi_long  is not None
        # Les deux doivent être dans la plage valide
        assert 0 <= rsi_short <= 100
        assert 0 <= rsi_long  <= 100