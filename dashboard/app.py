# path: dashboard/app.py
"""Dashboard CryptoFlow — consomme l'API v1. Chaque zone se rafraîchit seule (st.fragment)."""

import os

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

API = os.getenv("API_URL", "http://localhost:8000").rstrip("/")
HEADERS = {"X-API-Key": os.environ["API_KEY"]} if os.getenv("API_KEY") else {}
ASSETS = ["BTC", "ETH", "SOL", "BNB", "XRP"]
WINDOWS = ["1 minute", "5 minutes", "1 hour"]
STATUS_ICON = {"ok": "🟢", "degraded": "🟠", "down": "🔴"}
CORR_KEYS = {
    ("BTC", "ETH"): "btc_eth", ("BTC", "SOL"): "btc_sol", ("BTC", "BNB"): "btc_bnb", ("BTC", "XRP"): "btc_xrp",
    ("ETH", "SOL"): "eth_sol", ("ETH", "BNB"): "eth_bnb", ("ETH", "XRP"): "eth_xrp",
    ("SOL", "BNB"): "sol_bnb", ("SOL", "XRP"): "sol_xrp", ("BNB", "XRP"): "bnb_xrp",
}

st.set_page_config(page_title="CryptoFlow", page_icon="📈", layout="wide")


@st.cache_data(ttl=2, show_spinner=False)
def api(path: str, **params):
    r = requests.get(f"{API}/v1{path}", params=params, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()


def safe(path: str, default=None, **params):
    """Appel d'API qui ne fait jamais planter la page (503 tant que dbt n'a pas tourné, etc.)."""
    try:
        return api(path, **params)
    except requests.RequestException:
        return default


def csv_download(label: str, path: str, filename: str, **params) -> None:
    try:
        r = requests.get(f"{API}/v1{path}", params={**params, "format": "csv"}, headers=HEADERS, timeout=10)
        r.raise_for_status()
        st.download_button(label, r.content, file_name=filename, mime="text/csv")
    except requests.RequestException:
        st.caption("Export indisponible")


with st.sidebar:
    st.header("CryptoFlow")
    symbol = st.selectbox("Actif", ASSETS)
    window = st.selectbox("Fenêtre des indicateurs", WINDOWS, index=1)
    refresh = st.slider("Rafraîchissement des graphiques (s)", 5, 60, 15)
    min_eth = st.slider("Seuil whale (ETH)", 100, 5000, 500, step=100)


@st.fragment(run_every=10)
def health_badge():
    status = safe("/status")
    if not status:
        st.error("API injoignable")
        return
    st.markdown(f"{STATUS_ICON.get(status['status'], '⚪')} **Plateforme : {status['status']}**")
    for name, info in status["freshness"].items():
        age = info["age_seconds"]
        label = "aucune donnée" if age is None else f"{age:.0f} s"
        st.caption(f"{name} : {label} ({info['status']})")
    if status.get("trades_last_minute") is not None:
        st.caption(f"{status['trades_last_minute']} trades / min")


with st.sidebar:
    health_badge()


@st.fragment(run_every=2)
def live_prices():
    prices = safe("/prices", {})
    summary = pd.DataFrame(safe("/market/summary", []))
    if not summary.empty:
        summary = summary.set_index("symbol")
    for col, sym in zip(st.columns(len(ASSETS)), ASSETS):
        p = prices.get(sym)
        delta = None
        if p and sym in summary.index and pd.notna(summary.loc[sym].get("pct_change_24h")):
            delta = f"{summary.loc[sym, 'pct_change_24h']:+.2f}%"
        col.metric(sym, f"{p['price']:,.2f}" if p else "—", delta)


st.title("CryptoFlow — marché crypto temps réel")
live_prices()

tab_market, tab_corr, tab_whales, tab_health = st.tabs(["Marché", "Corrélations", "Whales", "Santé"])

with tab_market:

    @st.fragment(run_every=refresh)
    def market_charts():
        ohlcv = pd.DataFrame(safe("/ohlcv/" + symbol, {}, limit=200).get("items", []))
        ind = pd.DataFrame(safe("/indicators/" + symbol, {}, window=window, limit=200).get("items", []))
        left, right = st.columns(2)

        with left:
            st.subheader(f"{symbol} — bougies 5 min")
            if ohlcv.empty:
                st.info("Pas encore de bougies (backfill Binance ou quelques minutes de trades).")
            else:
                fig = go.Figure(go.Candlestick(
                    x=ohlcv["bucket"], open=ohlcv["open"], high=ohlcv["high"],
                    low=ohlcv["low"], close=ohlcv["close"]))
                fig.update_layout(xaxis_rangeslider_visible=False, height=380, margin=dict(t=10))
                st.plotly_chart(fig, use_container_width=True)

        with right:
            st.subheader(f"{symbol} — Bollinger & RSI ({window})")
            if ind.empty:
                st.info("Pas encore d'indicateurs (job Spark non lancé ?).")
            else:
                ind = ind.sort_values("window_start")
                fig = go.Figure()
                for c in ("bb_upper", "bb_middle", "bb_lower", "vwap"):
                    fig.add_trace(go.Scatter(x=ind["window_start"], y=ind[c], name=c))
                fig.update_layout(height=250, margin=dict(t=10))
                st.plotly_chart(fig, use_container_width=True)
                rsi = go.Figure(go.Scatter(x=ind["window_start"], y=ind["rsi"], name="RSI"))
                rsi.add_hline(y=70, line_dash="dot")
                rsi.add_hline(y=30, line_dash="dot")
                rsi.update_layout(height=150, margin=dict(t=10), yaxis_range=[0, 100])
                st.plotly_chart(rsi, use_container_width=True)

    market_charts()
    c1, c2, _ = st.columns([1, 1, 4])
    with c1:
        csv_download("Exporter les bougies", f"/ohlcv/{symbol}", f"{symbol}_ohlcv_5m.csv", limit=2000)
    with c2:
        csv_download("Exporter les indicateurs", f"/indicators/{symbol}", f"{symbol}_indicators.csv",
                     window=window, limit=1000)

with tab_corr:
    corr_window = st.radio("Fenêtre", ["7d", "30d"], horizontal=True)
    corr = safe("/market/correlations", None, window=corr_window)
    if not corr:
        st.info("Corrélations indisponibles (dbt pas encore exécuté ?).")
    else:
        z = [[1.0 if a == b else corr.get(CORR_KEYS.get((a, b)) or CORR_KEYS.get((b, a))) for b in ASSETS]
             for a in ASSETS]
        fig = go.Figure(go.Heatmap(
            z=z, x=ASSETS, y=ASSETS, zmin=-1, zmax=1, colorscale="RdBu",
            text=[[f"{v:.2f}" if v is not None else "" for v in row] for row in z], texttemplate="%{text}"))
        fig.update_layout(height=450, margin=dict(t=10))
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"Corrélation des log-returns journaliers — calculée le {corr.get('computed_date')}")

with tab_whales:

    @st.fragment(run_every=30)
    def whales():
        recent = pd.DataFrame(safe("/whales/recent", [], min_eth=min_eth, limit=100))
        st.subheader(f"Transactions ≥ {min_eth:,} ETH")
        if recent.empty:
            st.info("Aucune transaction à ce seuil.")
        else:
            st.dataframe(recent, use_container_width=True, hide_index=True)
        daily = pd.DataFrame(safe("/whales/daily", [], min_eth=min_eth, limit=50))
        if not daily.empty:
            st.subheader("Activité quotidienne")
            st.dataframe(daily, use_container_width=True, hide_index=True)

    whales()

with tab_health:

    @st.fragment(run_every=10)
    def health_detail():
        status = safe("/status")
        if not status:
            st.error("API injoignable")
            return
        st.json({k: status[k] for k in ("status", "generated_at", "services", "freshness")})
        lag = [
            {"groupe": g, "topic": t, "retard": n}
            for g, info in status.get("kafka", {}).items()
            for t, n in info.get("lag", {}).items()
        ]
        st.subheader("Retard des consumers Kafka")
        if lag:
            st.dataframe(pd.DataFrame(lag), use_container_width=True, hide_index=True)
        else:
            st.caption("Aucun retard mesuré (ou Kafka injoignable depuis l'API).")

    health_detail()
