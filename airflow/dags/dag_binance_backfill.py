# path: airflow/dags/dag_binance_backfill.py

"""
DAG de backfill des bougies 5 min depuis l'API publique Binance (klines).

Tourne chaque jour à 02:00 UTC. Premier passage : BACKFILL_DAYS jours (30 par défaut) ;
ensuite il reprend à la dernière bougie connue de chaque asset (idempotent).
Remplace l'ancien DAG CoinGecko (bougies trop grossières pour un RSI).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from binance_klines import backfill_symbol            # airflow/plugins
from slack_alert import on_failure_callback, on_success_callback

log = logging.getLogger(__name__)

TIMESCALE_DSN = os.getenv("CRYPTOFLOW_TIMESCALE_DSN", "postgresql://crypto:crypto@localhost:5433/cryptoflow")
ASSETS = [a.strip().upper() for a in os.getenv("CRYPTOFLOW_ASSETS", "BTC,ETH,SOL,BNB,XRP").split(",") if a.strip()]
BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", 30))

DEFAULT_ARGS = {
    "owner":               "cryptoflow",
    "depends_on_past":     False,
    "start_date":          datetime(2024, 1, 1),
    "retries":             3,
    "retry_delay":         timedelta(minutes=10),
    "on_failure_callback": on_failure_callback,
}


def fetch_and_store(symbol: str, days: int = BACKFILL_DAYS, **_):
    import psycopg2

    conn = psycopg2.connect(TIMESCALE_DSN)
    try:
        written = backfill_symbol(conn, symbol, days)
    finally:
        conn.close()
    log.info("%s: %d bougies 5 min écrites.", symbol, written)
    return written


def validate_backfill(**_):
    """Chaque asset doit avoir des bougies, et la plus récente doit dater de moins de 24 h."""
    import psycopg2

    errors = []
    conn = psycopg2.connect(TIMESCALE_DSN)
    try:
        with conn.cursor() as cur:
            for symbol in ASSETS:
                cur.execute("SELECT count(*), max(bucket) FROM klines_5m WHERE symbol = %s", (symbol,))
                count, newest = cur.fetchone()
                if not count:
                    errors.append(f"{symbol}: aucune donnée")
                elif datetime.now(timezone.utc) - newest > timedelta(hours=24):
                    errors.append(f"{symbol}: dernière bougie trop ancienne ({newest})")
                else:
                    log.info("%s: %d bougies, dernière %s", symbol, count, newest)
    finally:
        conn.close()
    if errors:
        raise ValueError(f"Validation du backfill échouée : {errors}")


def notify(**context):
    on_success_callback(context)


with DAG(
    dag_id          = "cryptoflow_binance_backfill",
    default_args    = DEFAULT_ARGS,
    description     = "Backfill bougies 5 min Binance → klines_5m (TimescaleDB)",
    schedule        = "0 2 * * *",
    catchup         = False,
    max_active_runs = 1,
    tags            = ["cryptoflow", "ingestion", "backfill"],
) as dag:

    fetch_tasks = [
        PythonOperator(
            task_id         = f"fetch_klines_{sym.lower()}",
            python_callable = fetch_and_store,
            op_kwargs       = {"symbol": sym},
        )
        for sym in ASSETS
    ]

    validate = PythonOperator(task_id="validate_backfill", python_callable=validate_backfill)
    done = PythonOperator(task_id="notify", python_callable=notify, trigger_rule=TriggerRule.ALL_SUCCESS)

    fetch_tasks >> validate >> done
