# path: airflow/dags/dag_rsi_batch.py

"""
DAG RSI batch précis — toutes les 5 minutes.

Lance le rsi_batch_job.py qui calcule le RSI Wilder(14)
depuis l'historique TimescaleDB et publie dans crypto.indicators.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from slack_alert import on_failure_callback   # airflow/plugins

DEFAULT_ARGS = {
    "owner":           "cryptoflow",
    "depends_on_past": False,
    "start_date":      datetime(2024, 1, 1),
    "retries":         1,
    "retry_delay":     timedelta(minutes=1),
    "on_failure_callback": on_failure_callback,
}

RSI_JOB_PATH = os.getenv("RSI_JOB_PATH", "/opt/airflow/jobs/rsi_batch_job.py")

with DAG(
    dag_id          = "cryptoflow_rsi_batch",
    default_args    = DEFAULT_ARGS,
    description     = "RSI précis Wilder(14) toutes les 5 min → crypto.indicators",
    schedule        = "*/5 * * * *",
    catchup         = False,
    max_active_runs = 1,
    tags            = ["cryptoflow", "indicators", "batch"],
) as dag:

    run_rsi = BashOperator(
        task_id      = "run_rsi_batch_job",
        bash_command = f"python {RSI_JOB_PATH}",
        append_env   = True,      # sinon env= écrase PATH et le reste de l'environnement
        env          = {
            "KAFKA_BOOTSTRAP_SERVERS": os.getenv(
                "CRYPTOFLOW_KAFKA_SERVERS", "localhost:9092"
            ),
            "TIMESCALE_DSN": os.getenv(
                "CRYPTOFLOW_TIMESCALE_DSN",
                "postgresql://crypto:crypto@localhost:5433/cryptoflow"
            ),
        },
    )