# path: airflow/dags/dag_dbt_hourly.py

"""
DAG principal CryptoFlow — dbt run + tests toutes les heures.

Flow :
  1. dbt deps        → installe les packages dbt
  2. dbt run staging → staging views
  3. dbt test src    → tests sources (not_null, accepted_values)
  4. dbt run marts   → tables agrégées
  5. dbt run gold    → tables Gold
  6. dbt test all    → tests qualité complets
  7. Slack notify    → résumé succès/échec
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from slack_alert import on_failure_callback, on_success_callback   # airflow/plugins

# ── Config ────────────────────────────────────────────────────────────────────

DBT_DIR        = "/opt/airflow/dbt"
DBT_BIN        = "/home/airflow/dbt_venv/bin/dbt"

DEFAULT_ARGS = {
    "owner":               "cryptoflow",
    "depends_on_past":     False,
    "start_date":          datetime(2024, 1, 1),
    "retries":             2,
    "retry_delay":         timedelta(minutes=5),
    "email_on_failure":    False,
    "on_failure_callback": on_failure_callback,
}

DBT_CMD = f"cd {DBT_DIR} && {DBT_BIN}"     # profiles.yml via DBT_PROFILES_DIR

# ── Slack helper ──────────────────────────────────────────────────────────────

def notify_success(**context):
    on_success_callback(context)

# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id          = "cryptoflow_dbt_hourly",
    default_args    = DEFAULT_ARGS,
    description     = "dbt run + tests CryptoFlow — cadence horaire",
    schedule        = "@hourly",
    catchup         = False,
    max_active_runs = 1,
    tags            = ["cryptoflow", "dbt", "transform"],
) as dag:

    # ── 1. dbt deps ─────────────────────────────────────────────
    dbt_deps = BashOperator(
        task_id         = "dbt_deps",
        bash_command    = f"{DBT_CMD} deps",    )

    # ── 2. Staging views ─────────────────────────────────────────
    dbt_staging = BashOperator(
        task_id         = "dbt_run_staging",
        bash_command    = f"{DBT_CMD} run --select staging",    )

    # ── 3. Tests sources ─────────────────────────────────────────
    dbt_test_sources = BashOperator(
        task_id         = "dbt_test_sources",
        bash_command    = f"{DBT_CMD} test --select source:timescale",    )

    # ── 4. Marts tables ──────────────────────────────────────────
    dbt_marts = BashOperator(
        task_id         = "dbt_run_marts",
        bash_command    = f"{DBT_CMD} run --select marts",    )

    # ── 5. Gold tables ───────────────────────────────────────────
    dbt_gold = BashOperator(
        task_id         = "dbt_run_gold",
        bash_command    = f"{DBT_CMD} run --select gold",    )

    # ── 6. Tests qualité complets ─────────────────────────────────
    dbt_test_all = BashOperator(
        task_id         = "dbt_test_all",
        bash_command    = f"{DBT_CMD} test",    )

    # ── 7. Notify succès ─────────────────────────────────────────
    notify_success = PythonOperator(
        task_id             = "notify_slack_success",
        python_callable     = notify_success,
        trigger_rule        = TriggerRule.ALL_SUCCESS,
    )

    # ── DAG ordering ─────────────────────────────────────────────
    (
        dbt_deps
        >> dbt_staging
        >> dbt_test_sources
        >> dbt_marts
        >> dbt_gold
        >> dbt_test_all
        >> notify_success
    )