FROM apache/airflow:2.8.1-python3.11

# dbt dans un venv dédié : évite les conflits de dépendances avec Airflow
# (PIP_USER=false : l'image de base force PIP_USER=true, incompatible avec un venv)
RUN python -m venv /home/airflow/dbt_venv \
 && PIP_USER=false /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core==1.7.4 dbt-postgres==1.7.4

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
