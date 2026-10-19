# CryptoFlow — raccourcis. `make help` liste les cibles.
#   make start        → menu : choisir le mode (minimal / light / full), puis démarrer
#   make up MODE=light → sans menu. Le mode choisi est mémorisé dans .mode
#     minimal : Kafka x1, Zookeeper, TimescaleDB, Redis (sans Spark, MinIO, Kafka UI)
#     light   : minimal + Spark, MinIO, Kafka UI (1 broker, sans Schema Registry)
#     full    : cluster complet (3 brokers, Schema Registry...) — défaut

.DEFAULT_GOAL := help
-include .mode
MODE     ?= full
LIGHT    ?= 0
PY       ?= python
RATE     ?= 200
DURATION ?= 60

ifeq ($(LIGHT),1)
MODE := light
endif

ifeq ($(filter $(MODE),minimal light full),)
$(error MODE invalide : '$(MODE)' (attendu : minimal, light ou full))
endif

ifeq ($(MODE),full)
COMPOSE := docker compose
else
COMPOSE := docker compose -f docker-compose.yml -f docker-compose.light.yml
ifeq ($(MODE),minimal)
COMPOSE += -f docker-compose.minimal.yml
endif
export KAFKA_BOOTSTRAP   := broker-1:29092
export KAFKA_REPLICATION := 1
endif
AIRFLOW := docker compose -f airflow/docker-compose.airflow.yml

.PHONY: help start setup up app streaming delta migrate airflow-init airflow down logs ps test lint load status config-check

help: ## Affiche cette aide
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-14s %s\n", $$1, $$2}'

start: ## Menu de choix du mode (ou MODE=...), puis démarre infra + application
	@$(PY) tools/choose_mode.py $(if $(filter command line,$(origin MODE)),$(MODE))
	@$(MAKE) --no-print-directory up app

setup: ## Crée config/.env et installe les dépendances de dev
	@$(PY) -c "import os, shutil; os.path.exists('config/.env') or shutil.copy('config/.env.example', 'config/.env')"
	$(PY) -m pip install -r requirements-dev.txt

up: ## Infra : Kafka, Spark, MinIO, TimescaleDB, Redis
	$(COMPOSE) up -d

app: ## Producers, sinks, alertes, API (:8000), dashboard (:8501)
	$(COMPOSE) --profile app up -d --build

streaming: ## Job Spark Streaming (indicateurs temps réel)
ifeq ($(MODE),minimal)
	$(error 'streaming' exige Spark : utilisez MODE=light ou MODE=full)
endif
	$(COMPOSE) --profile streaming up -d spark-submit

delta: ## Archivage Delta Lake
ifeq ($(MODE),minimal)
	$(error 'delta' exige Spark et MinIO : utilisez MODE=light ou MODE=full)
endif
	$(COMPOSE) --profile delta up -d spark-submit-delta

migrate: ## Applique les migrations TimescaleDB sur un volume existant
	docker cp spark/init_timescale_policies.sql cryptoflow-timescaledb:/tmp/policies.sql
	docker exec cryptoflow-timescaledb psql -U crypto -d cryptoflow -v ON_ERROR_STOP=1 -f /tmp/policies.sql

airflow-init: ## Initialise Airflow (base + utilisateur admin)
	$(AIRFLOW) up airflow-init

airflow: ## Démarre Airflow (:8085)
	$(AIRFLOW) up -d --build

down: ## Arrête tout (les volumes sont conservés)
	$(AIRFLOW) down
	$(COMPOSE) --profile app --profile streaming --profile delta --profile full down

logs: ## Logs d'un service : make logs SERVICE=timescale-sink
	$(COMPOSE) --profile app logs -f --tail=100 $(SERVICE)

ps: ## État des conteneurs
	$(COMPOSE) --profile app ps

test: ## Tests unitaires
	$(PY) -m pytest -q

lint: ## Lint (erreurs critiques uniquement)
	$(PY) -m ruff check .

load: ## Charge synthétique : make load RATE=500 DURATION=60
	$(PY) tools/load_generator.py --rate $(RATE) --duration $(DURATION)

status: ## Santé de l'API et fraîcheur des données (API_KEY=... si activée)
	@curl -s http://localhost:8000/health; echo
	@curl -s -H "X-API-Key: $(API_KEY)" http://localhost:8000/v1/status; echo

config-check: ## Valide la syntaxe des fichiers compose
	$(COMPOSE) --profile app config -q
	$(AIRFLOW) config -q
