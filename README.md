# CryptoFlow

Plateforme data de bout en bout pour le suivi du marché crypto : ingestion temps réel (Kafka), calcul
d'indicateurs (Spark Structured Streaming), stockage (TimescaleDB, Redis, Delta Lake), transformations
(dbt), orchestration (Airflow), exposition (API FastAPI + WebSocket), visualisation (Streamlit) et alertes
(Slack / Telegram).

> Guide pas à pas pour utiliser la plateforme au quotidien : [docs/MANUEL_UTILISATEUR.md](docs/MANUEL_UTILISATEUR.md).

## Sommaire

1. [Fonctionnalités](#fonctionnalités)
2. [Architecture](#architecture)
3. [Stack technique](#stack-technique)
4. [Structure du projet](#structure-du-projet)
5. [Prérequis](#prérequis)
6. [Modes de démarrage](#modes-de-démarrage)
7. [Démarrage rapide](#démarrage-rapide)
8. [Configuration](#configuration)
9. [Services et ports](#services-et-ports)
10. [Topics Kafka et formats de messages](#topics-kafka-et-formats-de-messages)
11. [Indicateurs calculés](#indicateurs-calculés)
12. [Modèle de données TimescaleDB](#modèle-de-données-timescaledb)
13. [Transformations dbt](#transformations-dbt)
14. [Orchestration Airflow](#orchestration-airflow)
15. [API](#api)
16. [Dashboard](#dashboard)
17. [Alertes](#alertes)
18. [Commandes utiles (Makefile)](#commandes-utiles-makefile)
19. [Tests, lint et CI](#tests-lint-et-ci)
20. [Mesure de performance](#mesure-de-performance)
21. [Exploitation et dépannage](#exploitation-et-dépannage)

---

## Fonctionnalités

- **Ingestion multi-sources** : trades Binance en temps réel (WebSocket), OHLCV CoinGecko (polling 60 s),
  transactions ETH et transferts de tokens Etherscan (polling 30 s).
- **Indicateurs temps réel** : VWAP, RSI approximatif, bandes de Bollinger, ratio acheteurs/vendeurs sur
  des fenêtres de 1 minute, 5 minutes et 1 heure.
- **RSI précis** (lissage de Wilder, période 14) calculé en batch toutes les 5 minutes.
- **Stockage multi-couches** : TimescaleDB (hypertables, agrégats continus, compression, rétention),
  Redis (dernier prix), Delta Lake sur MinIO (archivage brut).
- **Modèles analytiques dbt** : OHLCV quotidien, indicateurs horaires, matrice de corrélation, résumé de
  marché, activité des baleines, avec tests de qualité.
- **API REST + WebSocket** : prix, bougies, indicateurs, on-chain, statut du pipeline ; pagination, export
  CSV, authentification par clé, limitation de débit, cache.
- **Dashboard Streamlit** : bougies 5 min, Bollinger/RSI, corrélations, baleines, santé du pipeline.
- **Alertes** : RSI en zone extrême, squeeze de Bollinger, gros transferts ETH, avec anti-doublon.
- **Observabilité** : healthchecks Docker, fraîcheur des données par source, retard des consumers Kafka.

## Architecture

```
Binance WebSocket ───────► crypto.prices ──┬──► Spark Streaming ──► crypto.indicators ──┬──► timescale-sink ──► TimescaleDB
CoinGecko API (60 s) ────► crypto.history   │                                           │                          │
Etherscan API (30 s) ────► crypto.onchain ──┼──► timescale-sink ───────────────────────┘                          ├──► dbt (Airflow)
                                            ├──► redis-sink ──► Redis (dernier prix, TTL 5 s)                     │
                                            ├──► Spark Delta ──► MinIO (Delta Lake)                               ▼
                                            └──► alert-service ──► Slack / Telegram                      API FastAPI ──► Dashboard
                                                                                                          (REST + WebSocket)
```

Flux principaux :

| Flux | Chemin |
|---|---|
| Temps réel | Binance → `crypto.prices` → Spark → `crypto.indicators` → TimescaleDB → API → Dashboard |
| Dernier prix | `crypto.prices` → `redis-sink` → Redis → API (`/v1/prices`, `/v1/ws/prices`) |
| Analytique | TimescaleDB → dbt (toutes les heures, via Airflow) → tables `marts` / `gold` → API |
| Historique | Airflow (backfill Binance) → `klines_5m` → vue `candles_5m` |
| Archivage | `crypto.prices` → Spark Delta → MinIO (`s3a://cryptoflow`) |
| Alertes | `crypto.indicators` + `crypto.onchain` → `alert-service` → Slack / Telegram |

## Stack technique

| Couche | Technologie |
|---|---|
| Messagerie | Apache Kafka 3 brokers (Confluent 7.5), Zookeeper, Schema Registry, Kafka UI |
| Traitement | Apache Spark 3.5 (Structured Streaming + batch) |
| Stockage | TimescaleDB (PostgreSQL 16), Redis 7, MinIO + Delta Lake |
| Transformation | dbt (dbt-postgres 1.7) |
| Orchestration | Apache Airflow |
| API | FastAPI, Uvicorn, WebSocket |
| Dashboard | Streamlit, Plotly |
| Conteneurisation | Docker Compose |
| Qualité | pytest, ruff, GitHub Actions |
| Langage | Python 3.11 |

## Structure du projet

```
cryptoflow-data/
├── docker-compose.yml            # Infra + services applicatifs (profils app / streaming / delta)
├── docker-compose.light.yml      # Surcharge légère : 1 broker Kafka, sans Schema Registry
├── docker-compose.minimal.yml    # Surcharge minimale (avec light) : sans Spark, MinIO, Kafka UI
├── Dockerfile.app                # Image commune producers / sinks / alertes / API / dashboard
├── Makefile                      # Raccourcis (make help)
├── pyproject.toml                # Configuration pytest / ruff
├── requirements.txt              # Dépendances runtime
├── requirements-dev.txt          # Dépendances de test (pytest, pyspark, ruff)
├── config/
│   └── .env.example              # Modèle de configuration
├── producers/
│   ├── binance_producer.py       # WebSocket temps réel → crypto.prices
│   ├── coingecko_producer.py     # OHLCV → crypto.history
│   └── etherscan_producer.py     # Transactions on-chain → crypto.onchain
├── consumers/
│   └── test_consumer.py          # Consumer de validation (--topic / --all)
├── spark/
│   ├── streaming_job.py          # Indicateurs temps réel → crypto.indicators
│   ├── indicators.py             # Fonctions d'indicateurs (VWAP, RSI, Bollinger)
│   ├── rsi_batch_job.py          # RSI précis (Wilder 14), exécuté par Airflow
│   ├── delta_sink_job.py         # Archivage Delta Lake sur MinIO
│   ├── timescale_sink.py         # Kafka → TimescaleDB
│   ├── redis_sink.py             # Kafka → Redis
│   ├── init_timescale.sql        # Schéma initial (hypertables)
│   └── init_timescale_policies.sql  # Agrégats continus, klines, compression, rétention
├── alerts/
│   ├── service.py                # Boucle de consommation Kafka
│   ├── rules.py                  # Règles RSI / squeeze / whale
│   └── notifiers.py              # Slack, Telegram
├── api/
│   ├── main.py                   # Routes FastAPI, WebSocket, middlewares
│   ├── schemas.py                # Modèles Pydantic
│   └── settings.py               # Configuration de l'API
├── dashboard/
│   └── app.py                    # Application Streamlit
├── common/
│   ├── heartbeat.py              # Healthcheck des services sans port HTTP
│   ├── kafka_lag.py              # Calcul du retard des consumers
│   └── jsonutil.py               # Utilitaires JSON
├── dbt/
│   ├── models/{staging,marts,gold}/
│   ├── macros/generate_schema_name.sql
│   └── tests/                    # Tests de données personnalisés
├── airflow/
│   ├── docker-compose.airflow.yml
│   ├── Dockerfile / requirements.txt
│   ├── dags/                     # rsi_batch, dbt_hourly, binance_backfill
│   └── plugins/                  # Client klines Binance, alertes Slack
├── tools/
│   └── load_generator.py         # Générateur de charge synthétique
├── tests/                        # Tests unitaires
└── .github/workflows/ci.yml      # Lint, tests, validation compose, parse dbt
```

## Prérequis

- Docker et Docker Compose v2
- Python 3.11 (uniquement pour les tests et outils locaux)
- `make` (optionnel, sous Windows via Git Bash ou WSL)
- Une clé API [Etherscan](https://etherscan.io/apis) gratuite (CoinGecko et Binance ne demandent pas de clé)
- Ressources conseillées : 8 Go de RAM pour le mode `full` ; les modes `light` et `minimal` réduisent
  l'empreinte (voir [Modes de démarrage](#modes-de-démarrage))

## Modes de démarrage

Trois modes selon les ressources de la machine :

| Mode | Services démarrés | Services retirés |
|---|---|---|
| `minimal` | Zookeeper, 1 broker Kafka, TimescaleDB, Redis (+ profil `app`) | Spark, MinIO, Kafka UI, brokers 2 et 3, Schema Registry |
| `light` | `minimal` + Spark, MinIO, Kafka UI | Brokers 2 et 3, Schema Registry |
| `full` (défaut) | Cluster complet : 3 brokers, Schema Registry, Spark, MinIO, Kafka UI… | Rien |

```bash
make start                  # menu interactif : choisir le mode, puis démarrer infra + application
make start MODE=minimal     # sans menu
make up app MODE=light      # démarrage seul
```

Le mode choisi est mémorisé dans `.mode` (non versionné) et réutilisé par les commandes `make` suivantes
(`ps`, `logs`, `down`…). `MODE=...` en ligne de commande l'emporte sur `.mode` ; `LIGHT=1` reste un alias de
`MODE=light`. Pour changer de mode : `make down`, puis `make start`.

En mode `minimal`, `make streaming` et `make delta` sont refusés (Spark et MinIO ne sont pas démarrés).
Sans `make`, combiner les fichiers compose à la main :

```bash
# light
KAFKA_BOOTSTRAP=broker-1:29092 KAFKA_REPLICATION=1   docker compose -f docker-compose.yml -f docker-compose.light.yml up -d
# minimal
KAFKA_BOOTSTRAP=broker-1:29092 KAFKA_REPLICATION=1   docker compose -f docker-compose.yml -f docker-compose.light.yml -f docker-compose.minimal.yml up -d
```

## Démarrage rapide

```bash
# 1. Configuration
cp config/.env.example config/.env        # puis renseigner ETHERSCAN_API_KEY

# 2. Infrastructure : Kafka, Spark, MinIO, TimescaleDB, Redis
docker compose up -d

# 3. Services applicatifs : producers, sinks, alertes, API, dashboard
docker compose --profile app up -d --build

# 4. Indicateurs temps réel (Spark Streaming)
docker compose --profile streaming up -d spark-submit

# 5. (optionnel) Archivage Delta Lake
docker compose --profile delta up -d spark-submit-delta

# 6. Airflow (le réseau cryptoflow-network doit exister : compose racine démarré avant)
docker compose -f airflow/docker-compose.airflow.yml up airflow-init
docker compose -f airflow/docker-compose.airflow.yml up -d --build
```

Équivalent avec le Makefile :

```bash
make setup
make up app streaming        # MODE=light ou MODE=minimal pour alléger (voir ci-dessus)
make airflow-init airflow
```

Vérifier ensuite :

```bash
make status                                           # santé de l'API + fraîcheur des données
docker exec cryptoflow-broker-1 kafka-topics \
  --bootstrap-server localhost:9092 --list            # 4 topics attendus
```

Dans Airflow (http://localhost:8085), activer les 3 DAGs.

### Sur un volume TimescaleDB existant

Les scripts d'initialisation ne s'exécutent qu'à la création du volume. Pour appliquer les évolutions
(colonne `indicators.window_start`, table `klines_5m`, vue `candles_5m`, politiques de compression et de
rétention) sur un volume déjà peuplé, lancer une fois (idempotent) :

```bash
make migrate
```

ou manuellement :

```bash
docker cp spark/init_timescale_policies.sql cryptoflow-timescaledb:/tmp/policies.sql
docker exec cryptoflow-timescaledb psql -U crypto -d cryptoflow -v ON_ERROR_STOP=1 -f /tmp/policies.sql
```

### Arrêt

```bash
make down       # arrête tout, les volumes sont conservés
```

## Configuration

Toute la configuration passe par `config/.env` (modèle : `config/.env.example`).

| Variable | Défaut | Rôle |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9093,localhost:9094` | Brokers (hors Docker) |
| `ASSETS` | `BTC,ETH,SOL,BNB,XRP` | Actifs suivis |
| `BINANCE_WS_URL` | `wss://stream.binance.com:9443/stream` | Flux Binance |
| `COINGECKO_FETCH_INTERVAL_SEC` | `60` | Période de polling CoinGecko |
| `ETHERSCAN_API_KEY` | — | **Requise** pour le producer on-chain |
| `ETHERSCAN_FETCH_INTERVAL_SEC` | `30` | Période de polling Etherscan |
| `KAFKA_ACKS`, `KAFKA_RETRIES`, `KAFKA_LINGER_MS`, `KAFKA_BATCH_SIZE` | `all`, `5`, `5`, `16384` | Réglages des producers |
| `TIMESCALE_HOST/PORT/DB/USER/PASSWORD` | `localhost` / `5433` / `cryptoflow` / `crypto` / `crypto` | TimescaleDB |
| `REDIS_HOST`, `REDIS_PORT`, `REDIS_TTL_SECONDS` | `localhost`, `6379`, `5` | Redis |
| `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `DELTA_LAKE_PATH` | MinIO local | Delta Lake |
| `API_KEY` | vide | Active l'authentification si renseignée |
| `CORS_ORIGINS` | `*` | Origines autorisées |
| `RATE_LIMIT_PER_MIN` | `120` | Débit max par IP (0 = illimité) |
| `CACHE_TTL_SUMMARY` | `10` | Durée de cache du résumé de marché (s) |
| `ALERT_RSI_OVERBOUGHT` / `ALERT_RSI_OVERSOLD` | `70` / `30` | Seuils RSI |
| `ALERT_SQUEEZE_BANDWIDTH` | `0.02` | Seuil de squeeze Bollinger |
| `ALERT_WHALE_ETH` | `500` | Seuil de transfert ETH |
| `ALERT_COOLDOWN_SEC` | `900` | Délai anti-doublon par règle et actif |
| `ALERT_SLACK_WEBHOOK`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` | vides | Canaux de notification |
| `ENABLE_CONSOLE_SINK` | `false` | Sortie console de debug dans Spark |

Dans Docker, les services applicatifs surchargent automatiquement les hôtes (`broker-1:29092`,
`timescaledb`, `redis`…) : le `.env` local reste utilisable tel quel pour les scripts lancés hors conteneur.

**Airflow** : `AIRFLOW_FERNET_KEY`, `AIRFLOW_SECRET_KEY` et `AIRFLOW_ADMIN_PASSWORD` se définissent dans
l'environnement ou dans un `.env` à côté de `airflow/docker-compose.airflow.yml`. Les valeurs par défaut ne
conviennent qu'au développement.

> **Sécurité** : les identifiants par défaut (`crypto/crypto`, `minioadmin/minioadmin`, `admin/admin`) sont
> destinés au développement local. Les changer, et renseigner `API_KEY`, avant toute exposition réseau.

## Services et ports

| Service | URL / port | Remarque |
|---|---|---|
| API (Swagger) | http://localhost:8000/docs | |
| Dashboard | http://localhost:8501 | |
| Kafka UI | http://localhost:8080 | |
| Spark UI | http://localhost:8090 | Worker : 8091 |
| Airflow | http://localhost:8085 | `admin` / `admin` par défaut |
| MinIO (console) | http://localhost:9001 | API S3 : 9000 |
| Schema Registry | http://localhost:8081 | |
| TimescaleDB | `localhost:5433` | base `cryptoflow`, utilisateur `crypto` |
| Redis | `localhost:6379` | |
| Kafka | `localhost:9092`, `9093`, `9094` | |

En modes `light` et `minimal`, Schema Registry et les brokers 2 et 3 ne sont pas démarrés ; en `minimal`,
Spark UI, MinIO et Kafka UI non plus. Profils Docker Compose : le compose racine démarre l'infrastructure ; `app`, `streaming` et `delta`
activent respectivement les services applicatifs, le job Spark Streaming et l'archivage Delta Lake.

## Topics Kafka et formats de messages

| Topic | Source | Partitions | Rétention | Contenu |
|---|---|---|---|---|
| `crypto.prices` | Binance WebSocket | 6 | 24 h | Trades temps réel (tick data) |
| `crypto.history` | CoinGecko | 3 | 7 j | OHLCV et snapshots de marché |
| `crypto.onchain` | Etherscan | 3 | 7 j | Transactions de baleines et transferts de tokens |
| `crypto.indicators` | Spark Streaming | 6 | 7 j | Indicateurs par fenêtre |

Les topics sont créés par le service `kafka-init` (création automatique désactivée sur les brokers).
Le facteur de réplication vaut 3 par défaut, 1 en modes `light` et `minimal`.

### `crypto.prices`

```json
{
  "event_type": "trade",
  "symbol": "BTC",
  "pair": "BTCUSDT",
  "price": 65432.10,
  "quantity": 0.00234,
  "trade_id": 3456789,
  "side": "buy",
  "trade_time": 1710000000000,
  "trade_time_iso": "2024-03-09T12:00:00+00:00",
  "ingested_at": "2024-03-09T12:00:00.123456+00:00",
  "source": "binance_ws"
}
```

### `crypto.history`

```json
{
  "event_type": "ohlcv",
  "symbol": "BTC",
  "coin_id": "bitcoin",
  "open": 65000.0,
  "high": 66100.0,
  "low": 64800.0,
  "close": 65432.1,
  "timestamp_iso": "2024-03-09T12:00:00+00:00",
  "source": "coingecko_ohlcv"
}
```

### `crypto.onchain`

```json
{
  "event_type": "eth_transaction",
  "symbol": "ETH",
  "tx_hash": "0xabc123...",
  "value_eth": 500.0,
  "from_address": "0xdef456...",
  "to_address": "0x789abc...",
  "block_number": 19400000,
  "is_whale": true,
  "source": "etherscan_api"
}
```

### Valider la réception

```bash
python consumers/test_consumer.py --topic crypto.prices    # un topic
python consumers/test_consumer.py --all                    # les trois topics en parallèle
```

## Indicateurs calculés

Le job `spark/streaming_job.py` agrège `crypto.prices` sur des fenêtres de **1 minute, 5 minutes et
1 heure** (watermark de 10 minutes) et publie sur `crypto.indicators` :

| Indicateur | Description |
|---|---|
| `vwap` | Prix moyen pondéré par les volumes |
| `rsi_approx` | RSI approximatif sur la fenêtre (ne vaut que 0, 50 ou 100, à ne pas utiliser seul) |
| `bb_upper`, `bb_middle`, `bb_lower` | Bandes de Bollinger |
| `bb_bandwidth` | Largeur relative des bandes (détection de squeeze) |
| `buy_ratio` | Part des trades acheteurs |
| `trade_count`, `volume` | Activité sur la fenêtre |

Le job batch `spark/rsi_batch_job.py` (DAG `cryptoflow_rsi_batch`, toutes les 5 minutes) calcule le
**RSI précis** (Wilder, période 14) sur les fenêtres 1 minute, 5 minutes et 1 heure. C'est la valeur à
utiliser pour l'analyse et les alertes.

**Ré-émission des fenêtres** : Spark réémet chaque fenêtre à chaque trigger. La lecture (API, dbt) ne
conserve que la dernière version par `(symbol, window_label, window_start)`.

## Modèle de données TimescaleDB

| Objet | Type | Rôle |
|---|---|---|
| `raw_trades` | hypertable | Trades Binance bruts (compression à 7 j, rétention 30 j) |
| `prices_5min` | hypertable | Prix agrégés par bucket de 5 minutes |
| `indicators` | hypertable | Indicateurs Spark et RSI précis du batch, dans la même colonne `rsi_approx` (rétention 90 j) |
| `onchain_events` | hypertable | Transactions et transferts on-chain |
| `ohlcv_5m` | agrégat continu | Bougies 5 minutes calculées depuis `raw_trades` |
| `klines_5m` | hypertable | Bougies 5 minutes historiques Binance (backfill) |
| `candles_5m` | vue | Bougies temps réel et historique réunies |

Schémas : `spark/init_timescale.sql` (tables) et `spark/init_timescale_policies.sql` (agrégats,
politiques, `klines_5m`).

## Transformations dbt

Projet dans `dbt/`, exécuté toutes les heures par Airflow.

| Couche | Modèles |
|---|---|
| `staging` | `stg_trades`, `stg_indicators`, `stg_onchain` (nettoyage, déduplication) |
| `marts` | `daily_ohlcv`, `hourly_indicators` (incrémentaux), `correlation_matrix` |
| `gold` | `market_summary`, `whale_activity` |

Tests de données personnalisés (`dbt/tests/`) : `assert_high_gte_low`, `assert_rsi_range`,
`assert_vwap_positive`. Packages : `dbt_date`, `dbt_expectations`.

`daily_ohlcv` et `hourly_indicators` recalculent uniquement les 2 derniers jours / 2 dernières heures.
Après une modification de logique :

```bash
cd dbt && dbt run --full-refresh --profiles-dir .
```

## Orchestration Airflow

| DAG | Planification | Rôle |
|---|---|---|
| `cryptoflow_rsi_batch` | `*/5 * * * *` | RSI précis (Wilder 14) vers `indicators` |
| `cryptoflow_dbt_hourly` | `@hourly` | `dbt run` puis `dbt test` |
| `cryptoflow_binance_backfill` | `0 2 * * *` | Bougies 5 min Binance vers `klines_5m` |

Le backfill charge 30 jours au premier passage (`BACKFILL_DAYS`), puis reprend à la dernière bougie
connue. Les échecs de DAG sont notifiés sur Slack via le plugin `airflow/plugins/slack_alert.py`.
Dans l'image Airflow, dbt est installé dans un environnement virtuel dédié.

## API

Documentation interactive : http://localhost:8000/docs

| Route | Contenu |
|---|---|
| `GET /health` | Santé Redis + TimescaleDB (sans authentification) |
| `GET /v1/prices`, `/v1/prices/{symbol}` | Dernier prix (Redis, repli sur TimescaleDB) |
| `GET /v1/market/summary` | Résumé de marché (table dbt, mis en cache `CACHE_TTL_SUMMARY` s) |
| `GET /v1/market/correlations?window=7d` | Matrice de corrélation |
| `GET /v1/ohlcv/{symbol}` | Bougies, paginées |
| `GET /v1/indicators/{symbol}` | Indicateurs, paginés |
| `GET /v1/whales/recent?min_eth=` | Transactions ETH récentes au-dessus d'un seuil |
| `GET /v1/whales/daily` | Activité quotidienne des baleines |
| `GET /v1/status` | Fraîcheur par source, trades/min, retard des consumers Kafka |
| `WS /v1/ws/prices` | Prix diffusés chaque seconde (une lecture partagée entre tous les clients) |

- **Pagination** : `limit`, `offset`, réponse avec `next_offset`.
- **Export** : `?format=csv` sur `/v1/ohlcv/{symbol}` et `/v1/indicators/{symbol}`.
- **Authentification** : si `API_KEY` est défini, envoyer l'en-tête `X-API-Key` (ou `?api_key=` pour le
  WebSocket).
- **Limitation de débit** : `RATE_LIMIT_PER_MIN` par IP.
- **CORS** : `CORS_ORIGINS`.

Exemples :

```bash
curl http://localhost:8000/v1/prices/BTC
curl -H "X-API-Key: $API_KEY" "http://localhost:8000/v1/ohlcv/ETH?limit=100&format=csv"
```

## Dashboard

Application Streamlit sur http://localhost:8501, alimentée uniquement par l'API :

| Onglet | Contenu |
|---|---|
| Marché | Bougies 5 min, bandes de Bollinger et RSI par fenêtre |
| Corrélations | Heatmap de corrélation entre actifs |
| Whales | Transactions ETH au-dessus d'un seuil, activité quotidienne |
| Santé | Fraîcheur des sources et retard des consumers Kafka |

## Alertes

Le service `alert-service` lit `crypto.indicators` et `crypto.onchain`.

| Règle | Déclencheur (réglable via `ALERT_*`) |
|---|---|
| RSI | RSI précis (Wilder 14, fenêtre 5 min) ≥ 70 ou ≤ 30 |
| Squeeze Bollinger | Bandwidth 1 h < 0,02 |
| Whale | Transaction ETH ≥ `ALERT_WHALE_ETH` (500) ; critique à 10× le seuil |

Canaux : `ALERT_SLACK_WEBHOOK` et/ou `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID`. Sans canal configuré, les
alertes sont uniquement écrites dans les logs. Une même alerte (règle + actif) n'est renvoyée qu'après
`ALERT_COOLDOWN_SEC` secondes.

## Commandes utiles (Makefile)

```bash
make help              # liste des cibles
make start             # menu de choix du mode (minimal / light / full), puis démarre tout
make setup             # config/.env + dépendances de dev
make up                # infrastructure
make app               # producers, sinks, alertes, API, dashboard
make streaming         # job Spark Streaming
make delta             # archivage Delta Lake
make migrate           # migrations TimescaleDB sur un volume existant
make airflow-init      # initialise Airflow
make airflow           # démarre Airflow
make logs SERVICE=timescale-sink
make ps                # état des conteneurs
make status            # santé de l'API et fraîcheur des données (API_KEY=... si activée)
make test              # tests unitaires
make lint              # ruff
make load RATE=500 DURATION=60
make config-check      # valide la syntaxe des fichiers compose
make down              # arrête tout (volumes conservés)
```

`MODE=light` (ex. `make up app streaming MODE=light`) utilise `docker-compose.light.yml` : un seul broker Kafka,
réplication à 1, sans Schema Registry. `MODE=minimal` ajoute `docker-compose.minimal.yml` : sans Spark, MinIO
ni Kafka UI. Voir [Modes de démarrage](#modes-de-démarrage).

## Tests, lint et CI

```bash
pip install -r requirements-dev.txt
pytest -q
ruff check .
```

Les tests couvrent les indicateurs (fonctions pures et expressions Spark natives), le sink TimescaleDB, les
règles d'alerte, l'API, le client klines Binance et le générateur de charge. Les tests Spark nécessitent
Java 17.

La CI GitHub Actions (`.github/workflows/ci.yml`) exécute à chaque push et pull request :

1. **test** : `ruff` puis `pytest` avec couverture (`spark`, `api`, `alerts`, `common`)
2. **compose** : validation des fichiers compose (modes full, light et minimal, Airflow)
3. **dbt** : `dbt deps` et `dbt parse`

## Mesure de performance

Le générateur de charge injecte des trades synthétiques dans `crypto.prices` pour mesurer le débit de bout
en bout :

```bash
make load RATE=500 DURATION=60     # 500 trades/s pendant 60 s
make status                        # observer trades/min, fraîcheur et retard des consumers
```

## Exploitation et dépannage

- **Healthchecks** : les services sans port HTTP (sinks, alertes) écrivent un heartbeat vérifié par
  `python -m common.heartbeat`. `docker compose ps` indique leur état.
- **Fraîcheur des données** : `GET /v1/status` donne l'âge de la dernière donnée par source ; l'onglet
  « Santé » du dashboard l'affiche avec le retard des consumers.
- **Pas de données dans l'API** : vérifier que `binance-producer`, `timescale-sink` et `redis-sink` tournent
  (`make ps`, `make logs SERVICE=...`), puis que les messages arrivent dans Kafka UI.
- **Pas d'indicateurs** : le job Spark est lancé séparément (`make streaming`, indisponible en mode `minimal`) ; consulter la Spark UI
  (http://localhost:8090) et les logs du conteneur `cryptoflow-spark-submit`.
- **Erreur de colonne ou table manquante** (`window_start`, `klines_5m`, `candles_5m`) : volume TimescaleDB
  antérieur aux évolutions du schéma, lancer `make migrate`.
- **Airflow ne démarre pas** : le réseau `cryptoflow-network` doit exister, donc démarrer le compose racine
  avant Airflow.
- **Machine limitée en ressources** : utiliser `MODE=light` ou `MODE=minimal` (`make start`) et ne pas activer le profil `delta`.
- **RSI incohérent** : le RSI approximatif de Spark et le RSI précis du job batch sont stockés dans la même
  colonne `indicators.rsi_approx` (l'API l'expose sous le nom `rsi`). Les valeurs 0 / 50 / 100 viennent de
  Spark ; les autres du job batch. Voir le [manuel d'utilisation](docs/MANUEL_UTILISATEUR.md#12-limites-connues).
