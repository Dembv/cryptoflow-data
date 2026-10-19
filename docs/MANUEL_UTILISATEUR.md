# CryptoFlow — Manuel d'utilisation

Ce manuel explique comment **utiliser** la plateforme au quotidien : la démarrer, lire le dashboard,
interroger l'API, régler les alertes, exploiter les données et résoudre les problèmes courants.
Pour la description technique (architecture, schémas, structure du code), voir le [README](../README.md).

## Sommaire

1. [Présentation](#1-présentation)
2. [Démarrer et arrêter la plateforme](#2-démarrer-et-arrêter-la-plateforme)
3. [Vérifier que tout fonctionne](#3-vérifier-que-tout-fonctionne)
4. [Utiliser le dashboard](#4-utiliser-le-dashboard)
5. [Utiliser l'API](#5-utiliser-lapi)
6. [Recevoir des alertes](#6-recevoir-des-alertes)
7. [Airflow : planifications et relances](#7-airflow--planifications-et-relances)
8. [Explorer les données](#8-explorer-les-données)
9. [Opérations courantes](#9-opérations-courantes)
10. [Sécurité](#10-sécurité)
11. [Dépannage](#11-dépannage)
12. [Limites connues](#12-limites-connues)
13. [Glossaire](#13-glossaire)

---

## 1. Présentation

CryptoFlow collecte en continu des données de marché crypto, calcule des indicateurs techniques et les met
à disposition via un dashboard, une API et des alertes.

| Ce que vous obtenez | Où |
|---|---|
| Prix en direct de 5 actifs (BTC, ETH, SOL, BNB, XRP) | Dashboard, `GET /v1/prices`, WebSocket |
| Bougies 5 minutes (temps réel + 30 jours d'historique Binance) | Dashboard, `GET /v1/ohlcv/{symbole}` |
| VWAP, Bollinger, RSI, ratio acheteurs | Dashboard, `GET /v1/indicators/{symbole}` |
| Résumé du marché et corrélations | Dashboard, `GET /v1/market/*` |
| Gros transferts ETH (« whales ») | Dashboard, `GET /v1/whales/*` |
| Notifications Slack / Telegram | Service d'alertes |
| État de santé du pipeline | Dashboard (onglet Santé), `GET /v1/status` |

Les actifs suivis sont définis par `ASSETS` dans `config/.env` (par défaut `BTC,ETH,SOL,BNB,XRP`).

---

## 2. Démarrer et arrêter la plateforme

### 2.1 Première installation

1. Installer Docker (Compose v2). Prévoir environ 8 Go de RAM pour le mode complet, ou choisir un mode plus léger (voir 2.3).
2. Créer la configuration :

   ```bash
   cp config/.env.example config/.env
   ```

3. Ouvrir `config/.env` et renseigner `ETHERSCAN_API_KEY` (clé gratuite sur etherscan.io). Sans elle, les
   données on-chain et les alertes « whale » ne fonctionnent pas ; le reste n'est pas affecté.

### 2.2 Démarrage complet

Cette section décrit le mode `full`. Pour une machine plus modeste, voir d'abord les modes en 2.3.

Exécuter dans l'ordre, en attendant que chaque étape soit terminée :

```bash
# 1. Infrastructure : Kafka, Spark, MinIO, TimescaleDB, Redis
docker compose up -d

# 2. Applications : producers, sinks, alertes, API, dashboard
docker compose --profile app up -d --build

# 3. Calcul des indicateurs temps réel
docker compose --profile streaming up -d spark-submit

# 4. Orchestration (le réseau cryptoflow-network doit exister : étape 1 faite avant)
docker compose -f airflow/docker-compose.airflow.yml up airflow-init
docker compose -f airflow/docker-compose.airflow.yml up -d --build
```

Avec `make` : `make setup`, puis `make up app streaming`, puis `make airflow-init airflow`.

Ensuite, **dans Airflow** (http://localhost:8085, `admin` / `admin` par défaut), activer les trois DAGs
(interrupteur à gauche de leur nom) :

| DAG | À quoi il sert |
|---|---|
| `cryptoflow_rsi_batch` | RSI précis, toutes les 5 minutes |
| `cryptoflow_dbt_hourly` | Résumé de marché, corrélations, activité whale, toutes les heures |
| `cryptoflow_binance_backfill` | 30 jours de bougies historiques, chaque nuit à 2 h |

Pour avoir tout de suite des données dans le dashboard, déclencher manuellement `cryptoflow_binance_backfill`
puis `cryptoflow_dbt_hourly` (bouton ▶ dans Airflow).

### 2.3 Choisir un mode de démarrage

Trois modes existent selon les ressources de la machine :

| Mode | Services démarrés | Services retirés | Usage |
|---|---|---|---|
| `minimal` | Zookeeper, 1 broker Kafka, TimescaleDB, Redis (+ application : producers, sinks, alertes, API, dashboard) | Spark, MinIO, Kafka UI, brokers 2 et 3, Schema Registry | Machine très limitée, démonstration |
| `light` | `minimal` + Spark, MinIO, Kafka UI | Brokers 2 et 3, Schema Registry | Machine limitée, avec indicateurs temps réel |
| `full` (défaut) | Cluster complet : 3 brokers, Schema Registry, Spark, MinIO, Kafka UI… | Rien | Machine confortable |

**Avec un menu** : `make start` propose le choix du mode (1 = `minimal`, 2 = `light`, 3 = `full`), puis démarre
l'infrastructure et l'application.

```bash
make start
```

**Sans menu** : indiquer le mode directement.

```bash
make start MODE=minimal      # choix + démarrage infra et application
make up app MODE=light       # démarrage seul
```

Le mode choisi est mémorisé dans le fichier `.mode` (non versionné) : les commandes `make` suivantes
(`make ps`, `make logs`, `make down`…) l'utilisent automatiquement. Pour changer de mode, lancer `make down`,
puis `make start` avec le nouveau choix. `MODE=...` en ligne de commande l'emporte sur `.mode`, et l'ancienne
option `LIGHT=1` reste équivalente à `MODE=light`.

Limites du mode `minimal` : Spark et MinIO n'étant pas démarrés, `make streaming` (indicateurs temps réel) et
`make delta` (archivage Delta Lake) sont refusés avec un message explicite. Passer en `light` ou `full` pour les
utiliser.

### 2.4 Archivage Delta Lake (optionnel)

Pour conserver tous les trades dans MinIO :

```bash
make delta        # ou : docker compose --profile delta up -d spark-submit-delta
```

Indisponible en mode `minimal`, à éviter en mode `light` sur une machine très limitée. Les fichiers sont consultables dans la console MinIO (http://localhost:9001,
`minioadmin` / `minioadmin` par défaut), bucket `cryptoflow`.

### 2.5 Volume de données existant

Si vous avez déjà un volume TimescaleDB créé avant une mise à jour, exécuter une fois :

```bash
make migrate
```

La commande est sans danger si on la relance (idempotente).

### 2.6 Arrêt

```bash
make down
```

`make down` arrête les services de tous les modes. Les données sont conservées (volumes Docker). Pour **tout effacer**, y compris les données, ajouter
`-v` aux commandes `docker compose down` : action irréversible, à ne faire que volontairement.

### 2.7 Ordre et délais à connaître

| Après le démarrage | Ce qui apparaît |
|---|---|
| ~30 s | Prix en direct dans le dashboard |
| ~1 à 5 min | Bougies temps réel, premiers indicateurs Spark |
| Dès qu'Airflow a joué le backfill | 30 jours de bougies 5 min |
| Dès que le DAG dbt a tourné | Résumé de marché, corrélations, activité whale quotidienne |
| ~15 à 20 min de trades | Premier RSI précis (14 périodes nécessaires) |

Tant que les DAGs n'ont pas tourné, certaines zones affichent « indisponible » : c'est normal.

---

## 3. Vérifier que tout fonctionne

### 3.1 En une commande

```bash
make status
```

Affiche la santé de l'API puis le statut détaillé. Si l'authentification est activée :
`make status API_KEY=votre_cle`.

### 3.2 Lire le statut

`GET /v1/status` (ou l'onglet **Santé** du dashboard) renvoie :

| Champ | Signification |
|---|---|
| `status` | `ok`, `degraded` (une source en retard ou Redis HS) ou `down` (TimescaleDB injoignable) |
| `services` | État de Redis et TimescaleDB |
| `freshness.trades` | Âge du dernier trade. `ok` si moins de 60 s |
| `freshness.indicators` | Âge du dernier indicateur. `ok` si moins de 180 s |
| `freshness.onchain` | Âge de la dernière transaction on-chain. `ok` si moins de 3600 s |
| `trades_last_minute` | Nombre de trades reçus sur la dernière minute |
| `kafka` | Retard (lag) des consumers `cryptoflow-timescale-sink` et `cryptoflow-redis-sink` |

Un état `stale` signifie « la donnée est plus vieille que le seuil » ; `empty` signifie « aucune donnée ».

### 3.3 Contrôles par composant

```bash
make ps                                    # état des conteneurs (healthy / unhealthy)
make logs SERVICE=binance-producer         # journaux d'un service
docker exec cryptoflow-broker-1 kafka-topics --bootstrap-server localhost:9092 --list
```

Quatre topics doivent exister : `crypto.prices`, `crypto.history`, `crypto.onchain`, `crypto.indicators`.
Dans Kafka UI (http://localhost:8080), onglet *Topics*, les compteurs de messages doivent augmenter.

Pour afficher quelques messages bruts en ligne de commande :

```bash
python consumers/test_consumer.py --topic crypto.prices
python consumers/test_consumer.py --all
```

---

## 4. Utiliser le dashboard

Adresse : **http://localhost:8501**

### 4.1 Barre latérale

| Contrôle | Effet |
|---|---|
| **Actif** | Choisit l'actif affiché dans l'onglet Marché et pour les exports |
| **Fenêtre des indicateurs** | 1 minute, 5 minutes ou 1 heure (défaut : 5 minutes) |
| **Rafraîchissement des graphiques (s)** | De 5 à 60 s (défaut 15). Les prix, eux, se rafraîchissent toutes les 2 s |
| **Seuil whale (ETH)** | De 100 à 5000 ETH (défaut 500) pour l'onglet Whales |
| **Badge de santé** | 🟢 ok · 🟠 dégradé · 🔴 hors service, avec l'âge des données et les trades/min |

Si le badge affiche « API injoignable », l'API est arrêtée ou redémarre : voir [Dépannage](#11-dépannage).

### 4.2 Bandeau des prix

Cinq cartes (une par actif) affichent le dernier prix, mis à jour toutes les 2 secondes, et la variation sur
24 h (lue dans le résumé dbt). Le prix affiche « — » si aucun trade récent n'est disponible ; la variation
n'apparaît qu'après la première exécution de dbt.

### 4.3 Onglet Marché

- **Bougies 5 min** (gauche) : 200 dernières bougies de l'actif, mélange d'historique Binance et de trades
  temps réel. Vide au début : lancer le backfill ou attendre quelques minutes.
- **Bollinger & VWAP** (droite, haut) : bande haute, médiane, basse et VWAP sur la fenêtre choisie.
- **RSI** (droite, bas) : courbe entre 0 et 100 avec deux repères à 70 (surachat) et 30 (survente).
- **Exporter les bougies / les indicateurs** : télécharge un CSV (jusqu'à 2000 bougies, 1000 indicateurs).

**Lecture rapide**

| Signal | Interprétation usuelle |
|---|---|
| RSI > 70 | Zone de surachat |
| RSI < 30 | Zone de survente |
| Prix qui touche la bande haute / basse | Mouvement étendu par rapport à la moyenne récente |
| Bandes très resserrées (bandwidth faible) | « Squeeze » : volatilité basse, mouvement possible |
| Ratio acheteurs > 0,5 | Davantage de trades à l'achat qu'à la vente sur la fenêtre |

Ces indicateurs sont informatifs ; la plateforme ne donne aucun conseil d'investissement.

### 4.4 Onglet Corrélations

Heatmap de la corrélation entre actifs (de −1 à +1) sur **7 jours** ou **30 jours** (bouton radio). Rouge =
corrélation positive, bleu = négative selon l'échelle RdBu. Calculée sur les rendements journaliers ; la date
de calcul est affichée sous le graphique. Le message « indisponible » signifie que le DAG dbt n'a pas encore
tourné.

### 4.5 Onglet Whales

- **Transactions ≥ seuil** : jusqu'à 100 transactions ETH récentes (hash, adresses, montant, bloc).
- **Activité quotidienne** : agrégats par jour issus de dbt.
- Le seuil se règle dans la barre latérale. Rafraîchi toutes les 30 s.

### 4.6 Onglet Santé

Détail du statut (services, fraîcheur des sources) et tableau du **retard des consumers Kafka** : un retard
qui grandit en continu indique que le sink ne suit pas le débit.

---

## 5. Utiliser l'API

Documentation interactive (essai direct des routes) : **http://localhost:8000/docs**

### 5.1 Authentification

Par défaut (`API_KEY` vide), aucune authentification. Si `API_KEY` est défini dans `config/.env`, toutes les
routes `/v1` exigent l'en-tête :

```
X-API-Key: votre_cle
```

`GET /health` reste public. Sans clé valide : réponse `401`.

### 5.2 Limites

- **Débit** : `RATE_LIMIT_PER_MIN` requêtes par minute et par IP (120 par défaut). Au-delà : `429` avec
  l'en-tête `Retry-After: 60`.
- **Symboles** : seuls ceux de `ASSETS` sont acceptés (insensible à la casse) ; sinon `404`.
- **CORS** : limité aux origines de `CORS_ORIGINS` (`*` par défaut), méthode GET uniquement.

### 5.3 Routes

| Route | Paramètres | Réponse |
|---|---|---|
| `GET /health` | — | `{"redis": "ok", "timescale": "ok"}` ou `503` |
| `GET /v1/prices` | — | Dernier prix de tous les actifs |
| `GET /v1/prices/{symbol}` | — | Dernier prix d'un actif |
| `GET /v1/ohlcv/{symbol}` | `limit` (1–2000, défaut 200), `offset`, `format` | Bougies 5 min, ordre chronologique |
| `GET /v1/indicators/{symbol}` | `window`, `limit` (1–1000, défaut 100), `offset`, `format` | Indicateurs, du plus récent au plus ancien |
| `GET /v1/market/summary` | — | Résumé par actif (dbt) |
| `GET /v1/market/correlations` | `window` = `7d` ou `30d` | Matrice de corrélation la plus récente |
| `GET /v1/whales/recent` | `min_eth` (défaut 100), `limit` (1–500, défaut 50), `format` | Transactions ETH récentes |
| `GET /v1/whales/daily` | `symbol`, `min_eth`, `limit`, `offset` | Activité quotidienne |
| `GET /v1/status` | — | Santé du pipeline |
| `WS /v1/ws/prices` | `api_key` (si auth activée) | Flux de prix, 1 message par seconde |

`window` accepte exactement : `1 minute`, `5 minutes`, `1 hour`.

### 5.4 Exemples

Dernier prix du Bitcoin :

```bash
curl http://localhost:8000/v1/prices/BTC
```

```json
{"price": 65432.1, "quantity": 0.00234, "side": "buy",
 "trade_time_iso": "2024-03-09T12:00:00+00:00", "source": "binance_ws", "cache": "redis"}
```

Le champ `cache` indique l'origine : `redis` (cas normal) ou `timescale` (repli quand la valeur Redis a
expiré, 5 s de TTL).

Bougies de l'ETH en CSV, avec authentification :

```bash
curl -H "X-API-Key: $API_KEY" "http://localhost:8000/v1/ohlcv/ETH?limit=500&format=csv" -o eth_5m.csv
```

Indicateurs 1 heure du SOL :

```bash
curl "http://localhost:8000/v1/indicators/SOL?window=1%20hour&limit=24"
```

Baleines de plus de 1000 ETH :

```bash
curl "http://localhost:8000/v1/whales/recent?min_eth=1000&limit=20"
```

### 5.5 Pagination

Les routes `ohlcv` et `indicators` renvoient :

```json
{"items": [ ... ], "limit": 100, "offset": 0, "next_offset": 100}
```

Pour tout parcourir, rappeler la route avec `offset=<next_offset>` jusqu'à ce que `next_offset` soit `null`.

### 5.6 WebSocket

Exemple en Python (`pip install websockets`) :

```python
import asyncio, json, websockets

async def main():
    async with websockets.connect("ws://localhost:8000/v1/ws/prices") as ws:
        while True:
            prices = json.loads(await ws.recv())
            print({sym: p["price"] for sym, p in prices.items()})

asyncio.run(main())
```

Avec authentification : `ws://localhost:8000/v1/ws/prices?api_key=votre_cle`. Une clé invalide ferme la
connexion avec le code `4401`. Les messages envoyés par le client sont ignorés.

### 5.7 Codes de réponse

| Code | Signification |
|---|---|
| 200 | Succès |
| 401 | Clé d'API absente ou invalide |
| 404 | Symbole inconnu, aucun trade récent, ou aucune corrélation disponible |
| 422 | Paramètre invalide (ex. `limit` hors bornes, `window` inconnue) |
| 429 | Trop de requêtes |
| 503 | Tables dbt absentes (lancer `cryptoflow_dbt_hourly`) ou dépendance en panne |

### 5.8 Résumé de marché : champs

`GET /v1/market/summary` renvoie, par actif :

| Champ | Description |
|---|---|
| `price_usd`, `open`, `high`, `low`, `volume`, `volume_usd` | Dernière bougie journalière |
| `pct_change_24h` | Variation journalière |
| `vwap_24h`, `vwap_1h` | VWAP journalier et horaire |
| `rsi`, `rsi_zone`, `trading_signal` | RSI horaire, zone, signal dérivé |
| `bb_upper`, `bb_middle`, `bb_lower`, `bb_bandwidth` | Bollinger horaires |
| `buy_ratio` | Part d'achats sur l'heure |
| `corr_with_btc_7d` | Corrélation avec le BTC sur 7 jours (vide pour BTC) |
| `data_quality_score` | Score 0–100 de complétude des données (RSI en zone normale, bandes, volume, date du jour) |
| `dbt_updated_at` | Date du dernier calcul |

Le résumé est mis en cache `CACHE_TTL_SUMMARY` secondes (10 par défaut).

---

## 6. Recevoir des alertes

### 6.1 Règles

Le service `alert-service` surveille `crypto.indicators` et `crypto.onchain`.

| Règle | Sévérité | Déclenchement |
|---|---|---|
| Surachat RSI | warning | RSI ≥ `ALERT_RSI_OVERBOUGHT` (70), fenêtre 5 minutes |
| Survente RSI | warning | RSI ≤ `ALERT_RSI_OVERSOLD` (30), fenêtre 5 minutes |
| Squeeze Bollinger | info | Bandwidth 1 heure entre 0 et `ALERT_SQUEEZE_BANDWIDTH` (0,02) |
| Whale | warning | Transaction ETH ≥ `ALERT_WHALE_ETH` (500) |
| Whale | critical | Transaction ETH ≥ 10 × `ALERT_WHALE_ETH` |

Une alerte identique (même règle, même actif) n'est pas renvoyée avant `ALERT_COOLDOWN_SEC` secondes
(900 s = 15 min par défaut).

### 6.2 Activer Slack

1. Créer un *Incoming Webhook* dans votre espace Slack.
2. Renseigner `ALERT_SLACK_WEBHOOK=<url>` dans `config/.env`. Cette URL est un secret : ne pas la commiter.
3. Redémarrer le service :

   ```bash
   docker compose --profile app up -d --force-recreate alert-service
   ```

### 6.3 Activer Telegram

1. Créer un bot avec @BotFather et récupérer son jeton.
2. Récupérer l'identifiant de votre conversation (`chat_id`).
3. Renseigner `TELEGRAM_BOT_TOKEN` et `TELEGRAM_CHAT_ID` dans `config/.env`, puis recréer `alert-service`
   comme ci-dessus.

Sans aucun canal configuré, les alertes apparaissent seulement dans les logs :

```bash
make logs SERVICE=alert-service
```

### 6.4 Régler la sensibilité

Modifier les variables `ALERT_*` dans `config/.env`, puis recréer `alert-service`. Exemples :

| Objectif | Réglage |
|---|---|
| Moins de bruit RSI | `ALERT_RSI_OVERBOUGHT=80`, `ALERT_RSI_OVERSOLD=20` |
| Ne suivre que les très gros transferts | `ALERT_WHALE_ETH=2000` |
| Plus d'espace entre deux alertes identiques | `ALERT_COOLDOWN_SEC=3600` |

Les alertes d'échec de DAG Airflow passent par un mécanisme distinct (plugin `slack_alert.py`).

---

## 7. Airflow : planifications et relances

Interface : **http://localhost:8085**

| Action | Comment |
|---|---|
| Activer / suspendre un DAG | Interrupteur à gauche du nom |
| Lancer un DAG à la demande | Bouton ▶ (*Trigger DAG*) |
| Voir pourquoi un DAG échoue | Cliquer sur le DAG → exécution en rouge → tâche → *Logs* |
| Relancer une tâche | Sur la tâche en échec : *Clear* |

### 7.1 Les trois DAGs

- **`cryptoflow_rsi_batch`** (`*/5 * * * *`) : calcule le RSI précis (Wilder, 14 périodes) et le publie dans
  Kafka vers `indicators`. Il faut au moins 15 clôtures de bougies pour produire une valeur.
- **`cryptoflow_dbt_hourly`** (`@hourly`) : exécute les modèles dbt puis leurs tests. Alimente le résumé de
  marché, les corrélations et l'activité whale. Un test en échec signale un problème de qualité de données.
- **`cryptoflow_binance_backfill`** (`0 2 * * *`) : charge les bougies 5 minutes dans `klines_5m`. Premier
  passage : 30 jours (`BACKFILL_DAYS`) ; ensuite reprise à la dernière bougie connue.

### 7.2 Recalculer entièrement dbt

Après un changement de logique dans un modèle incrémental (`daily_ohlcv`, `hourly_indicators`) :

```bash
docker compose -f airflow/docker-compose.airflow.yml exec airflow-scheduler \
  bash -c "cd /opt/airflow/dbt && dbt run --full-refresh --profiles-dir ."
```

> Le nom du service et le chemin dépendent de `airflow/docker-compose.airflow.yml` ; adapter si besoin.
> Hors conteneur : `cd dbt && dbt run --full-refresh --profiles-dir .` avec dbt-postgres installé.

---

## 8. Explorer les données

### 8.1 Se connecter à TimescaleDB

```bash
docker exec -it cryptoflow-timescaledb psql -U crypto -d cryptoflow
```

Ou depuis un client SQL : hôte `localhost`, port `5433`, base `cryptoflow`, utilisateur `crypto`.

### 8.2 Tables utiles

| Table / vue | Contenu |
|---|---|
| `raw_trades` | Trades Binance bruts (conservés 30 jours) |
| `candles_5m` | Bougies 5 min : temps réel + historique |
| `indicators` | Indicateurs par fenêtre (conservés 90 jours) |
| `onchain_events` | Transactions et transferts on-chain |
| `klines_5m` | Historique Binance chargé par le backfill |
| `marts.*`, `gold.*` | Tables dbt (corrélations, résumé, activité whale…) |

### 8.3 Requêtes d'exemple

Dernier prix et volume de la dernière heure :

```sql
SELECT symbol, last(price, trade_time) AS price, sum(quantity) AS volume
FROM raw_trades
WHERE trade_time > now() - interval '1 hour'
GROUP BY symbol;
```

Dernières bougies du BTC :

```sql
SELECT * FROM candles_5m WHERE symbol = 'BTC' ORDER BY bucket DESC LIMIT 20;
```

Indicateurs 5 minutes en dédoublonnant les ré-émissions Spark :

```sql
SELECT DISTINCT ON (window_start)
       window_start, vwap, rsi_approx AS rsi, bb_bandwidth
FROM indicators
WHERE symbol = 'ETH' AND window_label = '5 minutes' AND window_start IS NOT NULL
ORDER BY window_start DESC, computed_at DESC
LIMIT 50;
```

Résumé dbt :

```sql
SELECT symbol, price_usd, pct_change_24h, rsi, trading_signal FROM gold.market_summary;
```

Pensez à toujours dédoublonner les indicateurs par `(symbol, window_label, window_start)` : Spark réécrit
chaque fenêtre à chaque trigger.

### 8.4 Console Redis

```bash
docker exec -it cryptoflow-redis redis-cli --scan --pattern "crypto:last_price:*"
docker exec -it cryptoflow-redis redis-cli get crypto:last_price:BTC
```

Les prix y expirent après `REDIS_TTL_SECONDS` (5 s) : un résultat vide signifie qu'aucun trade n'est arrivé
récemment pour cet actif.

### 8.5 Archive Delta Lake

Console MinIO : http://localhost:9001, bucket `cryptoflow`. Les tables Delta se lisent avec Spark
(`spark.read.format("delta").load("s3a://cryptoflow/...")`) en configurant l'accès S3 vers
`http://minio:9000`.

---

## 9. Opérations courantes

### 9.1 Ajouter ou retirer un actif

1. Modifier `ASSETS` dans `config/.env`.
2. Recréer les services concernés :

   ```bash
   docker compose --profile app up -d --force-recreate
   ```

3. Limites actuelles : la liste des actifs est aussi codée en dur dans `dashboard/app.py` (`ASSETS`, y
   compris la clé des corrélations) et dans les modèles dbt de corrélation (paires BTC/ETH/SOL/BNB/XRP). Un
   nouvel actif nécessite donc de modifier aussi ces fichiers pour apparaître partout.

### 9.2 Appliquer un changement de configuration

Après modification de `config/.env`, recréer le service concerné :

```bash
docker compose --profile app up -d --force-recreate <service>
```

Services applicatifs : `binance-producer`, `coingecko-producer`, `etherscan-producer`, `timescale-sink`,
`redis-sink`, `alert-service`, `api`, `dashboard`.

### 9.3 Mettre à jour le code

```bash
docker compose --profile app up -d --build
```

Le job Spark lit le code monté depuis `./spark` : après modification de `streaming_job.py` ou
`indicators.py`, relancer le job :

```bash
docker compose --profile streaming up -d --force-recreate spark-submit
```

### 9.4 Redémarrer un seul service

```bash
docker compose --profile app restart api
```

### 9.5 Surveiller les ressources

```bash
docker stats
```

Si la machine sature : mode léger (`LIGHT=1`), pas de profil `delta`, et réduire `ASSETS`.

### 9.6 Mesurer la capacité (test de charge)

```bash
make load RATE=500 DURATION=60
```

Injecte 500 trades synthétiques par seconde pendant 60 s dans `crypto.prices`. Observer ensuite
`make status` : `trades_last_minute`, fraîcheur et retard des consumers montrent si la chaîne suit. Les
trades synthétiques se mêlent aux vraies données : ne pas lancer de test de charge sur une base à valeur
analytique sans le savoir.

### 9.7 Sauvegarde

Les données persistent dans les volumes Docker (`timescaledb-data`, `minio-data`, `broker-*-data`, …). Pour
sauvegarder TimescaleDB :

```bash
docker exec cryptoflow-timescaledb pg_dump -U crypto -d cryptoflow -Fc > cryptoflow.dump
```

Restauration : `pg_restore` dans une base vide avec l'extension `timescaledb` (procédure TimescaleDB
standard `timescaledb_pre_restore()` / `timescaledb_post_restore()`).

---

## 10. Sécurité

Les valeurs par défaut sont prévues pour un poste de développement.

| Élément | Par défaut | À faire avant toute exposition réseau |
|---|---|---|
| API | Sans authentification | Définir `API_KEY`, restreindre `CORS_ORIGINS` |
| TimescaleDB | `crypto` / `crypto`, port 5433 publié | Changer le mot de passe, ne pas publier le port |
| MinIO | `minioadmin` / `minioadmin` | Changer les identifiants |
| Airflow | `admin` / `admin` | Définir `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_FERNET_KEY`, `AIRFLOW_SECRET_KEY` |
| Kafka, Redis | Sans authentification, ports publiés | Ne pas exposer à Internet |

Bonnes pratiques :

- Ne jamais commiter `config/.env` (clé Etherscan, webhook Slack, jeton Telegram).
- Mettre l'API et le dashboard derrière un reverse proxy HTTPS pour un usage hors machine locale.
- La clé d'API passée en paramètre (`?api_key=`, WebSocket uniquement) peut apparaître dans des journaux de
  proxy : préférer l'en-tête `X-API-Key` partout où c'est possible.

---

## 11. Dépannage

Commencer par `make ps` (conteneurs `unhealthy` ou arrêtés) et `make status`.

### Le dashboard affiche « API injoignable »

- `make ps` : le service `api` est-il `healthy` ? Sinon `make logs SERVICE=api`.
- L'API démarre après TimescaleDB et Redis : attendre leur état `healthy`.
- `curl http://localhost:8000/health` doit renvoyer `{"redis":"ok","timescale":"ok"}`.

### Les prix affichent « — »

- `binance-producer` tourne-t-il ? `make logs SERVICE=binance-producer` (erreur réseau, Binance bloqué dans
  certains pays ou réseaux d'entreprise).
- Des messages arrivent-ils dans `crypto.prices` (Kafka UI) ?
- `redis-sink` et `timescale-sink` sont-ils `healthy` ?

### Pas de bougies

Lancer le DAG `cryptoflow_binance_backfill` (historique) ou attendre 5 à 10 minutes de trades. Si le
backfill échoue : logs de la tâche dans Airflow (accès à api.binance.com).

### Pas d'indicateurs, ou indicateurs figés

- Le job Spark n'est pas lancé automatiquement avec `app` : `make streaming`.
- Spark UI (http://localhost:8090) : l'application doit être *RUNNING*.
- Logs : `docker logs cryptoflow-spark-submit`. Le conteneur est en `restart: "no"` : s'il s'est arrêté, le
  relancer avec `make streaming`.

### Le RSI est toujours 0, 50 ou 100

C'est la valeur approximative de Spark. Le RSI précis n'apparaît qu'après exécution du DAG
`cryptoflow_rsi_batch` (voir [Limites connues](#12-limites-connues)).

### « Tables dbt absentes » (503) ou « dbt pas encore exécuté »

Activer et lancer `cryptoflow_dbt_hourly` dans Airflow. Si le DAG échoue, lire les logs de la tâche : un
`dbt test` en échec indique un souci de qualité de données.

### Pas de données on-chain ni d'alertes whale

- `ETHERSCAN_API_KEY` est-elle renseignée avec une vraie clé ?
- `make logs SERVICE=etherscan-producer` : erreurs `Invalid API Key` ou limite de débit.
- Le seuil est-il trop haut ? Baisser `min_eth` ou `ALERT_WHALE_ETH`.

### Aucune alerte Slack / Telegram

- Canal configuré dans `config/.env` **et** `alert-service` recréé après modification ?
- `make logs SERVICE=alert-service` : l'alerte est-elle détectée mais l'envoi échoue, ou bloquée par le
  délai anti-doublon (`ALERT_COOLDOWN_SEC`) ?

### Erreur « column window_start / relation klines_5m / candles_5m does not exist »

Volume TimescaleDB antérieur à une mise à jour du schéma : `make migrate`.

### Le retard Kafka (lag) augmente sans cesse

Un sink est trop lent ou bloqué. Consulter `make logs SERVICE=timescale-sink` (erreurs de base de données).
Réduire la charge (moins d'actifs) ou augmenter les ressources Docker.

### Airflow ne démarre pas

- Le réseau `cryptoflow-network` doit exister : démarrer d'abord `docker compose up -d`.
- Première utilisation : exécuter `airflow-init` avant le démarrage.

### Un conteneur redémarre en boucle

`docker logs <conteneur>` pour la cause (variable manquante, dépendance non prête, mémoire insuffisante). En
cas de manque de mémoire, utiliser `LIGHT=1`.

### Port déjà utilisé

Un autre programme occupe un des ports (8000, 8080, 8501, 5433, 6379, 9092…). Le libérer, ou modifier le
mapping de ports dans `docker-compose.yml`.

---

## 12. Limites connues

- **RSI : deux sources dans une colonne.** Le RSI approximatif de Spark (valeurs 0, 50 ou 100) et le RSI
  précis du job batch (Wilder 14) sont enregistrés dans la même colonne `indicators.rsi_approx`, exposée
  par l'API et le dashboard sous le nom `rsi`. Sur les graphiques, des paliers à 0/50/100 proviennent de
  Spark ; les valeurs continues du batch. Les alertes RSI, elles, ne retiennent que les messages portant
  le RSI précis (fenêtre 5 minutes).
- **Liste d'actifs.** Elle est partiellement codée en dur (dashboard, corrélations dbt) : voir
  [9.1](#91-ajouter-ou-retirer-un-actif).
- **Instance unique.** La limitation de débit et le cache du WebSocket sont en mémoire : prévus pour une seule
  instance de l'API.
- **Historique de prix.** Les bougies profondes viennent de Binance (30 jours au premier backfill) ; les
  trades bruts ne sont gardés que 30 jours.
- **Données de marché publiques, sans garantie.** Les flux dépendent de la disponibilité de Binance,
  CoinGecko et Etherscan et de leurs quotas.
- **Pas un outil de conseil financier.** Indicateurs et alertes sont des aides à la lecture du marché.

---

## 13. Glossaire

| Terme | Définition |
|---|---|
| **Bandwidth (Bollinger)** | Largeur relative des bandes ; faible = marché calme (squeeze) |
| **Backfill** | Chargement de données historiques passées |
| **Bougie (OHLCV)** | Ouverture, plus haut, plus bas, clôture et volume sur une période (ici 5 min) |
| **Bollinger (bandes)** | Moyenne mobile entourée de deux bandes d'écart-type |
| **Consumer lag** | Nombre de messages Kafka reçus mais pas encore traités par un consumer |
| **dbt** | Outil de transformation SQL qui construit les tables `marts` et `gold` |
| **Delta Lake** | Format de stockage de fichiers transactionnel, ici sur MinIO |
| **DAG** | Workflow planifié dans Airflow |
| **Hypertable** | Table TimescaleDB partitionnée automatiquement par le temps |
| **Kafka (topic)** | File de messages nommée, par ex. `crypto.prices` |
| **RSI** | Indicateur de 0 à 100 mesurant la force des hausses face aux baisses récentes |
| **Squeeze** | Resserrement des bandes de Bollinger, souvent avant un mouvement |
| **TTL** | Durée de vie d'une valeur en cache avant expiration |
| **VWAP** | Prix moyen pondéré par les volumes échangés |
| **Watermark (Spark)** | Retard maximal toléré pour les événements tardifs (ici 10 minutes) |
| **Whale** | Gros porteur ; ici, transaction ETH dépassant un seuil configurable |
