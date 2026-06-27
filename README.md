# Plateforme de Détection de Fraude Temps Réel

**Stack :** Apache Kafka + PyFlink + Cassandra + Redis + FastAPI + Prometheus + Grafana  
**Volume cible :** 1M+ events/minute | **Latence :** < 100ms

---

## Comprendre le projet

### En langage simple (non-tech)

Imagine une banque qui traite des millions de paiements par minute. Des fraudeurs tentent d'utiliser des cartes volées. Ce projet est un **système de surveillance automatique** qui :

1. **Simule des transactions** bancaires (achats normaux et frauduleux)
2. **Analyse chaque transaction en temps réel** avec 4 règles :
   - *Montant suspect* → un achat de 15 000€ chez le boulanger
   - *Trop de transactions* → 5 paiements sur la même carte en 5 minutes
   - *Voyage trop rapide* → carte utilisée à Paris puis New York en 30 min
   - *Pattern carding* → petit test (1€) puis gros retrait (1000€) en 10 min
3. **Déclenche une alerte** immédiatement stockée dans une base de données
4. **Affiche les alertes** sur un dashboard en temps réel

> C'est comme un videur de boîte de nuit qui vérifie chaque entrée, repère les comportements suspects, et alerte la sécurité instantanément.

### En langage technique

```
┌─────────────────────────────────────────────────────────────┐
│  Producer (Python/Faker)                                    │
│  génère 10 tx/sec (5% frauduleuses)                         │
│  Format: JSON (defaut) ou Avro                              │
└──────────────────┬──────────────────────────────────────────┘
                   │ transactions
                   ▼
┌─────────────────────────────────────────────────────────────┐
│  Schema Registry (Confluent)                                 │
│  Gère les schémas Avro (transaction.avsc, fraud_alert.avsc) │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│  Kafka 3.9 (broker KRaft)                                   │
│  Topics: transactions, fraud-alerts                         │
└──────────────────┬──────────────────────────────────────────┘
                   │ stream
                   ▼
┌─────────────────────────────────────────────────────────────┐
│  Détection (2 modes)                                        │
│  ┌─ Mode simple: Consumer Python (defaultdict)              │
│  └─ Mode prod:   PyFlink Table API (HOP windows)           │
│                                                             │
│  Règles implémentées :                                      │
│  · amount > 10 000€  (filtre SQL)                          │
│  · >= 3 tx en 5min   (HOP window COUNT)                    │
│  · >= 2 villes en 1h (HOP window COUNT DISTINCT)           │
│  · Pattern carding   (< 5€ puis > 1000€ en 10min)          │
└──────────────────┬──────────────────────────────────────────┘
                   │ fraud-alerts (JSON)
                   ▼
           ┌───────┴───────┐
           ▼               ▼
┌──────────────────┐ ┌──────────────────┐
│  Cassandra       │ │  Redis           │
│  Stockage durable │ │  100 dernières   │
│  Table alerts    │ │  TTL 1h          │
└────────┬─────────┘ └────────┬─────────┘
         │                    │
         ▼                    ▼
┌──────────────────┐ ┌──────────────────┐
│  Grafana         │ │  FastAPI         │
│  Dashboard       │ │  GET /alerts/*   │
│  7 panels        │ │  GET /metrics    │
└──────────────────┘ └──────────────────┘
         ▲
         │ Prometheus
┌──────────────────┐
│  Metrics Exporter│
│  (lit Redis)     │
└──────────────────┘

┌──────────────────┐
│  MinIO (S3)      │
│  Archive Parquet │
│  (rejeu ML)      │
└──────────────────┘
```

---

## Prérequis

- **Docker** + Docker Compose
- **Python 3.12+**
- **Git**

## Démarrage rapide

```bash
# 1. Cloner le projet
git clone <url-du-repo>
cd 01-Fraude-Temps-Reel-Kafka-Flink

# 2. Lancer l'infrastructure
docker compose up -d kafka schema-registry cassandra redis

# 3. Vérifier que tout est prêt
docker compose logs --tail=5 kafka schema-registry cassandra redis

# 4. Ouvrir 5 terminaux :

# Terminal 1 - Générer des transactions (JSON)
python src/producer/transaction_generator.py

# Ou en mode Avro
SERIALIZATION_FORMAT=avro python src/producer/transaction_generator.py

# Terminal 2 - Détecter les fraudes
python src/flink_jobs/fraud_detection.py

# Terminal 3 - Stocker dans Redis
python src/sink/redis_writer.py

# Terminal 4 - Stocker dans Cassandra
python src/sink/cassandra_writer.py

# Terminal 5 - API REST
uvicorn src.api.main:app --reload --port 8000

# 5. Voir les alertes
curl http://localhost:8000/alerts/recent
curl http://localhost:8000/alerts/stats

# 6. Dashboard Grafana
# http://localhost:3001 (admin/admin)
```

---

## Guide détaillé des composants

### Producer `src/producer/transaction_generator.py`
Génère des transactions financières synthétiques avec Faker (locale `fr_FR`).
- 95% transactions normales (1-500€), 5% frauduleuses (500-25 000€)
- 10 transactions/seconde
- Envoie au topic Kafka `transactions`
- Support JSON (défaut) ou Avro via `SERIALIZATION_FORMAT=avro`

### Schema Registry
- Service Confluent Schema Registry (port 8085)
- Schémas Avro dans `src/schemas/` :
  - `transaction.avsc` — schéma des transactions
  - `fraud_alert.avsc` — schéma des alertes

### Détection de fraude `src/flink_jobs/fraud_detection.py`
**Mode simple** (consumer Python) - 4 règles de détection :
| Règle | Seuil | Fenêtre |
|-------|-------|---------|
| Montant excessif | `amount > 10 000€` | Aucune (par transaction) |
| Vélocité anormale | `>= 3 tx en 5 min` | Glissante 5 min |
| Changement pays rapide | `>= 2 villes en 1h` | Glissante 1h |
| Pattern carding | `< 5€ puis > 1 000€ en 10 min` | Glissante 10 min |

### Mode production PyFlink `src/flink_jobs/fraud_detection_flink.py`
Version scalable avec PyFlink Table API utilisant des HOP windows.
Nécessite un cluster Flink (docker-compose inclus).

### Writers (stockage)
| Writer | Destination | Rôle |
|--------|-------------|------|
| `cassandra_writer.py` | Cassandra | Stockage historique des alertes |
| `redis_writer.py` | Redis | Cache des 100 dernières alertes (TTL 1h) |
| `minio_writer.py` | MinIO (S3) | Archive Parquet des transactions brutes |

### API REST `src/api/main.py`
| Endpoint | Description |
|----------|-------------|
| `GET /health` | Healthcheck + uptime |
| `GET /alerts/recent?limit=10` | N dernières alertes (depuis Redis) |
| `GET /alerts/stats` | Statistiques globales (par règle, sévérité) |
| `GET /alerts/card/{card_id}?limit=20` | Historique des alertes d'une carte (Cassandra) |
| `GET /metrics` | Métriques opérationnelles (uptime, throughput) |

### Monitoring
| Service | Port | Accès |
|---------|------|-------|
| Grafana | 3001 | http://localhost:3001 (admin/admin) |
| Prometheus | 9091 | http://localhost:9091 |
| API | 8000 | http://localhost:8000/docs |

Le dashboard Grafana est auto-provisionné au démarrage avec 7 panels.

### Archive MinIO
| Service | Port | Accès |
|---------|------|-------|
| MinIO API | 9002 | |
| MinIO Console | 9003 | http://localhost:9003 (admin/password123) |

---

## Variables d'environnement

Copier `.env` et ajuster si nécessaire :

```env
KAFKA_HOST=localhost
KAFKA_PORT=9092
CASSANDRA_HOST=localhost
REDIS_HOST=localhost
MINIO_ENDPOINT=localhost:9002
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password123
API_PORT=8000
SERIALIZATION_FORMAT=json        # json | avro
SCHEMA_REGISTRY_URL=http://localhost:8085
```

---

## Structure du projet

```
.
├── .env                          # Configuration / variables d'environnement
├── .gitignore
├── .ruff.toml                    # Configuration du linter Ruff
├── docker-compose.yml            # Infra complète (Kafka, SR, Cassandra, Redis, Flink, MinIO, Grafana, Prometheus)
├── requirements.txt              # Dépendances Python
├── Dockerfile.metrics            # Image Docker pour le metrics exporter
├── submit_flink_job.sh           # Script de soumission du job PyFlink
├── PERFORMANCE.md                # Rapport de performance & benchmarks
├── implementation_plan.md        # Plan de complétion du projet
├── task.md                       # To-do list
│
├── .github/workflows/
│   └── ci.yml                    # CI pipeline (lint + tests + docker)
│
├── scripts/
│   └── benchmark.py              # Script de benchmark de performance
│
├── src/
│   ├── schemas/
│   │   ├── transaction.avsc      # Schéma Avro des transactions
│   │   └── fraud_alert.avsc      # Schéma Avro des alertes
│   │
│   ├── producer/
│   │   └── transaction_generator.py    # Génère les transactions → Kafka
│   │
│   ├── flink_jobs/
│   │   ├── fraud_detection.py          # Détection mode simple (Consumer Python)
│   │   └── fraud_detection_flink.py    # Détection mode prod (PyFlink Table API)
│   │
│   ├── sink/
│   │   ├── cassandra_writer.py         # Alertes → Cassandra
│   │   ├── redis_writer.py             # Alertes → Redis
│   │   ├── minio_writer.py             # Transactions → MinIO (Parquet)
│   │   └── alert_viewer.py             # Console de visualisation des alertes
│   │
│   ├── api/
│   │   └── main.py                     # FastAPI (5 endpoints + CORS)
│   │
│   ├── monitoring/
│   │   ├── metrics_exporter.py         # Expose métriques Prometheus
│   │   ├── prometheus.yml              # Config Prometheus
│   │   ├── dashboards/
│   │   │   └── fraud_detection.json    # Dashboard Grafana (7 panels)
│   │   └── grafana/
│   │       ├── datasources.yml         # Auto-provisioning source Prometheus
│   │       └── dashboards.yml          # Auto-provisioning dashboards
│   │
│   └── tests/
│       ├── conftest.py
│       ├── test_rules.py               # 16+ tests unitaires des règles
│       ├── test_api.py                 # Tests API
│       └── test_integration.py         # Tests d'intégration (mockés)
│
├── flink-jars/                         # JAR Kafka SQL connector pour Flink
└── architecture.md                     # Schéma d'architecture (Mermaid)
```

---

## Tests

```bash
# Tests unitaires des règles de détection (16+ tests)
python -m pytest src/tests/test_rules.py -v

# Tests API (nécessite Redis en cours)
python -m pytest src/tests/test_api.py -v

# Tests d'intégration (mockés, pas d'infra requise)
python -m pytest src/tests/test_integration.py -v

# Tout exécuter
python -m pytest src/tests/ -v
```

---

## CI/CD

Le pipeline CI (`.github/workflows/ci.yml`) exécute automatiquement :
1. **Lint** — Ruff check sur `src/`
2. **Tests** — Pytest avec service Redis
3. **Docker** — Validation du docker-compose.yml

---

## Benchmark & Performance

Voir [PERFORMANCE.md](./PERFORMANCE.md) pour les résultats de benchmark,
les SLA cibles et les recommandations d'optimisation.

```bash
python scripts/benchmark.py
```

---

## Déploiement GitHub

```bash
git init
git add .
git commit -m "Initial commit - Plateforme detection fraude temps reel"
gh repo create fraud-detection-realtime --public --source=. --push
```

---

## Licence

MIT
