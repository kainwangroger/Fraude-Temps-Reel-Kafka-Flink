# Plateforme de Détection de Fraude Temps Réel

**Stack :** Apache Kafka + PyFlink + Cassandra + Redis + FastAPI + Prometheus + Grafana  
**Volume cible :** 1M+ events/minute | **Latence :** < 100ms

---

## Comprendre le projet

### En langage simple (non-tech)

Imagine une banque qui traite des millions de paiements par minute. Des fraudeurs tentent d'utiliser des cartes volées. Ce projet est un **système de surveillance automatique** qui :

1. **Simule des transactions** bancaires (achats normaux et frauduleux)
2. **Analyse chaque transaction en temps réel** avec 3 règles :
   - *Montant suspect* → un achat de 15 000€ chez le boulanger
   - *Trop de transactions* → 5 paiements sur la même carte en 5 minutes
   - *Voyage trop rapide* → carte utilisée à Paris puis New York en 30 min
3. **Déclenche une alerte** immédiatement stockée dans une base de données
4. **Affiche les alertes** sur un dashboard en temps réel

> C'est comme un videur de boîte de nuit qui vérifie chaque entrée, repère les comportements suspects, et alerte la sécurité instantanément.

### En langage technique

```
┌─────────────────────────────────────────────────────────────┐
│  Producer (Python/Faker)                                    │
│  génère 10 tx/sec (5% frauduleuses)                         │
└──────────────────┬──────────────────────────────────────────┘
                   │ transactions (JSON)
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
│  Dashboard       │ │  GET /alerts     │
│  7 panels        │ │                  │
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
docker compose up -d kafka cassandra redis

# 3. Vérifier que tout est prêt
docker compose logs --tail=5 kafka cassandra redis

# 4. Ouvrir 5 terminaux :

# Terminal 1 - Générer des transactions
python src/producer/transaction_generator.py

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

### Détection de fraude `src/flink_jobs/fraud_detection.py`
**Mode simple** (consumer Python) - 3 règles de détection :
| Règle | Seuil | Fenêtre |
|-------|-------|---------|
| Montant excessif | `amount > 10 000€` | Aucune (par transaction) |
| Vélocité anormale | `>= 3 tx en 5 min` | Glissante 5 min |
| Changement pays rapide | `>= 2 villes en 1h` | Glissante 1h |

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
| `GET /health` | Healthcheck |
| `GET /alerts/recent?limit=10` | N dernières alertes (de Redis) |

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
```

---

## Structure du projet

```
.
├── .env                          # Configuration / variables d'environnement
├── .gitignore
├── docker-compose.yml            # Infra complète (Kafka, Cassandra, Redis, Flink, MinIO, Grafana, Prometheus)
├── requirements.txt              # Dépendances Python
├── Dockerfile.metrics            # Image Docker pour le metrics exporter
├── submit_flink_job.sh           # Script de soumission du job PyFlink
│
├── src/
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
│   │   └── main.py                     # FastAPI (GET /health, /alerts/recent)
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
│       ├── test_rules.py               # 12 tests unitaires des règles
│       └── test_api.py                 # Tests API
│
├── flink-jars/                         # JAR Kafka SQL connector pour Flink
└── architecture.md                     # Schéma d'architecture (Mermaid)
```

---

## Tests

```bash
# Tests unitaires des règles de détection (12 tests)
python -m pytest src/tests/test_rules.py -v

# Tests API (nécessite Redis en cours)
python -m pytest src/tests/test_api.py -v
```

---

## Déploiement GitHub

```bash
# Depuis la racine du projet 1
git init
git add .
git commit -m "Initial commit - Plateforme detection fraude temps reel"
gh repo create fraud-detection-realtime --public --source=. --push
```

Ou créer le repo sur github.com puis :
```bash
git remote add origin https://github.com/<user>/fraud-detection-realtime.git
git push -u origin main
```

---

## Aller plus loin

Idées d'amélioration pour le portfolio :
- **Exactly-once semantics** avec Kafka transactions + Flink checkpointing
- **Schema Registry** (Avro/Protobuf) pour l'évolution des schémas
- **ML scoring** avec modèle ONNX dans Flink (remplace les seuils statiques)
- **Pattern CEP** (carding : petit montant test → gros retrait)
- **Tests de résilience** : kill un node Kafka, latency P99, recovery time
- **Débit 1000 msg/sec** : ajuster le producer

---

## Licence

MIT
