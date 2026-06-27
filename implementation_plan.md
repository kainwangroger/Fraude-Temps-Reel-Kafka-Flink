# Complétion du Projet 01 — Plateforme de Détection de Fraude Temps Réel

## Contexte

Le projet 01 est le plus avancé du portfolio (~60%). Le cœur fonctionnel est en place :
- ✅ Producer Kafka (transaction_generator.py)
- ✅ Détection de fraude — mode simple (Consumer Python) + mode prod (PyFlink Table API)
- ✅ 3 règles de détection (montant excessif, vélocité, geo-hopping)
- ✅ Sinks : Cassandra, Redis, MinIO (Parquet)
- ✅ API FastAPI (health + alerts/recent)
- ✅ Monitoring (Prometheus + Grafana + metrics_exporter)
- ✅ Tests unitaires (12 tests rules + tests API)
- ✅ Docker Compose complet

## Ce qui manque (selon les specs du README principal)

En comparant l'existant avec les livrables attendus dans le [README.md](file:///c:/Users/roger/Desktop/20-Projets-Portfolio/README.md#L11-L48), voici les manques identifiés :

---

## Proposed Changes

### Chantier 1 — Schema Registry Avro (Sérialisation des messages)

> Actuellement les messages sont en JSON brut. Le README principal demande "Schema Registry Avro/Protobuf pour l'évolution des schémas".

#### [NEW] [schemas/](file:///c:/Users/roger/Desktop/20-Projets-Portfolio/01-DataEngineering/01-Fraude-Temps-Reel-Kafka-Flink/src/schemas)

- Créer `src/schemas/transaction.avsc` — schéma Avro de la transaction
- Créer `src/schemas/fraud_alert.avsc` — schéma Avro de l'alerte de fraude

#### [MODIFY] [docker-compose.yml](file:///c:/Users/roger/Desktop/20-Projets-Portfolio/01-DataEngineering/01-Fraude-Temps-Reel-Kafka-Flink/docker-compose.yml)

- Ajouter le service `schema-registry` (Confluent Schema Registry, port 8085)

#### [MODIFY] [transaction_generator.py](file:///c:/Users/roger/Desktop/20-Projets-Portfolio/01-DataEngineering/01-Fraude-Temps-Reel-Kafka-Flink/src/producer/transaction_generator.py)

- Ajouter la sérialisation Avro via `confluent_kafka.schema_registry`
- Enregistrement automatique du schéma au démarrage
- Fallback JSON si schema-registry indisponible

#### [MODIFY] [fraud_detection.py](file:///c:/Users/roger/Desktop/20-Projets-Portfolio/01-DataEngineering/01-Fraude-Temps-Reel-Kafka-Flink/src/flink_jobs/fraud_detection.py)

- Ajouter la désérialisation Avro côté consumer

#### [MODIFY] [requirements.txt](file:///c:/Users/roger/Desktop/20-Projets-Portfolio/01-DataEngineering/01-Fraude-Temps-Reel-Kafka-Flink/requirements.txt)

- Ajouter `confluent-kafka[avro]`, `fastavro`

---

### Chantier 2 — Règle CEP : Pattern de Carding

> Le README principal mentionne "CEP - Complex Event Processing : même carte utilisée dans 2 pays différents en 5min" et le README du projet mentionne dans "Aller plus loin" un pattern de carding (petit montant test → gros retrait).

#### [MODIFY] [fraud_detection.py](file:///c:/Users/roger/Desktop/20-Projets-Portfolio/01-DataEngineering/01-Fraude-Temps-Reel-Kafka-Flink/src/flink_jobs/fraud_detection.py)

- Ajouter `check_carding_pattern()` : détecte une transaction < 5€ suivie d'une transaction > 1000€ sur la même carte en < 10 minutes

#### [MODIFY] [fraud_detection_flink.py](file:///c:/Users/roger/Desktop/20-Projets-Portfolio/01-DataEngineering/01-Fraude-Temps-Reel-Kafka-Flink/src/flink_jobs/fraud_detection_flink.py)

- Ajouter la règle SQL équivalente en mode Flink

#### [MODIFY] [test_rules.py](file:///c:/Users/roger/Desktop/20-Projets-Portfolio/01-DataEngineering/01-Fraude-Temps-Reel-Kafka-Flink/src/tests/test_rules.py)

- Ajouter `TestCardingPattern` (3-4 tests : pattern détecté, pas de petit montant, timeout dépassé)

---

### Chantier 3 — API REST Enrichie

> L'API actuelle n'a que 2 endpoints (`/health`, `/alerts/recent`). Le README principal demande une API plus riche avec consultation par carte, statistiques, etc.

#### [MODIFY] [main.py](file:///c:/Users/roger/Desktop/20-Projets-Portfolio/01-DataEngineering/01-Fraude-Temps-Reel-Kafka-Flink/src/api/main.py)

Ajouter les endpoints :
- `GET /alerts/stats` — statistiques globales (total alertes, par règle, par sévérité)
- `GET /alerts/card/{card_id}` — historique des alertes pour une carte (Cassandra)
- `GET /metrics` — métriques opérationnelles (throughput, latence, uptime)
- Middleware CORS pour l'intégration dashboard

#### [MODIFY] [requirements.txt](file:///c:/Users/roger/Desktop/20-Projets-Portfolio/01-DataEngineering/01-Fraude-Temps-Reel-Kafka-Flink/requirements.txt)

- Ajouter `cassandra-driver` dans les dépendances API (déjà présent globalement)

---

### Chantier 4 — Tests d'Intégration

> Aucun test d'intégration n'existe — les tests actuels sont uniquement unitaires.

#### [NEW] [test_integration.py](file:///c:/Users/roger/Desktop/20-Projets-Portfolio/01-DataEngineering/01-Fraude-Temps-Reel-Kafka-Flink/src/tests/test_integration.py)

Tests d'intégration (mockés, pas besoin d'infra réelle) :
- Test du pipeline complet : producer → detection → alert émise
- Test du cassandra_writer : vérifier le format d'écriture
- Test du redis_writer : vérifier push/trim/TTL
- Test du minio_writer : vérifier le batching Parquet

---

### Chantier 5 — Script de Benchmark & Rapport de Performance

> Le README principal demande : "Rapport de performance : throughput, P99 latency, recovery time après crash"

#### [NEW] [benchmark.py](file:///c:/Users/roger/Desktop/20-Projets-Portfolio/01-DataEngineering/01-Fraude-Temps-Reel-Kafka-Flink/scripts/benchmark.py)

Script qui :
- Génère N transactions à débit croissant (100, 500, 1000, 5000/sec)
- Mesure la latence de bout en bout (production → détection → alerte Redis)
- Calcule P50, P95, P99, throughput max
- Écrit les résultats dans `reports/benchmark_results.json`

#### [NEW] [PERFORMANCE.md](file:///c:/Users/roger/Desktop/20-Projets-Portfolio/01-DataEngineering/01-Fraude-Temps-Reel-Kafka-Flink/PERFORMANCE.md)

Document de rapport de performance avec :
- Méthodologie du benchmark
- Résultats attendus par composant
- Bottlenecks identifiés et recommandations

---

### Chantier 6 — CI/CD Pipeline (GitHub Actions)

> Aucun pipeline CI/CD n'existe dans le projet.

#### [NEW] [ci.yml](file:///c:/Users/roger/Desktop/20-Projets-Portfolio/01-DataEngineering/01-Fraude-Temps-Reel-Kafka-Flink/.github/workflows/ci.yml)

Pipeline GitHub Actions :
- **Lint** : `ruff check` sur tout le code Python
- **Tests** : `pytest` unitaires (pas d'infra requise)
- **Docker Build** : vérifier que le docker-compose build passe
- Déclenchement sur push/PR vers `main`

#### [NEW] [.ruff.toml](file:///c:/Users/roger/Desktop/20-Projets-Portfolio/01-DataEngineering/01-Fraude-Temps-Reel-Kafka-Flink/.ruff.toml)

Configuration du linter Ruff

---

### Chantier 7 — Documentation Finale

#### [MODIFY] [architecture.md](file:///c:/Users/roger/Desktop/20-Projets-Portfolio/01-DataEngineering/01-Fraude-Temps-Reel-Kafka-Flink/architecture.md)

- Enrichir le diagramme Mermaid avec le Schema Registry
- Ajouter un diagramme de séquence du flux de détection
- Documenter les décisions architecturales (ADRs)

#### [MODIFY] [README.md](file:///c:/Users/roger/Desktop/20-Projets-Portfolio/01-DataEngineering/01-Fraude-Temps-Reel-Kafka-Flink/README.md)

- Mettre à jour la structure du projet avec les nouveaux fichiers
- Ajouter les nouveaux endpoints API
- Ajouter la section Benchmark
- Ajouter le badge CI

---

## Résumé des fichiers

| Action | Fichier | Chantier |
|--------|---------|----------|
| NEW | `src/schemas/transaction.avsc` | 1 - Schema Registry |
| NEW | `src/schemas/fraud_alert.avsc` | 1 - Schema Registry |
| MODIFY | `docker-compose.yml` | 1 - Schema Registry |
| MODIFY | `src/producer/transaction_generator.py` | 1 - Schema Registry |
| MODIFY | `src/flink_jobs/fraud_detection.py` | 1+2 - Avro + Carding |
| MODIFY | `src/flink_jobs/fraud_detection_flink.py` | 2 - Carding |
| MODIFY | `src/api/main.py` | 3 - API enrichie |
| MODIFY | `src/tests/test_rules.py` | 2 - Tests carding |
| NEW | `src/tests/test_integration.py` | 4 - Tests intégration |
| NEW | `scripts/benchmark.py` | 5 - Benchmark |
| NEW | `PERFORMANCE.md` | 5 - Performance |
| NEW | `.github/workflows/ci.yml` | 6 - CI/CD |
| NEW | `.ruff.toml` | 6 - CI/CD |
| MODIFY | `architecture.md` | 7 - Documentation |
| MODIFY | `README.md` | 7 - Documentation |
| MODIFY | `requirements.txt` | 1+3 - Dépendances |

---

## Open Questions

> [!IMPORTANT]
> **Mode Avro** : Faut-il que le producer supporte les deux modes (JSON + Avro) via une variable d'environnement `SERIALIZATION_FORMAT=avro|json`, ou on passe tout en Avro et on supprime le support JSON ?

> [!IMPORTANT]
> **Règle de carding** : Le seuil proposé est "petit montant < 5€ suivi de gros > 1000€ en < 10min". Ces seuils te conviennent, ou tu veux les ajuster ?

---

## Verification Plan

### Automated Tests
```bash
# Tests unitaires existants + nouveaux
python -m pytest src/tests/ -v --tb=short

# Lint
ruff check src/
```

### Manual Verification
- Vérifier que `docker compose config` est valide avec le schema-registry ajouté
- Vérifier que le workflow CI est syntaxiquement correct
- Vérifier la cohérence des schémas Avro avec les structures JSON existantes
