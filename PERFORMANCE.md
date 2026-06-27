# Rapport de Performance — Pipeline de Détection de Fraude

## Méthodologie

- **Outil** : `scripts/benchmark.py`
- **Scénario** : Génération de transactions à débit croissant (100, 500, 1000, 5000 tx/sec)
- **Mesure** : Latence de bout en bout (production Kafka → détection → alerte Redis)
- **Durée** : 30 secondes par palier de débit
- **Infrastructure** : Docker Compose local (single node)

## Résultats Attendus

| Débit (tx/sec) | P50 (ms) | P95 (ms) | P99 (ms) | Throughput Effectif |
|---|---|---|---|---|
| 100 | < 50 | < 100 | < 150 | ~100 tx/sec |
| 500 | < 100 | < 200 | < 300 | ~480 tx/sec |
| 1000 | < 150 | < 350 | < 500 | ~900 tx/sec |
| 5000 | < 300 | < 800 | < 1200 | ~4000 tx/sec |

## Composants Mesurés

| Composant | Latence Attendue | Goulot d'Étranglement Potentiel |
|---|---|---|
| Producer → Kafka | < 10ms | `acks=all`, réseau |
| Kafka → Consumer (détection) | < 50ms | `poll()` interval, traitement règles |
| Détection → Alerte Redis | < 20ms | Redis LPUSH, réseau |
| Détection → Cassandra | < 50ms | Write consistency level |
| Détection → MinIO (batch) | < 500ms | Taille de batch, I/O disque |
| API → Redis (lecture) | < 5ms | Taille liste, volume concurrent |

## SLA Cibles

| Métrique | Cible |
|---|---|
| Latence P99 (bout en bout) | < 500ms |
| Throughput max | 5000+ tx/sec |
| Taux de détection | 100% des transactions analysées |
| Taux de faux positifs | < 5% |
| Recovery time après crash | < 30s |

## Bottlenecks Identifiés

1. **Kafka** : `acks=all` et réplication factor > 1 impacteront la latence
2. **Cassandra** : écriture synchronisée, consistency level QUORUM
3. **MinIO** : batch de 100 transactions — compromis entre fraîcheur et performance
4. **Consumer unique** : le `FraudDetector` tourne en single thread

## Recommandations

- Passage à Kafka avec `acks=1` pour les workloads non-critiques
- Partitionning du topic Kafka (plus de consommateurs parallèles)
- Redis Cluster pour le cache d'alertes à très haute vélocité
- Batching adaptatif pour MinIO (taille variable selon débit)
