# Architecture — Détection de Fraude Temps Réel

```mermaid
graph TB
    subgraph "Data Sources"
        P[Producer Python<br/>Faker + Transactions]
    end

    subgraph "Schema Management"
        SR[Schema Registry<br/>Avro Schemas]
    end

    subgraph "Messaging Layer"
        K[("Kafka<br/>(topic: transactions)")]
    end

    subgraph "Stream Processing"
        F[Flink Job<br/>4 règles de détection]
    end

    subgraph "Storage"
        C[("Cassandra<br/>Alertes durables")]
        R[("Redis<br/>100 dernières alertes")]
        M[("MinIO/S3<br/>Transactions brutes<br/>format Parquet")]
    end

    subgraph "Serving"
        A[API FastAPI<br/>GET /alerts/recent<br/>GET /alerts/stats<br/>GET /alerts/card/{id}<br/>GET /metrics]
    end

    subgraph "Monitoring"
        G[Grafana<br/>Dashboards]
    end

    P -->|JSON ou Avro| K
    SR -.->|Enregistrement schéma| P
    K -->|Stream| F
    F -->|4 règles: montant, vélocité,<br/>geo-hopping, carding| C
    F -->|Alertes récentes| R
    K -->|Consumer| M
    R -->|Lecture| A
    C -->|Requêtes| G
    A -->|JSON| G

    style P fill:#4CAF50,color:#fff
    style SR fill:#FFC107,color:#000
    style K fill:#2196F3,color:#fff
    style F fill:#FF9800,color:#fff
    style C fill:#9C27B0,color:#fff
    style R fill:#E91E63,color:#fff
    style M fill:#00BCD4,color:#fff
    style A fill:#673AB7,color:#fff
    style G fill:#795548,color:#fff
```

## Flux des données

1. **Producer** génère des transactions (support JSON ou Avro via `SERIALIZATION_FORMAT`)
2. **Schema Registry** gère les schémas Avro pour l'évolution des données
3. **Kafka** reçoit et bufferise les transactions
4. **Flink** lit les transactions en streaming et applique 4 règles :
   - Montant excessif (> 10 000€)
   - Vélocité anormale (>= 3 tx en 5 min)
   - Changement pays rapide (>= 2 villes en 1h)
   - Pattern carding (petit montant < 5€ puis gros > 1 000€ en 10 min)
5. **Cassandra** stocke les alertes de façon durable
6. **Redis** garde les 100 dernières alertes pour l'API temps réel
7. **MinIO** archive toutes les transactions brutes en Parquet
8. **FastAPI** sert 4 endpoints REST
9. **Grafana** visualise les métriques en temps réel

## Ports

| Service        | Port |
|----------------|------|
| Kafka          | 9092 |
| Schema Registry| 8085 |
| Cassandra      | 9042 |
| Redis          | 6379 |
| MinIO API      | 9002 |
| MinIO Console  | 9003 |
| Grafana        | 3001 |
| Prometheus     | 9091 |
| Metrics Export | 8001 |

## Décisions Architecturales (ADRs)

### ADR-1 : Sérialisation JSON par défaut, Avro optionnel
- **Contexte** : Les messages peuvent être sérialisés en JSON ou Avro
- **Décision** : JSON par défaut, Avro activé via `SERIALIZATION_FORMAT=avro`
- **Raison** : Simplicité en dev, scalabilité en prod

### ADR-2 : Règles de détection in-process
- **Contexte** : Les règles de détection sont simples et ne nécessitent pas un moteur externe
- **Décision** : Implémentées directement dans le consumer Python
- **Raison** : Pas de dépendance externe, latence minimale
