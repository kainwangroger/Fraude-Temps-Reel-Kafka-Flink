# Architecture — Détection de Fraude Temps Réel

```mermaid
graph TB
    subgraph "Data Sources"
        P[Producer Python<br/>Faker + Transactions]
    end

    subgraph "Messaging Layer"
        K[("Kafka<br/>(topic: transactions)")]
    end

    subgraph "Stream Processing"
        F[Flink Job<br/>Règles de détection]
    end

    subgraph "Storage"
        C[("Cassandra<br/>Alertes durables")]
        R[("Redis<br/>100 dernières alertes")]
        M[("MinIO/S3<br/>Transactions brutes<br/>format Parquet")]
    end

    subgraph "Serving"
        A[API FastAPI<br/>GET /alerts]
    end

    subgraph "Monitoring"
        G[Grafana<br/>Dashboards]
    end

    P -->|Transactions JSON| K
    K -->|Stream| F
    F -->|Alertes| C
    F -->|Alertes récentes| R
    K -->|Consumer| M
    R -->|Lecture| A
    C -->|Requêtes| G
    A -->|JSON| G

    style P fill:#4CAF50,color:#fff
    style K fill:#2196F3,color:#fff
    style F fill:#FF9800,color:#fff
    style C fill:#9C27B0,color:#fff
    style R fill:#E91E63,color:#fff
    style M fill:#00BCD4,color:#fff
    style A fill:#673AB7,color:#fff
    style G fill:#795548,color:#fff
```

## Flux des données

1. **Producer** génère des transactions financières (95% normales, 5% frauduleuses)
2. **Kafka** reçoit et bufferise les transactions
3. **Flink** (prochaine étape) lit les transactions en streaming et applique les règles
4. **Cassandra** stocke les alertes de façon durable
5. **Redis** garde les 100 dernières alertes pour l'API temps réel
6. **MinIO** archive toutes les transactions brutes en Parquet pour rejeu et training ML
7. **FastAPI** sert les alertes via une API REST
8. **Grafana** visualise les métriques en temps réel

## Ports

| Service       | Port |
| ------------- | ---- |
| Kafka         | 9092 |
| Cassandra     | 9042 |
| Redis         | 6379 |
| MinIO API     | 9002 |
| MinIO Console | 9003 |
| Grafana       | 3001 |
