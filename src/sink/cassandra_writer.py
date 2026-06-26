import json
import os
import uuid
from datetime import datetime

from confluent_kafka import Consumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

KAFKA_BOOTSTRAP = f'{os.getenv("KAFKA_HOST", "localhost")}:{os.getenv("KAFKA_PORT", "9092")}'
ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "fraud-alerts")
GROUP_ID = os.getenv("CASSANDRA_GROUP_ID", "cassandra-writer")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "fraud_detection")


def ensure_schema(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS fraud_detection
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS fraud_detection.fraud_alerts (
            alert_id UUID PRIMARY KEY,
            transaction_id text,
            card_id bigint,
            rule_name text,
            amount double,
            city text,
            latitude double,
            longitude double,
            severity text,
            detected_at timestamp
        )
    """)
    session.execute("""
        CREATE INDEX IF NOT EXISTS idx_card_id ON fraud_detection.fraud_alerts (card_id)
    """)
    session.execute("""
        CREATE INDEX IF NOT EXISTS idx_severity ON fraud_detection.fraud_alerts (severity)
    """)


def main():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()
    ensure_schema(session)
    print("Schema Cassandra initialise.")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest"
    })
    consumer.subscribe([ALERT_TOPIC])

    insert_stmt = session.prepare("""
        INSERT INTO fraud_detection.fraud_alerts
            (alert_id, transaction_id, card_id, rule_name, amount, city, latitude, longitude, severity, detected_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    print("Cassandra writer en ecoute... (CTRL+C pour arreter)\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Erreur Kafka: {msg.error()}")
                continue

            alert = json.loads(msg.value().decode("utf-8"))
            ts_str = alert.get("timestamp", "")
            try:
                detected_at = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                detected_at = datetime.utcnow()

            session.execute(insert_stmt, (
                uuid.uuid4(),
                alert.get("transaction_id", ""),
                alert.get("card_id", 0),
                alert.get("rule", ""),
                alert.get("amount", 0.0),
                alert.get("city", ""),
                alert.get("latitude", 0.0),
                alert.get("longitude", 0.0),
                alert.get("severity", "low"),
                detected_at
            ))
            print(f"  [Cassandra] Alerte sauvegardee: {alert.get('rule')} | carte={alert.get('card_id')}")

    except KeyboardInterrupt:
        print("\nArret du Cassandra writer.")
    finally:
        consumer.close()
        cluster.shutdown()


if __name__ == "__main__":
    main()
