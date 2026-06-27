import json
import os
import time
from datetime import datetime, timezone
from collections import defaultdict

from confluent_kafka import Consumer, Producer

SERIALIZATION_FORMAT = os.getenv("SERIALIZATION_FORMAT", "json")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8085")

if SERIALIZATION_FORMAT == "avro":
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import SerializationContext, MessageField

KAFKA_BOOTSTRAP = f'{os.getenv("KAFKA_HOST", "localhost")}:{os.getenv("KAFKA_PORT", "9092")}'
INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "transactions")
OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "fraud-alerts")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "fraud-detector")


class FraudDetector:
    def __init__(self):
        self.card_history = defaultdict(list)
        self.card_geo = defaultdict(list)
        self.card_recent_small = defaultdict(list)

    def check_amount_threshold(self, tx):
        if tx.get("amount", 0) > 10000:
            return {
                "transaction_id": tx["transaction_id"],
                "card_id": tx["card_id"],
                "rule": "montant_excessif",
                "amount": tx["amount"],
                "timestamp": tx["timestamp"],
                "severity": "high"
            }
        return None

    def check_velocity(self, tx):
        card_id = tx["card_id"]
        now = time.time()
        self.card_history[card_id].append({
            "ts": now,
            "amount": tx.get("amount", 0),
            "city": tx.get("city", "")
        })
        recent = [h for h in self.card_history[card_id] if (now - h["ts"]) < 300]
        self.card_history[card_id] = recent
        if len(recent) >= 3:
            total = sum(h["amount"] for h in recent)
            return {
                "transaction_id": tx["transaction_id"],
                "card_id": card_id,
                "rule": "velocite_anormale",
                "nb_transactions": len(recent),
                "total_amount": round(total, 2),
                "timestamp": tx["timestamp"],
                "severity": "medium"
            }
        return None

    def check_geo_hopping(self, tx):
        card_id = tx["card_id"]
        now = time.time()
        self.card_geo[card_id].append({
            "ts": now,
            "city": tx.get("city", ""),
            "latitude": tx.get("latitude", 0),
            "longitude": tx.get("longitude", 0)
        })
        recent = [g for g in self.card_geo[card_id] if (now - g["ts"]) < 3600]
        self.card_geo[card_id] = recent
        cities = set(g["city"] for g in recent)
        if len(cities) >= 2:
            return {
                "transaction_id": tx["transaction_id"],
                "card_id": card_id,
                "rule": "changement_pays_rapide",
                "cities": list(cities),
                "timestamp": tx["timestamp"],
                "severity": "high"
            }
        return None

    def check_carding_pattern(self, tx):
        card_id = tx["card_id"]
        now = time.time()
        amount = tx.get("amount", 0)

        self.card_recent_small[card_id] = [
            s for s in self.card_recent_small[card_id] if (now - s["ts"]) < 600
        ]

        if amount < 5:
            self.card_recent_small[card_id].append({
                "ts": now,
                "transaction_id": tx["transaction_id"]
            })
            return None

        if amount > 1000 and self.card_recent_small[card_id]:
            return {
                "transaction_id": tx["transaction_id"],
                "card_id": card_id,
                "rule": "pattern_carding",
                "amount": amount,
                "small_tx_id": self.card_recent_small[card_id][-1]["transaction_id"],
                "timestamp": tx["timestamp"],
                "severity": "high"
            }

        return None

    def detect(self, tx):
        alerts = []
        for check in [self.check_amount_threshold, self.check_velocity, self.check_geo_hopping, self.check_carding_pattern]:
            alert = check(tx)
            if alert:
                alerts.append(alert)
        return alerts


def _create_avro_deserializer():
    client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    with open("src/schemas/transaction.avsc") as f:
        schema_str = f.read()
    return AvroDeserializer(client, schema_str)


_avro_deser = None

def _deserialize(msg_bytes):
    global _avro_deser
    if SERIALIZATION_FORMAT == "avro":
        if _avro_deser is None:
            _avro_deser = _create_avro_deserializer()
        ctx = SerializationContext(INPUT_TOPIC, MessageField.VALUE)
        return _avro_deser(msg_bytes, ctx)
    return json.loads(msg_bytes.decode("utf-8"))


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "latest"
    })
    consumer.subscribe([INPUT_TOPIC])

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    detector = FraudDetector()

    print(f"Detection de fraude en cours... (format={SERIALIZATION_FORMAT})\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Erreur: {msg.error()}")
                continue

            tx = _deserialize(msg.value())
            alerts = detector.detect(tx)

            for alert in alerts:
                producer.produce(
                    OUTPUT_TOPIC,
                    value=json.dumps(alert).encode("utf-8")
                )
                producer.poll(0)
                print(f"  [!] {alert['rule']} | carte={alert['card_id']} | "
                      f"{'amount='+str(alert.get('amount','')) if 'amount' in alert else ''}")

    except KeyboardInterrupt:
        print("\nArret du detecteur.")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
