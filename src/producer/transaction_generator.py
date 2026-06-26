import json
import os
import uuid
import time
from datetime import datetime, timezone

from faker import Faker
from confluent_kafka import Producer

fake = Faker("fr_FR")

TRANSACTION_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_BOOTSTRAP_SERVERS = f'{os.getenv("KAFKA_HOST", "localhost")}:{os.getenv("KAFKA_PORT", "9092")}'

FRAUD_PROBABILITY = 0.05

MERCHANTS = [
    "Amazon", "Carrefour", "SNCF", "TotalEnergies", "Apple Store",
    "Decathlon", "Fnac", "Leclerc", "Boulanger", "Sephora",
    "Airbnb", "Uber", "Deliveroo", "Netflix", "Spotify"
]

CITIES = [
    ("Paris", 48.8566, 2.3522),
    ("Lyon", 45.7640, 4.8357),
    ("Marseille", 43.2965, 5.3698),
    ("Bordeaux", 44.8378, -0.5792),
    ("Lille", 50.6292, 3.0573),
    ("Toulouse", 43.6047, 1.4442),
    ("Nice", 43.7102, 7.2620),
    ("New York", 40.7128, -74.0060),
    ("London", 51.5074, -0.1278),
    ("Tokyo", 35.6762, 139.6503)
]


def generate_transaction(is_fraud_forced: bool = False):
    is_fraud = is_fraud_forced or (fake.random.random() < FRAUD_PROBABILITY)
    city, lat, lon = fake.random.choice(CITIES)

    if is_fraud:
        amount = round(fake.random.uniform(500, 25000), 2)
    else:
        amount = round(fake.random.uniform(1, 500), 2)

    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "card_id": fake.random.randint(1000000000, 9999999999),
        "amount": amount,
        "currency": "EUR",
        "merchant": fake.random.choice(MERCHANTS),
        "city": city,
        "latitude": lat,
        "longitude": lon,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device_id": fake.random.randint(10000000, 99999999),
        "ip_address": fake.ipv4(),
        "is_fraud": is_fraud
    }
    return transaction


def delivery_report(err, msg):
    if err is not None:
        print(f"Erreur d'envoi: {err}")
    else:
        print(f"  -> topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    print(f"Envoi de transactions vers Kafka ({KAFKA_BOOTSTRAP_SERVERS})...")
    print("CTRL+C pour arreter\n")

    try:
        while True:
            tx = generate_transaction()
            producer.produce(
                TRANSACTION_TOPIC,
                value=json.dumps(tx).encode("utf-8"),
                callback=delivery_report
            )
            producer.poll(0)
            fraud_tag = " [FRAUDE]" if tx["is_fraud"] else ""
            print(f"  {tx['transaction_id'][:8]} | {tx['amount']:>8.2f} EUR | "
                  f"{tx['merchant']:<14} | {tx['city']:<12}{fraud_tag}")

            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\nArret du producer.")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
