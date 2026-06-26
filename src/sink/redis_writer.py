import json
import os

from confluent_kafka import Consumer
import redis

KAFKA_BOOTSTRAP = f'{os.getenv("KAFKA_HOST", "localhost")}:{os.getenv("KAFKA_PORT", "9092")}'
ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "fraud-alerts")
GROUP_ID = os.getenv("REDIS_GROUP_ID", "redis-writer")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_KEY = os.getenv("REDIS_ALERT_KEY", "fraud:alerts:recent")
MAX_ALERTS = int(os.getenv("REDIS_MAX_ALERTS", "100"))
ALERT_TTL = int(os.getenv("REDIS_ALERT_TTL", "3600"))


def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    print("Redis connecte.")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest"
    })
    consumer.subscribe([ALERT_TOPIC])

    print("Redis writer en ecoute... (CTRL+C pour arreter)\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Erreur Kafka: {msg.error()}")
                continue

            alert = json.loads(msg.value().decode("utf-8"))
            alert_str = json.dumps(alert)

            r.lpush(REDIS_KEY, alert_str)
            r.ltrim(REDIS_KEY, 0, MAX_ALERTS - 1)
            r.expire(REDIS_KEY, ALERT_TTL)

            print(f"  [Redis] Alerte mise en cache: {alert.get('rule')} | carte={alert.get('card_id')}")

    except KeyboardInterrupt:
        print("\nArret du Redis writer.")
    finally:
        consumer.close()
        r.close()


if __name__ == "__main__":
    main()
