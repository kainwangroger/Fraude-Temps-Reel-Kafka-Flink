import json
import os
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP = f'{os.getenv("KAFKA_HOST", "localhost")}:{os.getenv("KAFKA_PORT", "9092")}'
ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "fraud-alerts")

SEVERITY_COLORS = {
    "high": "\033[91m",
    "medium": "\033[93m",
    "low": "\033[94m"
}
RESET = "\033[0m"


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "alert-viewer",
        "auto.offset.reset": "earliest"
    })
    consumer.subscribe([ALERT_TOPIC])

    print("En attente d'alertes de fraude... (CTRL+C pour arreter)\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Erreur: {msg.error()}")
                continue

            alert = json.loads(msg.value().decode("utf-8"))
            color = SEVERITY_COLORS.get(alert.get("severity", "low"), "")
            sev = alert["severity"].upper()

            print(f"{color}[{sev}] Fraude detectee{RESET}")
            print(f"  Transaction: {alert['transaction_id'][:8]}")
            print(f"  Carte: {alert['card_id']}")
            print(f"  Regle: {alert['rule']}")

            if "amount" in alert:
                print(f"  Montant: {alert['amount']:.2f} EUR")
            if "cities" in alert:
                print(f"  Villes: {', '.join(alert['cities'])}")
            if "total_amount" in alert:
                print(f"  Total periodique: {alert['total_amount']:.2f} EUR")
                print(f"  Transactions en 5min: {alert['nb_transactions']}")
            print()

    except KeyboardInterrupt:
        print("\nArrete.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
