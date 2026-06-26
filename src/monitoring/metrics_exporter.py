import json
import os
import time
from collections import defaultdict

import redis
from prometheus_client import start_http_server, Gauge, Counter, Histogram

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_KEY = os.getenv("REDIS_ALERT_KEY", "fraud:alerts:recent")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))

alert_count = Counter("fraud_alerts_total", "Total des alertes", ["rule", "severity"])
alert_rate = Gauge("fraud_alerts_per_minute", "Alertes par minute", ["rule"])
cards_flagged = Gauge("fraud_cards_flagged", "Nombre de cartes flaggees")

latency_histogram = Histogram(
    "fraud_detection_latency_seconds",
    "Latence de detection (secondes)",
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60]
)

last_minute = defaultdict(list)


def track_alert(alert):
    rule = alert.get("rule", "unknown")
    severity = alert.get("severity", "low")
    alert_count.labels(rule=rule, severity=severity).inc()
    now = time.time()
    last_minute[rule].append(now)
    ts = alert.get("timestamp", "")
    if ts:
        try:
            tx_time = time.mktime(time.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S"))
            latency = now - tx_time
            latency_histogram.observe(max(latency, 0))
        except (ValueError, OSError):
            pass


def refresh_gauges(r):
    for rule in list(last_minute.keys()):
        cutoff = time.time() - 60
        last_minute[rule] = [t for t in last_minute[rule] if t > cutoff]
        alert_rate.labels(rule=rule).set(len(last_minute[rule]))

    alerts = r.lrange(REDIS_KEY, 0, -1)
    card_ids = set()
    for a in alerts:
        try:
            data = json.loads(a)
            card_ids.add(data.get("card_id"))
        except json.JSONDecodeError:
            pass
    cards_flagged.set(len(card_ids))


def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    print("Metrics exporter connecte a Redis.")

    start_http_server(METRICS_PORT)
    print(f"Metrics HTTP sur port {METRICS_PORT}")

    try:
        while True:
            alerts = r.lrange(REDIS_KEY, 0, -1)
            for a in alerts:
                try:
                    track_alert(json.loads(a))
                except json.JSONDecodeError:
                    pass
            refresh_gauges(r)
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nArret du metrics exporter.")


if __name__ == "__main__":
    main()
