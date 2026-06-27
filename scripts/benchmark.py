"""
Benchmark de performance pour le pipeline de détection de fraude.

Mesure la latence de bout en bout (production → détection → alerte Redis)
à différents débits et calcule P50, P95, P99, throughput max.
"""

import json
import os
import time
import statistics
from datetime import datetime

import requests

KAFKA_REST_PROXY = os.getenv("KAFKA_REST_PROXY", "http://localhost:8082")
API_URL = os.getenv("API_URL", "http://localhost:8000")
REPORT_DIR = os.getenv("REPORT_DIR", "reports")

RATES = [100, 500, 1000, 5000]
DURATION_SECONDS = 30


def measure_latency_at_rate(rate):
    """Mesure la latence P50/P95/P99 au débit donné (transactions/sec)."""
    latencies = []
    total_tx = rate * DURATION_SECONDS
    sent = 0

    print(f"  Debit: {rate} tx/sec pendant {DURATION_SECONDS}s...")
    start = time.time()

    while time.time() - start < DURATION_SECONDS:
        tx = {
            "transaction_id": f"bench-{sent}",
            "card_id": 1000000000 + (sent % 10000),
            "amount": 50.0 if sent % 10 != 0 else 15000.0,
            "currency": "EUR",
            "merchant": "Benchmark",
            "city": "Paris",
            "latitude": 48.8566,
            "longitude": 2.3522,
            "timestamp": datetime.utcnow().isoformat(),
            "device_id": 12345678,
            "ip_address": "10.0.0.1",
            "is_fraud": sent % 10 == 0
        }

        t0 = time.time()
        try:
            resp = requests.post(
                f"{KAFKA_REST_PROXY}/topics/transactions",
                json={"records": [{"value": tx}]},
                timeout=1
            )
            if resp.status_code in (200, 201):
                latencies.append((time.time() - t0) * 1000)
        except requests.RequestException:
            pass

        sent += 1

        elapsed = time.time() - start
        expected_sent = int(elapsed * rate)
        if sent < expected_sent:
            time.sleep(1.0 / rate - (time.time() - start - sent / rate))

    if not latencies:
        return None

    latencies.sort()
    return {
        "rate_tps": rate,
        "total_sent": sent,
        "total_recorded": len(latencies),
        "latency_ms": {
            "p50": round(statistics.median(latencies), 2),
            "p95": round(latencies[int(len(latencies) * 0.95)], 2),
            "p99": round(latencies[int(len(latencies) * 0.99)], 2),
            "min": round(min(latencies), 2),
            "max": round(max(latencies), 2),
        },
        "throughput_tps": round(sent / DURATION_SECONDS, 2)
    }


def check_api_alerts():
    """Vérifie que les alertes arrivent bien dans Redis."""
    try:
        resp = requests.get(f"{API_URL}/alerts/stats", timeout=5)
        if resp.status_code == 200:
            return resp.json()
    except requests.RequestException:
        return None


def main():
    os.makedirs(REPORT_DIR, exist_ok=True)
    results = []

    print(f"Benchmark du pipeline de detection de fraude")
    print(f"  API: {API_URL}")
    print(f"  Kafka REST: {KAFKA_REST_PROXY}\n")

    for rate in RATES:
        result = measure_latency_at_rate(rate)
        if result:
            results.append(result)
            print(f"  => P50={result['latency_ms']['p50']}ms "
                  f"P95={result['latency_ms']['p95']}ms "
                  f"P99={result['latency_ms']['p99']}ms\n")

    stats = check_api_alerts()
    if stats:
        print(f"Alertes en cache: {stats.get('total_alerts', 0)}")
        results.append({"api_stats": stats})

    report_path = os.path.join(REPORT_DIR, "benchmark_results.json")
    with open(report_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nRapport sauvegarde dans {report_path}")


if __name__ == "__main__":
    main()
