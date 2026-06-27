import json
import os
import time
from collections import Counter

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as aioredis
from cassandra.cluster import Cluster

app = FastAPI(title="Fraud Detection API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_KEY = os.getenv("REDIS_ALERT_KEY", "fraud:alerts:recent")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "fraud_detection")

START_TIME = time.time()


@app.on_event("startup")
async def startup():
    app.state.redis = await aioredis.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True
    )
    app.state.cassandra = Cluster([CASSANDRA_HOST]).connect()
    app.state.start_time = START_TIME


@app.on_event("shutdown")
async def shutdown():
    await app.state.redis.close()
    app.state.cassandra.shutdown()


@app.get("/health")
async def health():
    return {"status": "ok", "uptime_seconds": round(time.time() - app.state.start_time, 2)}


@app.get("/alerts/recent")
async def get_recent_alerts(limit: int = Query(10, ge=1, le=100)):
    alerts_json = await app.state.redis.lrange(REDIS_KEY, 0, limit - 1)
    alerts = [json.loads(a) for a in alerts_json]
    return {"count": len(alerts), "alerts": alerts}


@app.get("/alerts/stats")
async def get_alert_stats():
    alerts_json = await app.state.redis.lrange(REDIS_KEY, 0, -1)
    alerts = [json.loads(a) for a in alerts_json]

    by_rule = Counter(a.get("rule", "unknown") for a in alerts)
    by_severity = Counter(a.get("severity", "unknown") for a in alerts)
    card_ids = set(a.get("card_id") for a in alerts if a.get("card_id"))

    return {
        "total_alerts": len(alerts),
        "by_rule": dict(by_rule),
        "by_severity": dict(by_severity),
        "unique_cards_flagged": len(card_ids)
    }


@app.get("/alerts/card/{card_id}")
async def get_card_alerts(card_id: int, limit: int = Query(20, ge=1, le=100)):
    rows = app.state.cassandra.execute(
        f"SELECT * FROM {CASSANDRA_KEYSPACE}.fraud_alerts WHERE card_id = %s LIMIT %s",
        (card_id, limit)
    )
    alerts = []
    for row in rows:
        alerts.append({
            "alert_id": str(row.alert_id),
            "transaction_id": row.transaction_id,
            "rule_name": row.rule_name,
            "amount": row.amount,
            "city": row.city,
            "severity": row.severity,
            "detected_at": str(row.detected_at)
        })
    return {"card_id": card_id, "count": len(alerts), "alerts": alerts}


@app.get("/metrics")
async def get_metrics():
    alerts_json = await app.state.redis.lrange(REDIS_KEY, 0, -1)
    recent_count = len(alerts_json)
    uptime = round(time.time() - app.state.start_time, 2)

    return {
        "uptime_seconds": uptime,
        "alerts_in_cache": recent_count,
        "throughput_alerts_per_min": round(recent_count / max(uptime / 60, 1), 2),
        "status": "healthy"
    }
