import json
import os

from fastapi import FastAPI, Query
import redis.asyncio as aioredis

app = FastAPI(title="Fraud Detection API", version="1.0.0")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_KEY = os.getenv("REDIS_ALERT_KEY", "fraud:alerts:recent")


@app.on_event("startup")
async def startup():
    app.state.redis = await aioredis.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True
    )


@app.on_event("shutdown")
async def shutdown():
    await app.state.redis.close()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/alerts/recent")
async def get_recent_alerts(limit: int = Query(10, ge=1, le=100)):
    alerts_json = await app.state.redis.lrange(REDIS_KEY, 0, limit - 1)
    alerts = [json.loads(a) for a in alerts_json]
    return {
        "count": len(alerts),
        "alerts": alerts
    }
