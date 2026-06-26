import json
import pytest
from fastapi.testclient import TestClient
from api.main import app


client = TestClient(app)


@pytest.fixture(autouse=True)
async def clear_redis():
    await app.state.redis.flushdb()
    yield


class TestHealth:
    def test_health_endpoint(self):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


class TestRecentAlerts:
    def test_empty_returns_no_alerts(self):
        response = client.get("/alerts/recent")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 0
        assert data["alerts"] == []

    def test_with_alerts_in_redis(self):
        alert = {"rule": "montant_excessif", "card_id": 12345, "severity": "high"}
        import anyio
        anyio.run(app.state.redis.lpush, "fraud:alerts:recent", json.dumps(alert))

        response = client.get("/alerts/recent?limit=10")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] >= 1

    def test_limit_parameter(self):
        response = client.get("/alerts/recent?limit=5")
        assert response.status_code == 200

    def test_limit_max_100(self):
        response = client.get("/alerts/recent?limit=200")
        assert response.status_code == 422

    def test_limit_min_1(self):
        response = client.get("/alerts/recent?limit=0")
        assert response.status_code == 422
