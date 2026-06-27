import time
import pytest
from flink_jobs.fraud_detection import FraudDetector


def make_tx(amount=50, card_id=12345, city="Paris", lat=48.8566, lon=2.3522):
    return {
        "transaction_id": "tx-001",
        "card_id": card_id,
        "amount": amount,
        "city": city,
        "latitude": lat,
        "longitude": lon,
        "timestamp": "2024-01-01T12:00:00",
    }


class TestAmountThreshold:
    def test_high_amount_triggers_alert(self):
        detector = FraudDetector()
        tx = make_tx(amount=15000)
        alert = detector.check_amount_threshold(tx)
        assert alert is not None
        assert alert["rule"] == "montant_excessif"
        assert alert["severity"] == "high"
        assert alert["amount"] == 15000

    def test_low_amount_no_alert(self):
        detector = FraudDetector()
        tx = make_tx(amount=50)
        alert = detector.check_amount_threshold(tx)
        assert alert is None

    def test_boundary_no_alert(self):
        detector = FraudDetector()
        tx = make_tx(amount=10000)
        alert = detector.check_amount_threshold(tx)
        assert alert is None


class TestVelocity:
    def test_few_transactions_no_alert(self):
        detector = FraudDetector()
        tx = make_tx(card_id=1)
        alert = detector.check_velocity(tx)
        assert alert is None

    def test_three_transactions_triggers_alert(self):
        detector = FraudDetector()
        for _ in range(2):
            detector.check_velocity(make_tx(card_id=1))
        alert = detector.check_velocity(make_tx(card_id=1))
        assert alert is not None
        assert alert["rule"] == "velocite_anormale"
        assert alert["nb_transactions"] >= 3

    def test_old_transactions_are_ignored(self):
        detector = FraudDetector()
        tx = make_tx(card_id=1)
        detector.card_history[1] = [
            {"ts": time.time() - 400, "amount": 10, "city": "Paris"},
            {"ts": time.time() - 350, "amount": 20, "city": "Paris"},
        ]
        alert = detector.check_velocity(tx)
        assert alert is None


class TestGeoHopping:
    def test_same_city_no_alert(self):
        detector = FraudDetector()
        for _ in range(3):
            detector.check_geo_hopping(make_tx(card_id=1, city="Paris"))
        alert = detector.check_geo_hopping(make_tx(card_id=1, city="Paris"))
        assert alert is None

    def test_two_cities_triggers_alert(self):
        detector = FraudDetector()
        detector.check_geo_hopping(make_tx(card_id=1, city="Paris"))
        alert = detector.check_geo_hopping(make_tx(card_id=1, city="New York"))
        assert alert is not None
        assert alert["rule"] == "changement_pays_rapide"
        assert "Paris" in alert["cities"]
        assert "New York" in alert["cities"]

    def test_old_geo_expired(self):
        detector = FraudDetector()
        detector.card_geo[1] = [
            {"ts": time.time() - 7200, "city": "Tokyo", "latitude": 0, "longitude": 0},
        ]
        alert = detector.check_geo_hopping(make_tx(card_id=1, city="Paris"))
        assert alert is None


class TestDetect:
    def test_detect_returns_all_alerts(self):
        detector = FraudDetector()
        tx = make_tx(amount=15000, card_id=1)
        alerts = detector.detect(tx)
        assert len(alerts) >= 1
        rules = [a["rule"] for a in alerts]
        assert "montant_excessif" in rules

    def test_detect_returns_empty_for_normal(self):
        detector = FraudDetector()
        tx = make_tx(amount=50, card_id=1, city="Paris")
        alerts = detector.detect(tx)
        assert alerts == []


class TestCardingPattern:
    def test_small_then_large_triggers_alert(self):
        detector = FraudDetector()
        detector.check_carding_pattern(make_tx(amount=4, card_id=1))
        alert = detector.check_carding_pattern(make_tx(amount=1500, card_id=1))
        assert alert is not None
        assert alert["rule"] == "pattern_carding"
        assert alert["severity"] == "high"

    def test_no_small_tx_no_alert(self):
        detector = FraudDetector()
        alert = detector.check_carding_pattern(make_tx(amount=1500, card_id=2))
        assert alert is None

    def test_small_tx_timeout(self):
        detector = FraudDetector()
        detector.card_recent_small[1] = [
            {"ts": time.time() - 700, "transaction_id": "tx-small"}
        ]
        alert = detector.check_carding_pattern(make_tx(amount=1500, card_id=1))
        assert alert is None

    def test_large_tx_not_enough(self):
        detector = FraudDetector()
        alert = detector.check_carding_pattern(make_tx(amount=500, card_id=1))
        assert alert is None


class TestCardHistory:
    def test_card_history_trimmed(self):
        detector = FraudDetector()
        for i in range(10):
            detector.check_velocity(make_tx(card_id=1, amount=10))
        assert len(detector.card_history[1]) <= 10
