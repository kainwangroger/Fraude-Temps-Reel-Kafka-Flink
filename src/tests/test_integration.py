import json
import time
import uuid
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
from flink_jobs.fraud_detection import FraudDetector, _deserialize


@pytest.fixture
def sample_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id": 1234567890,
        "amount": 15000.0,
        "currency": "EUR",
        "merchant": "Amazon",
        "city": "Paris",
        "latitude": 48.8566,
        "longitude": 2.3522,
        "timestamp": "2024-06-01T12:00:00",
        "device_id": 12345678,
        "ip_address": "192.168.1.1",
        "is_fraud": True
    }


class TestPipelineIntegration:
    def test_full_pipeline_producer_to_detection(self, sample_transaction):
        detector = FraudDetector()
        alerts = detector.detect(sample_transaction)
        assert len(alerts) >= 1
        assert alerts[0]["rule"] == "montant_excessif"

    def test_clean_tx_no_alert(self):
        detector = FraudDetector()
        tx = {
            "transaction_id": str(uuid.uuid4()),
            "card_id": 1,
            "amount": 25.0,
            "city": "Lyon",
            "latitude": 45.7640,
            "longitude": 4.8357,
            "timestamp": "2024-06-01T12:00:00"
        }
        alerts = detector.detect(tx)
        assert alerts == []


class TestCassandraWriter:
    @patch("sink.cassandra_writer.Consumer")
    @patch("sink.cassandra_writer.Cluster")
    def test_alert_format_written(self, mock_cluster, mock_consumer):
        mock_session = MagicMock()
        mock_cluster.return_value.connect.return_value = mock_session

        from sink.cassandra_writer import main as cassandra_main
        msg_mock = MagicMock()
        msg_mock.error.return_value = None
        alert = {
            "transaction_id": "tx-001",
            "card_id": 12345,
            "rule": "montant_excessif",
            "amount": 15000.0,
            "city": "Paris",
            "severity": "high",
            "timestamp": "2024-06-01T12:00:00"
        }
        msg_mock.value.return_value = json.dumps(alert).encode("utf-8")
        mock_consumer.return_value.poll.side_effect = [msg_mock, KeyboardInterrupt]

        with pytest.raises(KeyboardInterrupt):
            cassandra_main()

        assert mock_session.execute.called


class TestRedisWriter:
    @patch("sink.redis_writer.Consumer")
    @patch("sink.redis_writer.redis.Redis")
    def test_redis_push_format(self, mock_redis, mock_consumer):
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance

        from sink.redis_writer import main as redis_main
        msg_mock = MagicMock()
        msg_mock.error.return_value = None
        alert = {"rule": "montant_excessif", "card_id": 12345, "severity": "high"}
        msg_mock.value.return_value = json.dumps(alert).encode("utf-8")
        mock_consumer.return_value.poll.side_effect = [msg_mock, KeyboardInterrupt]

        with pytest.raises(KeyboardInterrupt):
            redis_main()

        assert mock_redis_instance.lpush.called
        assert mock_redis_instance.ltrim.called
        assert mock_redis_instance.expire.called


class TestMinioWriter:
    @patch("sink.minio_writer.Consumer")
    @patch("sink.minio_writer.Minio")
    def test_minio_batch_flush(self, mock_minio, mock_consumer):
        mock_minio_instance = MagicMock()
        mock_minio_instance.bucket_exists.return_value = True
        mock_minio.return_value = mock_minio_instance

        from sink.minio_writer import main as minio_main
        msg_mock = MagicMock()
        msg_mock.error.return_value = None
        msg_mock.value.return_value = json.dumps({
            "transaction_id": "tx-001", "amount": 100.0
        }).encode("utf-8")
        msg_mock.offset.return_value = 0
        msg_mock.partition.return_value = 0

        # First N messages trigger batch, then KeyboardInterrupt
        side_effects = [msg_mock] * 105
        side_effects.append(KeyboardInterrupt)
        mock_consumer.return_value.poll.side_effect = side_effects

        with pytest.raises(KeyboardInterrupt):
            minio_main()

        assert mock_minio_instance.put_object.called


class TestSerialization:
    def test_json_deserialize(self):
        tx = {"transaction_id": "tx-001", "card_id": 1, "amount": 100.0}
        with patch("flink_jobs.fraud_detection.SERIALIZATION_FORMAT", "json"):
            result = _deserialize(json.dumps(tx).encode("utf-8"))
        assert result["transaction_id"] == "tx-001"
        assert result["amount"] == 100.0

    def test_deserialize_empty_returns_none(self):
        with patch("flink_jobs.fraud_detection.SERIALIZATION_FORMAT", "json"):
            result = _deserialize(b"")
        assert result is None
