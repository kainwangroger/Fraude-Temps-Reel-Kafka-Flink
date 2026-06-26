#!/usr/bin/env bash
set -euo pipefail

FLINK_HOST="${FLINK_HOST:-localhost}"
FLINK_PORT="${FLINK_PORT:-8081}"
JOB_FILE="src/flink_jobs/fraud_detection_flink.py"
KAFKA_JAR="file:///opt/flink/lib/flink-sql-connector-kafka-2.3.0.jar"

echo "Submitting PyFlink job to Flink at $FLINK_HOST:$FLINK_PORT..."

curl -X POST "http://$FLINK_HOST:$FLINK_PORT/v1/jobs/upload" \
    -F "jarfile=@$JOB_FILE" \
    --connect-timeout 5 \
    --max-time 30

echo ""
echo "Done. Check status at http://$FLINK_HOST:$FLINK_PORT"
