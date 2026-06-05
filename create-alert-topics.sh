#!/bin/bash
# =============================================================================
# create-alert-topics.sh
# Creates the two Kafka alert routing topics required by the alerting pipeline.
# Safe to re-run (--if-not-exists). Run this before registering connectors.
# =============================================================================
set -e

KAFKA_CONTAINER="kafka"
BOOTSTRAP="kafka:29092"
RETENTION_MS=$((7 * 24 * 60 * 60 * 1000))   # 7 days in milliseconds

echo "Creating Kafka alert topics..."

docker exec "${KAFKA_CONTAINER}" kafka-topics \
  --bootstrap-server "${BOOTSTRAP}" \
  --create \
  --if-not-exists \
  --topic northstream.alerts.infrastructure \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms="${RETENTION_MS}"

echo "  ✓ northstream.alerts.infrastructure"

docker exec "${KAFKA_CONTAINER}" kafka-topics \
  --bootstrap-server "${BOOTSTRAP}" \
  --create \
  --if-not-exists \
  --topic northstream.alerts.data-quality \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms="${RETENTION_MS}"

echo "  ✓ northstream.alerts.data-quality"

echo ""
echo "Alert topics ready. Verify at http://localhost:8090 under the northstream.* namespace."