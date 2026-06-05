#!/bin/bash
set -e

# ─── 1. Wait for MinIO ────────────────────────────────────────────────────────
echo "Waiting for MinIO to be ready..."
until curl -sf http://localhost:9000/minio/health/live > /dev/null; do
    printf '.'
    sleep 3
done
echo " MinIO is ready!"

# ─── 2. Create bronze bucket ──────────────────────────────────────────────────
echo "Creating MinIO bucket..."
MSYS_NO_PATHCONV=1 docker run --rm --network datebayozenith_default \
  --entrypoint /bin/sh minio/mc \
  -c "mc alias set local http://minio:9000 admin admin123 \
      && (mc ls local/raw    > /dev/null 2>&1 || mc mb local/raw) \
      && (mc ls local/bronze > /dev/null 2>&1 || mc mb local/bronze)"
echo "Bucket 'raw' and bronze' ready!"

# ─── 3. Wait for Schema Registry ─────────────────────────────────────────────
echo ""
echo "Waiting for Schema Registry to be ready..."
until curl -sf http://localhost:8081/subjects > /dev/null 2>&1; do
    printf '.'
    sleep 3
done
echo " Schema Registry is ready!"

# ─── 4a. Wait for Kafka Connect ───────────────────────────────────────────────

echo ""
echo "Waiting for Kafka Connect to be ready..."
until [[ "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8083/connectors)" == "200" ]]; do
    printf '.'
    sleep 5
done
echo " Kafka Connect is ready!"

# ─── 4b. Create Kafka alert topics ───────────────────────────────────────────
echo ""
echo "Creating Kafka alert topics..."
bash create-alert-topics.sh

# ─── 5. Register Debezium — source first ─────────────────────────────────────
echo ""
echo "Registering Debezium PostgreSQL connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/debezium-postgres.json

# ─── 6. Wait for Debezium to register schemas in Schema Registry ──────────────
echo "Waiting for Debezium to register Avro schemas..."
until curl -sf http://localhost:8081/subjects | grep -q "orders-value"; do
    printf '.'; sleep 3
done
echo " Schemas registered!"

# ─── 7. Register S3 Sink — only after schemas exist ──────────────────────────
echo ""
echo "Registering S3 MinIO sink connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/s3-sink-minio-production.json

# ─── 8. Status check ──────────────────────────────────────────────────────────
echo ""
echo "========================================="
echo "All Connectors Registered!"
echo "========================================="

echo ""
echo "Debezium Connector Status:"
curl -s http://localhost:8083/connectors/source-postgres-debezium/status

echo ""
echo "S3 Sink Connector Status:"
curl -s http://localhost:8083/connectors/minio-s3-sink-connector/status

echo ""
echo "Pipeline ready! PostgreSQL changes will flow to MinIO bronze bucket."