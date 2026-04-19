#!/bin/bash

echo "Waiting for Kafka Connect to be ready..."
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:8083/connectors)" != "200" ]]; do
    printf '.'
    sleep 5
done
echo " Kafka Connect is ready!"

echo "Creating MinIO bucket..."
docker exec minio mc alias set local http://localhost:9000 admin admin123
docker exec minio mc mb local/northwind-data-lake 2>/dev/null || echo "Bucket may already exist"

# Register Debezium PostgreSQL connector
echo ""
echo "Registering Debezium PostgreSQL connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/debezium-postgres.json

# Register S3 MinIO sink connector
echo ""
echo "Registering S3 MinIO sink connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/s3-sink-minio-production.json

echo ""
echo "========================================="
echo "All Connectors Registered!"
echo "========================================="

echo ""
echo "Current Connectors:"
curl -s http://localhost:8083/connectors/ 

echo ""
echo "Debezium Connector Status:"
curl -s http://localhost:8083/connectors/source-postgres-debezium/status 

echo ""
echo "S3 Sink Connector Status:"
curl -s http://localhost:8083/connectors/minio-s3-sink-connector/status

echo ""
echo "Pipeline ready! PostgreSQL changes will flow to MinIO"