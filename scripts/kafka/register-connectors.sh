#!/bin/bash

echo "Waiting for Kafka Connect to be ready..."
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:8083/connectors)" != "200" ]]; do
    printf '.'
    sleep 5
done
echo " Kafka Connect is ready!"

echo "Registering connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/debezium-postgres.json

echo ""
echo "Current Connectors:"
curl -s http://localhost:8083/connectors/

echo ""
echo "Connector status:"
curl -s http://localhost:8083/connectors/inventory-connector/status