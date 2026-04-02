#!/bin/bash

# Wait for Kafka to be ready
echo "Waiting for Kafka..."
while ! kafka-topics --bootstrap-server kafka:29092 --list; do
  sleep 2
done

# Create topics
kafka-topics --create --topic northwind.public.orders --partitions 4 --replication-factor 1 --bootstrap-server kafka:29092
kafka-topics --create --topic northwind.public.order_details --partitions 4 --replication-factor 1 --bootstrap-server kafka:29092
kafka-topics --create --topic northwind.public.products --partitions 2 --replication-factor 1 --bootstrap-server kafka:29092
kafka-topics --create --topic northwind.public.customers --partitions 2 --replication-factor 1 --bootstrap-server kafka:29092

echo "Topics created successfully"