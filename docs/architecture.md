# Pipeline Architecture

This document outlines the end-to-end Change Data Capture (CDC) pipeline architecture designed to stream real-time database changes from Postgres into a MinIO Data Lake.

## High-Level Flow

1. **Source (Postgres)**: The source Northwind database, configured with `wal_level=logical` to enable logical replication. It serves as the primary transactional system of record.
2. **Debezium Source Connector**: A Kafka Connect plugin that tails the Postgres Write-Ahead Log (WAL). It continuously captures insert, update, and delete (IUD) operations and pushes them as immutable event streams.
3. **Kafka Backbone**: The core distributed event streaming platform. It acts as a durable, fault-tolerant buffer, storing the CDC events within strictly ordered topics based on the source table names.
4. **Schema Registry**: A centralized service that defines, stores, and enforces the payload structures using the Avro format. This ensures downstream compatibility and prevents pipeline breakage due to schema drift.
5. **S3 Sink Connector**: A Kafka Connect plugin that consumes the Kafka topics and efficiently flushes the streamed events as batched, columnar Parquet files into a MinIO S3-compatible data lake.

## Core Infrastructure

The pipeline components are containerized and orchestrated via Docker Compose:
- **`postgres-db`**: The relational engine holding operational data.
- **`kafka`**: KRaft-based single-node broker for event storage.
- **`schema-registry`**: Enforces canonical Avro data schemas.
- **`kafka-connect`**: Distributed worker handling Source (Debezium) and Sink (S3) connector tasks.
- **`minio`**: Highly available S3-compatible object storage serving as the final data lake destination.
- **`akhq`**: Diagnostic web GUI for inspecting topics, schemas, and consumer groups.
